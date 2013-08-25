module Pwrake

  class TaskConditionVariable < ConditionVariable
    def signal(hint=nil)
      super()
    end
  end

  class PriorityQueueArray < Array
    def push(t)
      task_id = t.task_id
      if empty? || last.task_id <= task_id
        super(t)
      elsif first.task_id > task_id
        unshift(t)
      else
        lower = 0
        upper = size-1
        while lower+1 < upper
          mid = ((lower + upper) / 2).to_i
          if self[mid].task_id <= task_id
            lower = mid
          else
            upper = mid
          end
        end
        insert(upper,t)
      end
    end

    def index(t)
      if size < 40
        return super(t)
      end
      task_id = t.task_id
      if last.task_id < task_id || first.task_id > task_id
        nil
      else
        lower = 0
        upper = size-1
        while lower+1 < upper
          mid = ((lower + upper) / 2).to_i
          if self[mid].task_id < task_id
            lower = mid
          else
            upper = mid
          end
        end
        mid = upper
        if self[mid].task_id == task_id
          Log.debug "--- TQA#index=#{mid}, task_id=#{task_id}"
          mid
        end
      end
    end
  end # PriorityQueueArray

  class LifoQueueArray < Array
    def shift
      pop
    end
  end

  class TaskQueue

    def initialize(*args)
      @finished = false
      @halt = false
      @mutex = Mutex.new
      @th_end = []
      @enable_steal = true
      @reservation = {}
      @reserved_q = {}
      case Pwrake.application.pwrake_options['QUEUE_PRIORITY']||"DFS"
      when /dfs/i
        @array_class = PriorityQueueArray
      when /fifo/i
        @array_class = Array # Fifo
      when /lifo/i
        @array_class = LifoQueueArray
      else
        raise RuntimeError,"unknown option for QUEUE_PRIORITY"
      end
      Log.debug "--- TQ#initialize @array_class=#{@array_class.inspect}"
      init_queue(*args)
    end

    def init_queue(*args)
      @cv = TaskConditionVariable.new
      @q_prior = @array_class.new
      @q_later = Array.new
    end

    attr_reader :mutex
    attr_accessor :enable_steal

    def reserve(item)
      @reservation[item] = Thread.current
    end

    def halt
      @mutex.synchronize do
        @halt = true
      end
    end

    def resume
      @mutex.synchronize do
        @halt = false
        @cv.broadcast
      end
    end

    def synchronize(condition)
      ret = nil
      if condition
        @mutex.lock
	@halt = true
	begin
          ret = yield
	  @cv.broadcast
	ensure
	  @halt = false
	  @mutex.unlock
        end
      else
	ret = yield
      end
      @reserved_q.keys.each do |th|
        Log.debug "--- run #{th}";
        th.run
      end
      ret
    end

    # enq
    def enq(item)
      Log.debug "--- TQ#enq #{item.name}"
      t0 = Time.now
      if @halt
	enq_body(item)
      else
        @mutex.synchronize do
	  enq_body(item)
	  @cv.signal(item.suggest_location)
        end
      end
      @reserved_q.keys.each{|th|
        Log.debug "--- run #{th}";
        th.run
      }
      Log.debug "--- TQ#enq #{item.name} enq_time=#{Time.now-t0}"
    end

    def enq_body(item)
      if th = @reservation[item]
	@reserved_q[th] = item
      else
	enq_impl(item)
      end
    end

    def enq_impl(item)
      if item.prior?
        @q_prior.push(item)
      else
        @q_later.push(item)
      end
    end


    # deq
    def deq(hint=nil)
      n = 0
      loop do
        @mutex.synchronize do
          t0 = Time.now
          if @th_end.first == Thread.current
            @th_end.shift
            return false

          elsif @halt
            Log.debug "--- halt in TQ#deq @q=#{@q.inspect}"
            @cv.wait(@mutex)
            n = 0

          elsif item = @reserved_q.delete(Thread.current)
            Log.debug "--- deq from reserved_q=#{item.inspect}"
            return item

          elsif empty? # no item in queue
            #Log.debug "--- empty=true in #{self.class}#deq @finished=#{@finished.inspect}"
            if @finished
	      @cv.signal
              return false
            end
            #Log.debug "--- waiting in #{self.class}#deq @finished=#{@finished.inspect}"
            @cv.wait(@mutex)
            n = 0

          else
            if t = deq_impl(hint,n)
              t_inspect = t.inspect[0..1000]
              Log.debug "--- TQ#deq #{t_inspect} deq_time=#{Time.now-t0}"
              return t
            end
            #@cv.signal([hint])
            n += 1
          end
        end
        #Thread.pass
      end
    end

    def deq_impl(hint,n)
      Log.debug "--- TQ#deq_impl #{@q.inspect}"
      @q_prior.shift || @q_later.shift
    end

    def clear
      @q_prior.clear
      @q_later.clear
      @reserved_q.clear
    end

    def empty?
      @q_prior.empty? && @q_later.empty? && @reserved_q.empty?
    end

    def finish
      Log.debug "--- TQ#finish"
      @finished = true
      @cv.broadcast
    end

    def stop
      @mutex.synchronize do
        clear
        finish
      end
    end

    def thread_end(th)
      @th_end.push(th)
      @cv.broadcast
    end

    def after_check(tasks)
      # implimented at subclass
    end

  end
end
