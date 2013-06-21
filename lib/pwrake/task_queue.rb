module Pwrake

  class TaskConditionVariable < ConditionVariable
    def signal(hint=nil)
      super()
    end
  end

  class TaskQueueArray < Array
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
  end # TaskQueueArray

  class LifoQueueArray < Array
    def shift
      pop
    end
  end

  class TaskQueue

    def initialize(*args)
      case Pwrake.application.pwrake_options['QUEUE_PRIORITY']||"DFS"
      when /dfs/i
        @array_class = TaskQueueArray
      when /fifo/i
        @array_class = Array
      when /lifo/i
        @array_class = LifoQueueArray
      else
        raise RuntimeError,"unknown option for QUEUE_PRIORITY"
      end
      @finished = false
      @halt = false
      @mutex = Mutex.new
      @cv = TaskConditionVariable.new
      @th_end = []
      @enable_steal = true
      @q = @array_class.new
      @reservation = {}
      @reserved_q = {}
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
    def enq(item,hint=nil)
      hint = item.suggest_location
      Log.debug "--- TQ#enq #{item.name} hint=#{hint}"
      if @halt
	enq_body(item,hint)
      else
        @mutex.synchronize do
	  enq_body(item,hint)
          Log.debug "--- TQ#enq @cv.signal #{item.name} hint=#{hint}"
	  @cv.signal(hint)
        end
      end
      @reserved_q.keys.each{|th|
        Log.debug "--- run #{th}";
        th.run
      }
    end

    def enq_body(item,hint)
      if th = @reservation[item]
	@reserved_q[th] = item
      else
	enq_impl(item,hint)
      end
    end

    def enq_impl(item,hint)
      @q.push(item)          # FIFO Queue
    end


    # deq
    def deq(hint=nil)
      n = 0
      loop do
        @mutex.synchronize do
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
              Log.debug "--- TQ#deq #{t.inspect}"
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
      @q.shift               # FIFO Queue
    end

    def clear
      @q.clear
      @reserved_q.clear
    end

    def empty?
      @q.empty? && @reserved_q.empty?
    end

    def finish
      Log.debug "--- TQ#finish"
      @finished = true
      @cv.signal
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
