module Pwrake

  class TaskConditionVariable < ConditionVariable
    def signal(hint=nil)
      super()
    end
  end


  class PriorityQueueArray < Array
    def initialize(n)
      super()
    end

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
    def initialize(n)
      super()
    end

    def shift
      pop
    end
  end


  class FifoQueueArray < Array
    def initialize(n)
      super()
    end
  end


  # Rank-Even Last In First Out
  class RankQueueArray
    def initialize(n)
      @q = []
      @size = 0
      @n = (n>0) ? n : 1
    end

    def push(t)
      r = t.rank
      a = @q[r]
      if a.nil?
        @q[r] = a = []
      end
      @size += 1
      a.push(t)
    end

    def size
      @size
    end

    def empty?
      @size == 0
    end

    def shift
      if empty?
        return nil
      end
      (@q.size-1).downto(0) do |i|
        a = @q[i]
        next if a.nil? || a.empty?
        @size -= 1
        if a.size <= @n
          return pop_last_max(a)
        else
          return shift_weighted
        end
      end
      raise "ELIFO: @q=#{@q.inspect}"
    end

    def shift_weighted
      weight, weight_avg = RANK_STAT.rank_weight
      wsum = 0.0
      q = []
      @q.each_with_index do |a,i|
        next if a.nil? || a.empty?
        w = weight[i]
        w = weight_avg if w.nil?
        # w *= a.size
        wsum += w
        q << [a,wsum]
      end
      #
      x = rand() * wsum
      Log.debug "--- shift_weighted x=#{x} wsum=#{wsum} weight=#{weight.inspect}"
      q.each do |a,w|
        if w > x
          return a.pop
        end
      end
      raise "ELIFO: wsum=#{wsum} x=#{x}"
    end

    def pop_last_max(a)
      if a.size < 2
        return a.pop
      end
      y_max = nil
      i_max = nil
      n = [@n, a.size].min
      (-n..-1).each do |i|
        y = a[i].input_file_size
        if y_max.nil? || y > y_max
          y_max = y
          i_max = i
        end
      end
      a.delete_at(i_max)
    end

    def first
      return nil if empty?
      @q.size.times do |i|
        a = @q[i]
        unless a.nil? || a.empty?
          return a.first
        end
      end
    end

    def last
      return nil if empty?
      @q.size.times do |i|
        a = @q[-i-1]
        unless a.nil? || a.empty?
          return a.last
        end
      end
    end

    def delete(t)
      n = 0
      @q.each do |a|
        if a
          a.delete(t)
          n += a.size
        end
      end
      @size = n
    end

    def clear
      @q.clear
      @size = 0
    end
  end


  class NoActionQueue
    def initialize
      @que = []
      @num_waiting = 0
      @mutex = Mutex.new
      @cond = ConditionVariable.new
      @halt = false
      @th_end = {}
      prio = Pwrake.application.pwrake_options['NOACTION_QUEUE_PRIORITY'] || 'fifo'
      case prio
      when /fifo/i
        @prio = 0
        Log.debug "--- NOACTION_QUEUE_PRIORITY=FIFO"
      when /lifo/i
        @prio = 1
        Log.debug "--- NOACTION_QUEUE_PRIORITY=LIFO"
      when /rand/i
        @prio = 2
        Log.debug "--- NOACTION_QUEUE_PRIORITY=RAND"
      else
        raise RuntimeError,"unknown option for NOACTION_QUEUE_PRIORITY: "+prio
      end
    end

    def push(obj)
      if @halt
        @que.push obj
      else
        @mutex.synchronize do
          @que.push obj
          @cond.signal
        end
      end
    end
    alias << push
    alias enq push

    def pop
      @mutex.synchronize do
        t = Time.now
        while true
          if @th_end.delete(Thread.current)
            return false
          elsif @halt
            @cond.wait @mutex
          elsif @que.empty?
            if @finished
              @cond.signal
              return false
            end
            @cond.wait @mutex
          else
            case @prio
            when 0
              x = @que.shift
            when 1
              x = @que.pop
            when 2
              x = @que.delete_at(rand(@que.size))
            end
            Log.debug "--- NATQ#deq %.6f sec #{x.inspect}"%[Time.now-t]
            return x
          end
        end
      end
    end

    alias shift pop
    alias deq pop

    def halt
      @mutex.lock
      @halt = true
    end

    def resume
      @halt = false
      @mutex.unlock
      @cond.broadcast
    end

    def empty?
      @que.empty?
    end

    def clear
      @que.clear
    end

    def length
      @que.length
    end
    alias size length

    def first
      @que.first
    end

    def last
      @que.last
    end

    def finish
      @finished = true
      @cond.broadcast
    end

    def thread_end(th)
      @th_end[th] = true
      @cond.broadcast
    end

    def stop
      clear
      finish
    end
  end


  class TaskQueue

    def initialize(core_list)
      @finished = false
      @halt = false
      @mutex = Mutex.new
      @th_end = {}
      @enable_steal = true
      @q_noaction = NoActionQueue.new
      pri = Pwrake.application.pwrake_options['QUEUE_PRIORITY'] || "RANK"
      case pri
      when /dfs/i
        @array_class = PriorityQueueArray
      when /fifo/i
        @array_class = FifoQueueArray # Array # Fifo
      when /lifo/i
        @array_class = LifoQueueArray
      when /rank/i
        @array_class = RankQueueArray
      else
        raise RuntimeError,"unknown option for QUEUE_PRIORITY: "+pri
      end
      Log.debug "--- TQ#initialize @array_class=#{@array_class.inspect}"
      init_queue(core_list)
    end

    def init_queue(core_list)
      @cv = TaskConditionVariable.new
      @q_input = @array_class.new(core_list.size)
      @q_later = Array.new
    end

    attr_reader :mutex
    attr_accessor :enable_steal

    def halt
      @q_noaction.halt
      @mutex.lock
      @halt = true
    end

    def resume
      @halt = false
      @q_noaction.resume
      @mutex.unlock
      @cv.broadcast
    end

    def synchronize(condition)
      ret = nil
      if condition
        halt
        begin
          ret = yield
        ensure
          resume
        end
      else
        ret = yield
      end
      ret
    end

    # enq
    def enq(item)
      Log.debug "--- TQ#enq #{item.name}"
      t0 = Time.now
      if item.actions.empty?
        @q_noaction.enq(item)
      elsif @halt
        enq_body(item)
      else
        @mutex.synchronize do
          enq_body(item)
          @cv.signal(item.suggest_location)
        end
      end
      Log.debug "--- TQ#enq #{item.name} enq_time=#{Time.now-t0}"
    end

    def enq_body(item)
      enq_impl(item)
    end

    def enq_impl(t)
      if t.has_input_file?
        @q_input.push(t)
      else
        @q_later.push(t)
      end
    end

    # deq
    def deq(hint=nil)
      if hint == '(noaction)'
        return @q_noaction.deq
      end
      n = 0
      loop do
        @mutex.synchronize do
          t0 = Time.now
          if @th_end.delete(Thread.current)
            return false

          elsif @halt
            Log.debug "--- halt in TQ#deq @q=#{@q.inspect}"
            @cv.wait(@mutex)
            n = 0

          elsif empty? # no item in queue
            if @finished
              @cv.signal
              return false
            end
            @cv.wait(@mutex)
            n = 0

          else
            if t = deq_impl(hint,n)
              t_inspect = t.inspect[0..1000]
              Log.debug "--- TQ#deq #{t_inspect} deq_time=#{Time.now-t0}"
              return t
            end
            n += 1
          end
        end
      end
    end

    def deq_impl(hint,n)
      Log.debug "--- TQ#deq_impl #{@q.inspect}"
      @q_input.shift || @q_later.shift
    end

    def clear
      @q_noaction.clear
      @q_input.clear
      @q_later.clear
    end

    def empty?
      @q_noaction.empty? &&
        @q_input.empty? &&
        @q_later.empty?
    end

    def finish
      Log.debug "--- TQ#finish"
      @q_noaction.finish
      @finished = true
      @cv.broadcast
    end

    def stop
      @q_noaction.stop
      @mutex.synchronize do
        clear
        finish
      end
    end

    def thread_end(th)
      @th_end[th] = true
      @cv.broadcast
      @q_noaction.thread_end(th)
    end

    def after_check(tasks)
      # implimented at subclass
    end

  end
end
