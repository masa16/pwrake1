module Pwrake

  class TaskQueue

    def initialize(*args)
      @finished = false
      @halt = false
      @mutex = Mutex.new
      @cv = ConditionVariable.new
      @th_end = []
      @enable_steal = true
      @q = []
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

    def enq(item,hint=nil)
      Log.debug "--- #{self.class}#enq #{item.inspect}"
      th = nil
      if @halt
        if th = @reservation[item]
          @reserved_q[th] = item
        else
          enq_impl(item,hint)
        end
      else
        @mutex.synchronize do
          if th = @reservation[item]
            @reserved_q[th] = item
          else
            enq_impl(item,hint)
            @cv.signal
          end
        end
      end
      @reserved_q.keys.each{|th|
        Log.debug "--- run #{th}";
        th.run
      }
    end

    def enq_impl(item,hint)
      @q.push(item)          # FIFO Queue
    end


    def deq(hint=nil)
      Log.debug "--- #{self.class}#deq @halt=#{@halt.inspect} @q=#{@q.inspect} @reserved_q=#{@reserved_q.inspect} Thread.current=#{Thread.current}"
      n = 0
      loop do
        @mutex.synchronize do
          if @th_end.first == Thread.current
            @th_end.shift
            return false

          elsif @halt
            Log.debug "--- halt in #{self.class}#deq @q=#{@q.inspect}"
            @cv.wait(@mutex)

          elsif item = @reserved_q.delete(Thread.current)
            Log.debug "--- deq from reserved_q=#{item.inspect}"
            return item

          elsif empty? # no item in queue
            Log.debug "--- empty=true in #{self.class}#deq @finished=#{@finished.inspect}"
            if @finished
              @cv.signal
              return false
            end
            @cv.wait(@mutex)
            Log.debug "--- waited in #{self.class}#deq @finished=#{@finished.inspect}"

          else
            if t = deq_impl(hint,n)
              Log.debug "--- #{self.class}#deq #{t.inspect}"
              return t
            end
            n += 1
          end
        end
      end
    end

    def deq_impl(hint,n)
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
      Log.debug "--- #{self.class}#finish"
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
  end

end
