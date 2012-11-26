module Pwrake

  class TaskQueue

    def initialize(hosts=[])
      @finished = false
      @halt = false
      @mutex = Mutex.new
      @cv = ConditionVariable.new
      @th_end = []
      @enable_steal = true
      @q = []
    end

    attr_reader :mutex
    attr_accessor :enable_steal

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

    def enq(task)
      Log.debug "--- #{self.class}#enq #{task.inspect}"
      if @halt
        enq_impl(task)
      else
        @mutex.synchronize do
          enq_impl(task)
          @cv.signal
        end
      end
    end

    def enq_impl(task)
      @q.push(task)
    end


    def deq(host=nil)
      Log.debug "--- #{self.class}#deq @halt=#{@halt.inspect}"
      @mutex.synchronize do
        n = 0
        loop do
          if @th_end.first == Thread.current
            @th_end.shift
            return false
          elsif @halt
            Log.debug "--- halt in #{self.class}#deq @q=#{@q.inspect}"
            # @cv.wait(@mutex,1000)
            @cv.wait(@mutex)
          elsif empty? # no task in queue
            Log.debug "--- empty=true in #{self.class}#deq @finished=#{@finished.inspect}"
            if @finished
              @cv.signal
              return false
            end
            # @cv.wait(@mutex,1000)
            @cv.wait(@mutex)
            Log.debug "--- waited in #{self.class}#deq @finished=#{@finished.inspect}"
          else
            if t = deq_impl(host,n)
              Log.debug "--- #{self.class}#deq #{t.inspect}"
              return t
            end
            n += 1
          end
        end
      end
    end

    def deq_impl(host,n)
      @q.shift
    end

    def clear
      @q.clear
    end

    def empty?
      @q.empty?
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
