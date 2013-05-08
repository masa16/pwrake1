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
      # Log.debug "--- #{TQ}#enq #{item.inspect}"
      if @halt
	enq_body(item,hint)
      else
        @mutex.synchronize do
	  enq_body(item,hint)
	  @cv.signal
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
            n += 1
          end
        end
        Thread.pass
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
