module Pwrake

  class FinishQueue

    def initialize(max)
      @que = []
      @mutex = Mutex.new
      @cond = ConditionVariable.new
      @enq_cond = ConditionVariable.new
      @max = max
    end

    def push(obj)
      @mutex.synchronize do
        while @que.length >= @max
	  @enq_cond.wait(@mutex,3600)
        end
	@que.push obj
	@cond.signal
      end
    end

    alias << push
    alias enq push

    def pop
      @mutex.synchronize do
	while @que.empty?
	  @cond.wait(@mutex,3600)
	end
	q = @que
	@que = []
	@enq_cond.broadcast
	return q
      end
    end

    alias shift pop
    alias deq pop

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
  end

end
