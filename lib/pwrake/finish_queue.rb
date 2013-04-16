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
        while true
          break if @que.length < @max
	  @enq_cond.wait @mutex
        end
	@que.push obj
	@cond.signal
      end
    end

    alias << push
    alias enq push

    def pop(non_block=false)
      @mutex.synchronize do
	while true
	  if @que.empty?
	    @cond.wait @mutex
	  else
	    #puts "@que.size = #{@que.size}"
	    q = @que
	    @que = []
	    break
	  end
	end
	if @que.length < @max
	  @enq_cond.broadcast
	end
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
