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


  # N-th Last In First Out
  class NLifoQueueArray < Array
    def initialize(n)
      @n = n
      Log.debug "--- #{self.class}: @n=#{@n}"
      super()
    end

    def shift
      pop
    end

    def push(x)
      if @n==0
        super(x)
      elsif self.size > @n
        insert(-(@n+1),x)
      else
        unshift(x)
      end
    end
  end

  # Random Last In First Out
  class RLifoQueueArray < Array
    def initialize(n)
      @n = n*2
      Log.debug "--- #{self.class}: @n=#{@n}"
      super()
    end

    def shift
      pop
    end

    def push(x)
      if @n==0 || empty?
        super(x)
      else
        n = [self.size+1,@n].min
        insert(-(rand(n)+1),x)
      end
    end
  end

  # Alternate Last In First Out
  class ALifoQueueArray < Array
    def initialize(n)
      @i = 0
      @w = 8
      super()
    end

    def shift
      @i = 1 - @i
      if @i==1
        pop
      else
        pos = rand(size)
        pbeg = [0,pos-@w].max
        pend = [size,pos+@w].min-1
        idx = max_index(self[pbeg..pend]){|x| x.input_file_size}
        delete_at(pbeg+idx)
      end
    end

    def max_index(ary)
      y_max = nil
      i_max = nil
      ary.each_with_index do |x,i|
        y = yield(x)
        if y_max.nil? || y > y_max
          y_max = y
          i_max = i
        end
      end
      i_max
    end
  end

  # Alternate Last Max In First Out
  class MLifoQueueArray < Array
    def initialize(n)
      @i = 0
      @n = (n>0)? n : 1
      super()
    end

    def shift
      if size >= @n*2
        ibeg = size - @n*(@i+1)
        iend = size - @n*@i - 1
      else
        if @i==0
          ibeg = [0,size-@n].max
          iend = size - 1
        else
          ibeg = 0
          iend = [size,@n].min-1
        end
      end
      @i = 1 - @i
      a = self[ibeg..iend]
      if a.size > 1
        idx = max_index(a){|x| x.input_file_size}
        delete_at(ibeg+idx)
      else
        pop
      end
    end

    def max_index(ary)
      y_max = nil
      i_max = nil
      ary.each_with_index do |x,i|
        y = yield(x)
        if y_max.nil? || y > y_max
          y_max = y
          i_max = i
        end
      end
      i_max
    end
  end

  # ranK-rotate Last In First Out
  class KLifoQueueArray
    def initialize(n)
      @q = []
      @i = 0
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
      elsif @size < @n*2
        shift_noweight
      else
        shift_weighted
      end
    end

    def shift_noweight
      Log.debug "--- shift_noweight @q=#{@q.inspect}"
      rand_max = @q.count{|a| !(a.nil? || a.empty?)}
      x = rand(rand_max)
      n = 0
      @size -= 1
      @q.each do |a|
        next if a.nil? || a.empty?
        n += 1
        if n > x
          return pop_last_max(a)
          #return a.pop
        end
      end
    end

    def shift_weighted
      x = rand(@size)
      n = 0
      @size -= 1
      @q.each do |a|
        next if a.nil? || a.empty?
        n += a.size
        if n > x
          #return pop_last_max(a)
          return a.pop
        end
      end
    end

    def shift_bak
      return nil if empty?
      @size -= 1
      while true
        a = @q[@i]
        @i = (@i+1) % @q.size
        unless a.nil? || a.empty?
          return pop_last_max(a)
        end
      end
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
      @i = 0
      @size = 0
    end
  end

  # rank-Even Last In First Out
  class ELifoQueueArray
    def initialize(n)
      @q = []
      @i = 0
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
      elsif @size < @n*2
        return shift_high_rank
      else
        return shift_weighted
      end
    end

    def shift_noweight
      Log.debug "--- shift_noweight @q=#{@q.inspect}"
      rand_max = @q.count{|a| !(a.nil? || a.empty?)}
      x = rand(rand_max)
      n = 0
      @size -= 1
      @q.each do |a|
        next if a.nil? || a.empty?
        n += 1
        if n > x
          return pop_last_max(a)
          #return a.pop
        end
      end
    end

    def shift_high_rank
      (@q.size-1).downto(0) do |i|
        a = @q[i]
        next if a.nil? || a.empty?
        @size -= 1
        return pop_last_max(a)
      end
      nil
    end

    def shift_weighted_ending
      weight, weight_avg = RANK_STAT.rank_weight
      wsum = 0.0
      q = []
      @q.each_with_index do |a,i|
        next if a.nil? || a.empty?
        w = weight[i]
        w = weight_avg if w.nil?
        w *= 2**i
        wsum += w
        q << [a,wsum]
      end
      #
      x = rand() * wsum
      Log.debug "--- shift_weighted x=#{x} wsum=#{wsum} weight=#{weight.inspect}"
      @size -= 1
      q.each do |a,w|
        if w > x
          return pop_last_max(a)
          #return a.pop
        end
      end
      raise "ELIFO: wsum=#{wsum} x=#{x}"
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
      @size -= 1
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
      @i = 0
      @size = 0
    end
  end


  class FifoQueueArray < Array
    def initialize(n)
      super()
    end
  end


  class TaskQueue

    def initialize(core_list)
      @finished = false
      @halt = false
      @mutex = Mutex.new
      @th_end = []
      @enable_steal = true
      @reservation = {}
      @reserved_q = {}
      pri = Pwrake.application.pwrake_options['QUEUE_PRIORITY']||"DFS"
      case pri
      when /dfs/i
        @array_class = PriorityQueueArray
      when /fifo/i
        @array_class = FifoQueueArray # Array # Fifo
      when /nifo/i
        @array_class = NLifoQueueArray
      when /rifo/i
        @array_class = RLifoQueueArray
      when /aifo/i
        @array_class = ALifoQueueArray
      when /mifo/i
        @array_class = MLifoQueueArray
      when /kifo/i
        @array_class = KLifoQueueArray
      when /eifo/i
        @array_class = ELifoQueueArray
      when /lifo/i
        @array_class = LifoQueueArray
      else
        raise RuntimeError,"unknown option for QUEUE_PRIORITY: "+pri
      end
      Log.debug "--- TQ#initialize @array_class=#{@array_class.inspect}"
      init_queue(core_list)
    end

    def init_queue(core_list)
      @cv = TaskConditionVariable.new
      @q_prior = Array.new
      @q_input = @array_class.new(core_list.size)
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

    def enq_impl(t)
      if t.has_input_file?
        @q_input.push(t)
      else
        if t.actions.empty?
          @q_prior.push(t)
        else
          @q_later.push(t)
        end
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
      @q_prior.shift || @q_input.shift || @q_later.shift
    end

    def clear
      @q_prior.clear
      @q_input.clear
      @q_later.clear
      @reserved_q.clear
    end

    def empty?
      @q_prior.empty? && @q_input.empty? &&
        @q_later.empty? && @reserved_q.empty?
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
