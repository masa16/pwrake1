module Pwrake

  class MTimePriorityArray < Array

    def order_by(item)
      item.input_file_mtime
    end

    def push(t)
      order_value = order_by(t)
      if empty? || order_by(last) <= order_value
        super(t)
      elsif order_by(first) > order_value
        unshift(t)
      else
        lower = 0
        upper = size-1
        while lower+1 < upper
          mid = ((lower + upper) / 2).to_i
          if order_by(self[mid]) <= order_value
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
      order_value = t.order_by
      if order_by(last) < order_value || order_by(first) > order_value
        nil
      else
        lower = 0
        upper = size-1
        while lower+1 < upper
          mid = ((lower + upper) / 2).to_i
          if order_by(self[mid]) < order_value
            lower = mid
          else
            upper = mid
          end
        end
        mid = upper
        if order_by(self[mid]) == order_value
          Log.debug "--- TQA#index=#{mid}, order_value=#{order_value}"
          mid
        end
      end
    end
  end


  # rank-Even Last In First Out
  class CacheAwareQueue
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
        @q[r] = a = MTimePriorityArray.new
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

    def shift_high_rank
      (@q.size-1).downto(0) do |i|
        a = @q[i]
        next if a.nil? || a.empty?
        @size -= 1
        return pop_last_max(a)
      end
      nil
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

end
