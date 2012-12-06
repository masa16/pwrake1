module Pwrake

  module TaskAlgorithm
    def assigned
      @assigned ||= []
    end
  end


  class LocalityAwareQueue < TaskQueue

    class Throughput

      def initialize(list=nil)
        @interdomain_list = {}
        @interhost_list = {}
        if list
          values = []
          list.each do |x,y,v|
            hash_x = (@interdomain_list[x] ||= {})
            hash_x[y] = n = v.to_f
            values << n
          end
          @min_value = values.min
        else
          @min_value = 1
        end
      end

      def interdomain(x,y)
        hash_x = (@interdomain_list[x] ||= {})
        if v = hash_x[y]
          return v
        elsif v = (@interdomain_list[y] || {})[x]
          hash_x[y] = v
        else
          if x == y
            hash_x[y] = 1
          else
            hash_x[y] = 0.1
          end
        end
        hash_x[y]
      end

      def interhost(x,y)
        return @min_value if !x
        hash_x = (@interhost_list[x] ||= {})
        if v = hash_x[y]
          return v
        elsif v = (@interhost_list[y] || {})[x]
          hash_x[y] = v
        else
          x_short, x_domain = parse_hostname(x)
          y_short, y_domain = parse_hostname(y)
          v = interdomain(x_domain,y_domain)
          hash_x[y] = v
        end
        hash_x[y]
      end

      def parse_hostname(host)
        /^([^.]*)\.?(.*)$/ =~ host
        [$1,$2]
      end

    end # class Throughput


    def initialize(hosts,opt={})
      super(opt)
      @hosts = hosts
      @throughput = Throughput.new
      @size = 0
      @q = nil
      @q1 = []
      @q2 = {}
      @hosts.each{|h| @q2[h]=[]}
      @q2[nil] = []
      @enable_steal = !opt['disable_steal']
      @time_prev = Time.now
    end

    attr_reader :size


    def time_out
      if @size == 0
        if @q1.size == 1
          0.1
        else
          0.2
        end
      else
        1.0
      end
    end


    def log_bulk_mvq
      msg = @q1[0..2].map{|t| t.name}.inspect
      msg.sub!(/]$/,",...") if @q1.size > 3
      Log.info "-- bulk_mvq @q1.size=#{@q1.size} @q1=#{msg} @size=#{@size} timespan=#{Time.now-@time_prev}"
    end

    def bulk_mvq
      #Log.debug "--- #{self.class}#bulk_mvq nq=#{nq} @q1=#{@q1.inspect}"
      return if @q1.empty?

      time_now = Time.now
      return if time_now-@time_prev < time_out
      log_bulk_mvq
      @time_prev = time_now

      a = @q1
      @q1 = []

      where(a)

      while t = a.shift
        mvq(t)
      end

      @cv.broadcast
    end


    def where(task_list)
      # implemented in child class
    end


    def mvq(t)
      stored = false
      if t.respond_to? :location
        t.location.each do |h|
          if q = @q2[h]
            t.assigned.push(h)
            q.push(t)
            stored = true
          end
        end
      end
      if !stored
        @q2[@hosts[rand(@hosts.size)]].push(t)
        # @q2[nil].push(t)
      end
      @size += 1
    end


    def enq_impl(item,hint=nil)
      #Log.debug "--- #{self.class}#enq_impl #{item.inspect}"
      @q1.push(item)
      bulk_mvq
    end

    def deq_impl(host,n)
      bulk_mvq

      if t = deq_locate(host)
        Log.info "-- deq_locate n=#{n} task=#{t.name} host=#{host}"
        return t
      end

      if @enable_steal && n > 1
        if t = deq_steal(host)
          Log.info "-- deq_steal n=#{n} task=#{t.name} host=#{host}"
          return t
        end
      end

      m = [0.05*(n+1), 2].min
      @cv.wait(@mutex,m)
      nil
    end


    def deq_locate(host)
      q = @q2[host]
      if q && !q.empty?
        t = q.shift
        t.assigned.each{|x| @q2[x].delete_if{|x| t.equal? x}}
        @size -= 1
        return t
      else
        nil
      end
    end

    def deq_steal(host)
      # select a task based on many and close
      max_host = nil
      max_num  = 0
      @q2.each do |h,a|
        if !a.empty?
          d = @throughput.interhost(host,h) * a.size
          if d > max_num
            max_host = h
            max_num  = d
          end
        end
      end
      if max_host
        deq_locate(max_host)
      else
        deq_locate(nil)
      end
    end

    def size
      @q1.size + @size
    end

    def clear
      @q1.clear
      @hosts.each{|h| @q2[h].clear}
    end

    def empty?
      @q1.empty? && @hosts.all?{|h| @q2[h].empty?}
    end

    def finish
      super
      # @thread.run if @thread.alive?
    end

  end
end
