module Pwrake

  class GfarmAffinityScheduler < Scheduler
    include Log

    def on_trace(tasks)
      if Pwrake.manager.gfarm and Pwrake.manager.affinity
        gfwhere_result = {}
        filenames = []
        tasks.each do |t|
          if t.kind_of? Rake::FileTask and name = t.prerequisites[0]
            filenames << name
          end
        end
        gfwhere_result = GfarmSSH.gfwhere(filenames)
        tasks.each do |t|
          if t.kind_of? Rake::FileTask and prereq_name = t.prerequisites[0]
            t.locality = gfwhere_result[GfarmSSH.gf_path(prereq_name)]
          end
        end
      end
      tasks
    end

    def on_execute(task)
      if task.kind_of? Rake::FileTask and prereq_name = task.prerequisites[0]
        conn = Thread.current[:connection]
        scheduled = task.locality
        if conn
          exec_host = conn.host
          Pwrake.manager.counter.count( scheduled, exec_host )
          if Pwrake.manager.gfarm and conn
            if scheduled and scheduled.include? exec_host
              compare = "=="
            else
              compare = "!="
            end
            log "-- access to #{prereq_name}: gfwhere=#{scheduled.inspect} #{compare} exec=#{exec_host}"
          end
        end
      end
      task
    end

    def on_thread_end
    end

    def queue_class
      AffinityQueue
    end
  end

  manager.scheduler_class = GfarmAffinityScheduler


  class AffinityQueue < TaskQueue

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


    class HostQueue

      def initialize(hosts)
        @hosts = hosts
        @q = {}
        @hosts.each{|h| @q[h]=[]}
        @q[nil]=[]
        @throughput = Throughput.new
        @size = 0
      end

      attr_reader :size

      def push(j)
        stored = false
        if j.respond_to? :locality
          j.locality.each do |h|
            if q = @q[h]
              j.assigned.push(h)
              q.push(j)
              stored = true
            end
          end
        end
        if !stored
          @q[@hosts[rand(@hosts.size)]].push(j)
        end
        @size += 1
      end

      def pop(host)
        q = @q[host]
        if q && !q.empty?
          j = q.shift
          j.assigned.each{|x| @q[x].delete_if{|x| j.equal? x}}
          @size -= 1
          return j
        else
          nil
        end
      end

      def pop_alt(host)
        # select a task based on many and close
        max_host = nil
        max_num  = 0
        @q.each do |h,a|
          if !a.empty?
            d = @throughput.interhost(host,h) * a.size
            if d > max_num
              max_host = h
              max_num  = d
            end
          end
        end
        if max_host
          pop(max_host)
        else
          pop(nil)
        end
      end

      def clear
        @hosts.each{|h| @q[h].clear}
      end

      def empty?
        @hosts.all?{|h| @q[h].empty?}
      end

    end # class HostQueue

    def initialize(hosts=[])
      @q = HostQueue.new(hosts.uniq)
      super(hosts)
    end

  end # class AffinityQueue

end # module Pwrake
