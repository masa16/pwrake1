module Pwrake

  InvocationChain = Rake::InvocationChain
  TaskArguments = Rake::TaskArguments

  class RankStat
    def initialize
      @lock = Mutex.new
      @stat = []
    end

    def add_sample(rank,elap)
      @lock.synchronize do
        stat = @stat[rank]
        if stat.nil?
          @stat[rank] = stat = [0,0.0]
        end
        stat[0] += 1
        stat[1] += elap
        Log.debug "--- add_sample rank=#{rank} stat=#{stat.inspect} weight=#{stat[0]/stat[1]}"
      end
    end

    def rank_weight
      @lock.synchronize do
        sum = 0.0
        count = 0
        weight = @stat.map do |stat|
          if stat
            w = stat[0]/stat[1]
            sum += w
            count += 1
            w
          else
            nil
          end
        end
        if count == 0
          avg = 1.0
        else
          avg = sum/count
        end
        [weight, avg]
      end
    end
  end

  RANK_STAT = RankStat.new

  module TaskAlgorithm

    def location
      @location ||= []
    end

    def location=(a)
      @location = a
      @group = []
      @location.each do |host|
        @group |= [Pwrake.application.host_list.host2group[host]]
      end
    end

    def group
      @group ||= []
    end

    def group_id
      @group_id
    end

    def group_id=(i)
      @group_id = i
    end

    def suggest_location=(a)
      @suggest_location = a
    end

    def task_id
      @task_id
    end

    def invoke_modify(*args)
      return if @already_invoked

      if Pwrake.application.pwrake_options['GRAPH_PARTITION']
        require 'pwrake/mcgp'
        MCGP.graph_partition
      end

      application.start_worker

      if false
        th = Thread.new(args){|a| pw_search_tasks(a) }
      else
        pw_search_tasks(args)
        th = nil
      end
      Log.info "-- ps:\n"+`ps xwv|egrep 'PID|ruby'`

      if conn = Pwrake.current_shell
        application.thread_loop(conn,self)
      else
        if fname = application.pwrake_options['GC_PROFILE']
          File.open(fname,"w") do |f|
            gc_count = 0
            while true
              t = application.finish_queue.deq
              if GC.count > gc_count
                f.write Log.fmt_time(Time.now)+" "
                f.write(GC::Profiler.result)
                GC::Profiler.clear
                gc_count = GC.count
              end
              break if t==self
            end
          end
        else
          while true
            t = application.finish_queue.deq
            break if t==self
          end
        end
      end

      th.join if th
    end

    def pw_search_tasks(args)
      task_args = TaskArguments.new(arg_names, args)
      timer = Timer.new("search_task")
      h = application.pwrake_options['HALT_QUEUE_WHILE_SEARCH']
      application.task_queue.synchronize(h) do
	search_with_call_chain(nil, task_args, InvocationChain::EMPTY)
      end
      timer.finish
    end

    def pw_invoke
      time_start = Time.now
      if shell = Pwrake.current_shell
        shell.current_task = self
      end
      @lock.synchronize do
        return if @already_invoked
        @already_invoked = true
      end
      pw_execute(@arg_data) if needed?
      Log.debug("pw_execute: #{Time.now-time_start} sec, name=#{name}")
      if kind_of?(Rake::FileTask)
        t = Time.now
        application.postprocess(self)
        Log.debug("postprocess: #{Time.now-t} sec, name=#{name}")
        if File.exist?(name)
          t = Time.now
          @file_stat = File::Stat.new(name)
          Log.debug("File::Stat: #{Time.now-t} sec, name=#{name}")
        end
      end
      log_task(time_start)
      t = Time.now
      application.finish_queue.enq(self)
      shell.current_task = nil if shell
      pw_enq_subsequents
      Log.debug "--- pw_invoke (#{name}) enq_subseq time=#{Time.now-t} sec"
    end

    def get_file_stat
      @file_stat ||= File::Stat.new(name)
    end

    def log_task(time_start)
      time_end = Time.now

      loc = suggest_location()
      shell = Pwrake.current_shell

      if loc && !loc.empty? && shell && !@actions.empty?
        Pwrake.application.count( loc, shell.host )
      end
      return if !application.task_logger

      elap = time_end - time_start
      if !@actions.empty? && kind_of?(Rake::FileTask)
        RANK_STAT.add_sample(rank,elap)
      end

      row = [ @task_id, name, time_start, time_end, elap, @prerequisites.join('|') ]

      if loc
        row << loc.join('|')
      else
        row << ''
      end

      if shell
        row.concat [shell.host, shell.id]
      else
        row.concat ['','']
      end

      row << ((@actions.empty?) ? 0 : 1)
      row << ((@executed) ? 1 : 0)

      if @file_stat
        row.concat [@file_stat.size, @file_stat.mtime, self.location.join('|')]
      else
        row.concat ['','','']
      end

      s = row.map do |x|
        if x.kind_of?(Time)
          Profiler.format_time(x)
        elsif x.kind_of?(String) && x!=''
          '"'+x+'"'
        else
          x.to_s
        end
      end.join(',')

      # task_id task_name start_time end_time elap_time preq preq_host
      # exec_host shell_id has_action executed file_size file_mtime file_host
      application.task_logger.print s+"\n"
    end


    # Execute the actions associated with this task.
    def pw_execute(args=nil)
      args ||= Rake::EMPTY_TASK_ARGS
      if application.options.dryrun
        Log.info "** Execute (dry run) #{name}"
        return
      end
      if application.options.trace
        Log.info "** Execute #{name}"
      end
      application.enhance_with_matching_rule(name) if @actions.empty?
      begin
        @actions.each do |act|
          case act.arity
          when 1
            act.call(self)
          else
            act.call(self, args)
          end
        end
      rescue Exception=>e
        if kind_of?(Rake::FileTask) && File.exist?(name)
          opt = application.pwrake_options['FAILED_TARGET']||"rename"
          case opt
          when /rename/i
            dst = name+"._fail_"
            ::FileUtils.mv(name,dst)
            msg = "Rename failed target file '#{name}' to '#{dst}'"
            Log.stderr_puts(msg)
          when /delete/i
            ::FileUtils.rm(name)
            msg = "Delete failed target file '#{name}'"
            Log.stderr_puts(msg)
          when /leave/i
          end
        end
        raise e
      end
      @executed = true if !@actions.empty?
    end

    def pw_enq_subsequents
      @lock.synchronize do
        t = Time.now
        h = application.pwrake_options['HALT_QUEUE_WHILE_SEARCH']
        application.task_queue.synchronize(h) do
          @subsequents.each do |t|        # <<--- competition !!!
            if t && t.check_prereq_finished(self.name)
              application.task_queue.enq(t)
            end
          end
        end
        @already_finished = true        # <<--- competition !!!
        Log.debug "--- pw_enq_subseq (#{name}) time=#{Time.now-t} sec"
      end
    end

    def check_prereq_finished(preq_name=nil)
      @unfinished_prereq.delete(preq_name)
      @unfinished_prereq.empty?
    end


    # Same as search, but explicitly pass a call chain to detect
    # circular dependencies.
    def search_with_call_chain(subseq, task_args, invocation_chain) # :nodoc:
      new_chain = InvocationChain.append(self, invocation_chain)
      @lock.synchronize do
        if application.options.trace
          Log.info "** Search #{name} #{format_search_flags}"
        end

        return true if @already_finished # <<--- competition !!!
        @subsequents ||= []
        @subsequents << subseq if subseq # <<--- competition !!!

        if ! @already_searched
          @already_searched = true
          @arg_data = task_args
          @lock_rank = Monitor.new
          if @prerequisites.empty?
            @unfinished_prereq = {}
          else
            search_prerequisites(task_args, new_chain)
          end
          @task_id = application.task_id_counter
          #check_and_enq
          if @unfinished_prereq.empty?
            application.task_queue.enq(self)
          end
        end
        return false
      end
    rescue Exception => ex
      add_chain_to(ex, new_chain)
      raise ex
    end

    # Search all the prerequisites of a task.
    def search_prerequisites(task_args, invocation_chain) # :nodoc:
      @unfinished_prereq = {}
      @prerequisites.each{|t| @unfinished_prereq[t]=true}
      prerequisite_tasks.each { |prereq|
        #prereq_args = task_args.new_scope(prereq.arg_names) # in vain
        if prereq.search_with_call_chain(self, task_args, invocation_chain)
          @unfinished_prereq.delete(prereq.name)
        end
      }
    end

    # Format the trace flags for display.
    def format_search_flags
      flags = []
      flags << "finished" if @already_finished
      flags << "first_time" unless @already_searched
      flags << "not_needed" unless needed?
      flags.empty? ? "" : "(" + flags.join(", ") + ")"
    end
    private :format_search_flags

    def rank
      @lock_rank.synchronize do
        if @rank.nil?
          if @subsequents.nil? || @subsequents.empty?
            @rank = 0
          else
            max_rank = 0
            @subsequents.each do |subsq|
              r = subsq.rank
              if max_rank < r
                max_rank = r
              end
            end
            if @actions.empty? || !kind_of?(Rake::FileTask)
              step = 0
            else
              step = 1
            end
            @rank = max_rank + step
          end
          Log.debug "--- Task[#{name}] rank=#{@rank.inspect}"
        end
      end
      @rank
    end

    def file_size
      @file_stat ? @file_stat.size : 0
    end

    def file_mtime
      @file_stat ? @file_stat.mtime : Time.at(0)
    end

    def input_file_size
      unless @input_file_size
        @input_file_size = 0
        @prerequisites.each do |preq|
          @input_file_size += application[preq].file_size
        end
      end
      @input_file_size
    end

    def has_input_file?
      kind_of?(Rake::FileTask) && !@prerequisites.empty?
    end

    def suggest_location
      if has_input_file? && @suggest_location.nil?
        @suggest_location = []
        loc_fsz = Hash.new(0)
        @prerequisites.each do |preq|
          t = application[preq]
          loc = t.location
          fsz = t.file_size
          if loc && fsz > 0
            loc.each do |h|
              loc_fsz[h] += fsz
            end
          end
        end
        if !loc_fsz.empty?
          half_max_fsz = loc_fsz.values.max / 2
          Log.debug "--- loc_fsz=#{loc_fsz.inspect} half_max_fsz=#{half_max_fsz}"
          loc_fsz.each do |h,sz|
            if sz > half_max_fsz
              @suggest_location << h
            end
          end
        end
      end
      @suggest_location
    end

    def priority
      if has_input_file? && @priority.nil?
        sum_tm = 0
        sum_sz = 0
        @prerequisites.each do |preq|
          pq = application[preq]
          sz = pq.file_size
          if sz > 0
            tm = pq.file_mtime - START_TIME
            sum_tm += tm * sz
            sum_sz += sz
          end
        end
        if sum_sz > 0
          @priority = sum_tm / sum_sz
        else
          @priority = 0
        end
        Log.debug "--- task_name=#{name} priority=#{@priority} sum_file_size=#{sum_sz}"
      end
      @priority || 0
    end

    def input_file_mtime
      if has_input_file? && @input_file_mtime.nil?
        hash = Hash.new
        max_sz = 0
        @prerequisites.each do |preq|
          t = application[preq]
          sz = t.file_size
          if sz > 0
            hash[t] = sz
            if sz > max_sz
              max_sz = sz
            end
          end
        end
        half_max_sz = max_sz / 2
        hash.each do |t,sz|
          if sz > half_max_sz
            time = t.file_mtime
            if @input_file_mtime.nil? || @input_file_mtime < time
              @input_file_mtime = time
            end
          end
        end
      end
      @input_file_mtime
    end

  end

end # module Pwrake
