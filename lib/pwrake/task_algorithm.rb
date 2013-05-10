module Pwrake

  InvocationChain = Rake::InvocationChain
  TaskArguments = Rake::TaskArguments

  module TaskAlgorithm

    def location
      @location ||= []
    end

    def location=(a)
      @location = a
    end

    def invoke_modify(*args)
      return if @already_invoked

      application.start_worker

      th = Thread.new(args){|a| pw_search_tasks(a) }

      #pw_search_tasks(args)

      if conn = Pwrake.current_shell
        application.thread_loop(conn,self)
      else
        while true
          t = application.finish_queue.deq
          break if t==self
          #application.postprocess(t)   #        <---------
          #t.pw_enq_subsequents         #        <---------
        end
      end

      th.join
    end

    def pw_search_tasks(args)
      task_args = TaskArguments.new(arg_names, args)
      timer = Timer.new("search_task")
      h = application.pwrake_options['HALT_QUEUE_WHILE_SEARCH']
      application.task_queue.synchronize(h) do
	search_with_call_chain(self, task_args, InvocationChain::EMPTY)
      end
      timer.finish
    end

    def pw_invoke
      if shell = Pwrake.current_shell
        shell.current_task = self
        host = shell.host
        log_host(host)
=begin
        if host && kind_of? Rake::FileTask
          a = []
          @prerequisites.each do |x|
            preq = nil
            begin
              preq = Rake.application[x]
            rescue
            end
            if preq
              p preq.location
              if !preq.location.include?(host)
                if File.file?(preq.name)
                  a << x
                end
              end
            end
          end
          if !a.empty?
            cmd = "gfrep -q -D #{host} #{a.join ' '}"
            Log.info(cmd)
            puts cmd
            system cmd
          end
        end
=end
      end

      @lock.synchronize do
        return if @already_invoked
        @already_invoked = true
      end
      pw_execute(@arg_data) if needed?
      application.postprocess(self) #        <---------
      pw_enq_subsequents2           #        <---------
      application.finish_queue.enq(self)
      shell.current_task = nil if shell
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
      @actions.each do |act|
        case act.arity
        when 1
          act.call(self)
        else
          act.call(self, args)
        end
      end
    end

    def pw_enq_subsequents
      @lock.synchronize do
        @subsequents.each do |t|        # <<--- competition !!!
          t && t.check_and_enq(self.name)
        end
        @already_finished = true        # <<--- competition !!!
      end
    end

    def pw_enq_subsequents2
      @lock.synchronize do
        application.task_queue.synchronize(true) do
          @subsequents.each do |t|        # <<--- competition !!!
            t && t.check_and_enq(self.name)
          end
          @already_finished = true        # <<--- competition !!!
        end
      end
    end

    def check_and_enq(preq_name=nil)
      @unfinished_prereq.delete(preq_name)
      if @unfinished_prereq.empty?
	Log.debug "--- check_and_enq enq name=#{self.name} "
	application.task_queue.enq(self)
      end
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
        @subsequents << subseq           # <<--- competition !!!

        if ! @already_searched
          @already_searched = true
          @arg_data = task_args
          if @prerequisites.empty?
            @unfinished_prereq = {}
            application.task_queue.enq(self)
          else
            search_prerequisites(task_args, new_chain)
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
      @unfinished_prereq = @prerequisites.dup
      prerequisite_tasks.each { |prereq|
        #prereq_args = task_args.new_scope(prereq.arg_names) # in vain
        if prereq.search_with_call_chain(self, task_args, invocation_chain)
          @unfinished_prereq.delete(prereq.name)
        end
      }
      check_and_enq
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


    def suggest_location
      if kind_of?(Rake::FileTask) && preq_name = @prerequisites[0]
        application[preq_name].location
      end
    end

    def log_host(exec_host)
      # exec_host = Pwrake.current_shell.host
      if loc = suggest_location()
        Pwrake.application.count( loc, exec_host )
        if loc.include? exec_host
          compare = "=="
        else
          compare = "!="
        end
        Log.info "-- access to #{@prerequisites[0]}: file_host=#{loc.inspect} #{compare} exec_host=#{exec_host}"
      end
    end

  end

end # module Pwrake
