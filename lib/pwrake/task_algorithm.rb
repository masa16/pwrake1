module Pwrake

  InvocationChain = Rake::InvocationChain
  TaskArguments = Rake::TaskArguments

  module TaskAlgorithm

    # Execute the actions associated with this task.
    def pw_execute(args=nil) # execute_action(args=nil)
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

    def pw_invoke # execute_task
      @lock.synchronize do
        return if @already_invoked
        @already_invoked = true
      end
      pw_execute(@arg_data) if needed?
      @lock.synchronize do
        @postrequisite.each do |t|
          if t.nil?
            application.task_queue.finish
          elsif t.ready_for_invoke(self.name)        # <<---  competition !!!
            application.task_queue.enq t
          end
        end
        @already_finished = true                     # <<---  competition !!!
      end
    end

    def ready_for_invoke(prereq)
      @unfinished_prereq = @prerequisites.dup if !@unfinished_prereq
      @unfinished_prereq.delete(prereq)
      if @unfinished_prereq.empty?
        @unfinished_prereq = nil
        return true
      end
      return false
    end

    def search(*args)
      task_args = TaskArguments.new(arg_names, args)
      search_with_call_chain(nil, task_args, InvocationChain::EMPTY)
    end

    # Same as search, but explicitly pass a call chain to detect
    # circular dependencies.
    def search_with_call_chain(postreq, task_args, invocation_chain) # :nodoc:
      new_chain = InvocationChain.append(self, invocation_chain)
      @lock.synchronize do
        if application.options.trace
          Log.info "** Search #{name} #{format_search_flags}"
        end
        return true if @already_finished                # <<---  competition !!!
        @postrequisite ||= []
        if @postrequisite.include?(postreq)
          raise "multiplly provided post-requisistes"
        end
        @postrequisite << postreq                       # <<--- competition !!!
        return false if @already_searched
        @already_searched = true
        @arg_data = task_args
        search_prerequisites(task_args, new_chain)
        return false
      end
    rescue Exception => ex
      add_chain_to(ex, new_chain)
      raise ex
    end

    # Search all the prerequisites of a task.
    def search_prerequisites(task_args, invocation_chain) # :nodoc:
      if @prerequisites.empty?
        application.task_queue.enq self
      else
        all_finished = true
        prerequisite_tasks.each { |prereq|
          #prereq_args = task_args.new_scope(prereq.arg_names) # in vain
          if !prereq.search_with_call_chain(self, task_args, invocation_chain)
            all_finished = false
          end
        }
        if all_finished
          Log.debug "--- all_finished: #{name}'s prereq"
          application.task_queue.enq self
        end
      end
      #sleep 0.001
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
  end

end # module Pwrake
