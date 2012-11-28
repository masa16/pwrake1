module Rake

  class Task
    include Pwrake::TaskAlgorithm

    alias invoke_orig :invoke

    def invoke(*args)
      invoke_modify(*args)
    end

    #def invoke(*args)
    #  task_args = TaskArguments.new(arg_names, args)
    #  search_with_call_chain(self, task_args, InvocationChain::EMPTY)
    #end
  end

end # module Rake
