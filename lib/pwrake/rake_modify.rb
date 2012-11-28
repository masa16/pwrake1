module Rake

  class Task
    include Pwrake::TaskAlgorithm

    alias invoke_orig :invoke

    def invoke(*args)
      invoke_modify(*args)
    end

  end

end # module Rake
