module Rake

  class Task
    include Pwrake::TaskAlgorithm

    alias invoke_orig :invoke
    def invoke(*args)
      search(*args)
    end
  end

end # module Rake
