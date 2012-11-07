require "thread"

module Rake

  class Task
    include Pwrake::TaskAlgorithm
    attr_accessor :output_queue

    def assigned
      @assigned ||= []
    end

    def location
      @location ||= []
    end

    def location=(a)
      @location = a
    end
  end

end # module Rake
