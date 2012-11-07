module Rake
  class << self
    def application
      @application ||= Pwrake::Application.new
    end
  end
end


module Pwrake

  class << self
    # Current Rake Application
    def application
      Rake.application
    end
  end

  # The TaskManager module is a mixin for managing tasks.
  class Application < ::Rake::Application
    include Pwrake::Log

    # Run the Pwrake application.
    def run
      standard_exception_handling do
        init("pwrake")
        pwrake = nil
        begin
          load_rakefile
          pwrake = Pwrake.manager
          top_level
        ensure
          pwrake.finish if pwrake
        end
      end
    end

    def invoke_task(task_string)
      name, args = parse_task_string(task_string)
      t = self[name]
      begin
        operator = Pwrake.manager.operator
        operator.invoke(t,args)
      ensure
        operator.finish if operator
      end
    end

    def standard_rake_options
      opts = super
      opts.each_with_index do |a,i|
        if a[0] == '--version'
          a[3] = lambda { |value|
            puts "rake, version #{RAKEVERSION}"
            puts "pwrake, version #{Pwrake::PWRAKEVERSION}"
            exit
          }
        end
      end
      opts.concat(
      [
       ['--hostfile', '--nodefile FILE',
        "[Pwrake] Read remote host names from FILE",
         lambda { |value|
           options.hostfile = value
         }
       ],
       ['-L', '--logfile FILE', "[Pwrake] Write log to FILE",
         lambda { |value|
           options.logfile = value
         }
       ],
       ['--gfarm', "[Pwrake] Use Gfarm filesystem (FILESYSTEM=gfarm)",
         lambda { |value|
           options.filesystem = "gfarm"
         }
       ],
       ['-A', '--disable-affinity', "[Pwrake] Turn OFF affinity (AFFINITY=off)",
         lambda { |value|
           options.disable_affinity = true
         }
       ],
       ['-S', '--disable-steal', "[Pwrake] Turn OFF task steal",
         lambda { |value|
           options.disable_steal = true
         }
       ]
      ])
      opts
    end

  end
end
