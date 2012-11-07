require "thread"

module Pwrake

  class SSH < Shell

    attr_reader :host

    def system_cmd(*arg)
      if arg.size != 1
        raise ArgumentError, "wrong number of argument (1 for %d)"%[arg.size]
      end
      @host = arg[0]
      "ssh -x -T -q #{@host} #{@@nice} #{@@shell}"
    end

    def self.connect_list( hosts )
      connections = []
      hosts.map do |h|
        Thread.new(h) {|x|
          ssh = SSH.new(x) 
          ssh.cd_cwd
          connections << ssh
        }
      end.each{|t| t.join}
      connections
    end

  end

end # module Pwrake
