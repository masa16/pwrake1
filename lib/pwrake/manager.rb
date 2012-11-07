module Pwrake

  def self.manager
    if !@manager
      @manager = Manager.new
      @manager.setup_logger
      @manager.setup
      @manager.setup_dag
    end
    @manager
  end

  class Manager
    include Log
    attr_reader :node_group
    attr_reader :core_list
    attr_reader :counter
    attr_reader :threads
    attr_reader :logger
    attr_reader :gfarm
    attr_reader :affinity

    def scheduler_class=(a)
      @scheduler_class=a
    end

    def scheduler_class
      @scheduler_class ||= Scheduler
    end

    def connection_list
      setup_connection if @connection_list.nil?
      @connection_list
    end

    def initialize
      @logfile = nil
      @logger = Logger.new
      @prepare_done = false
      @connection_list = nil
    end


    def setup(scheduling=nil)
      if !@prepare_done
        @counter = Counter.new
        setup_hostlist
        setup_filesystem(scheduling)
        # setup_connection
        @prepare_done = true
      end
    end


    def setup_dag
      if @scheduling == "graph_partition"
        if Task.task_defined?("pwrake_dag")
          Task["pwrake_dag"].invoke
        end
        dag
      end
    end


    def setup_logger
      @logfile = Rake.application.options.logfile ||
        ENV["LOGFILE"] || ENV["LOG"]
      if @logfile
        logdir = File.dirname(@logfile)
        if !File.directory?(logdir)
          mkdir_p logdir
        end
        # turn trace option on
        Rake.application.options.trace = true
        Rake.application.options.verbose = true
        @logger.open(@logfile)
        log "logfile=#{@logfile}"
      else
        @logger.open($stdout)
      end
    end


    def setup_hostlist
      @hostfile = Rake.application.options.hostfile ||
        ENV["HOSTFILE"] || ENV["HOSTLIST"] || ENV["HOSTS"] ||
        ENV["NODEFILE"] || ENV["NODELIST"] || ENV["NODES"]
      #
      @host_group = []
      if @hostfile
        require "socket"
        tmplist = []
        File.open(@hostfile) {|f|
          while l = f.gets
            l = $1 if /^([^#]*)#/ =~ l
            host, ncore, group = l.split
            if host
              host  = Socket.gethostbyname(host)[0]
              ncore = (ncore || 1).to_i
              group = (group || 0).to_i
              tmplist << ([host] * ncore.to_i)
              @host_group[group] ||= []
              @host_group[group] << host
            end
          end
        }
        #
        @core_list = []
        begin # alternative order
          sz = 0
          tmplist.each do |a|
            @core_list << a.shift if !a.empty?
            sz += a.size
          end
        end while sz>0
      else
        @core_list = ["localhost"]
      end
      #
      @threads = @core_list.size
      log "HOSTS=\n" + @core_list.join("\n")
    end


    def setup_filesystem(scheduling=nil)
      fs = Rake.application.options.filesystem ||
        ENV["FILESYSTEM"] || ENV["FS"]
      case fs
      when "gfarm"
        @gfarm = true
        @scheduling = :affinity
        @affinity = true
        log "FILESYSTEM=Gfarm"
        require "pwrake/affinity"
      else
        @gfarm = false
        log "FILESYSTEM=non-Gfarm"
      end
      #
      #@gfarm_mountpoint = ENV["GFARM_MOUNTPOINT"] || ENV["GFARM_MP"]
      #
      @affinity = !(Rake.application.options.disable_affinity ||
          (ENV["AFFINITY"] || ENV["AF"] || "").downcase == "off")
    end


    def setup_connection
      if @core_list.all?{|x| x=="localhost" }
        @connection_class = Shell
      elsif @gfarm
        @connection_class = GfarmSSH
      else
        @connection_class = SSH
      end
      time_init_ssh = Time.now
      log "@connection_class = #{@connection_class}"
      @connection_list = @connection_class.connect_list(@core_list)
    end


    def finish
      if @prepare_done
        @counter.print
        @logger.close
        if @logfile
          $stderr.puts "log file : "+@logfile
        end
      end
    end

  end # class Pwrake::Manager

end
