module Pwrake

  def self.option
    @option ||= Option.new
  end

  module Option

    DEFAULT_CONFFILES = ["pwrake_conf.yaml"]

    DEFAULT_OPTIONS = {
      'HOSTFILE' => nil,
      'FILESYSTEM' => nil,
      'LOGFILE' => "Pwrake%Y%m%d-%H%M%S_%$.log",
      'TRACE' => false,
      'WORK_DIR' => '$HOME/%CWD_RELATIVE_TO_HOME',
      'MAIN_HOSTNAME' => `hostname -f`.chomp,
      'GFARM_BASEDIR' => '/tmp',
      'GFARM_PREFIX'  => "pwrake_#{ENV['USER']}",
      'GFARM_SUBDIR'  => '/'
    }

    # ----- init -----

    def init_option
      @host_group = []
      init_options
      init_pass_env
      init_logger
      @counter = Counter.new
    end

    attr_reader :core_list
    attr_reader :counter
    attr_reader :logfile
    attr_reader :queue_class
    attr_reader :shell_class

    # Option order:
    #  DEFAULT_CONF < pwrake_conf < ENV < command_option

    def init_options
      @pwrake_conf = Rake.application.options.pwrake_conf

      if @pwrake_conf
        if !File.exist?(@pwrake_conf)
          raise "Configuration file not found: #{@pwrake_conf}"
        end
      else
        @pwrake_conf = DEFAULT_CONFFILES.find{|fn| File.exist?(fn)}
      end

      if @pwrake_conf.nil?
        @opt = {}
      else
        Log.debug "@pwrake_conf=#{@pwrake_conf}"
        require "yaml"
        @opt = YAML.load(open(@pwrake_conf))
      end

      @opt['PWRAKE_CONF'] = @pwrake_conf

      DEFAULT_OPTIONS.each do |key,value|
        if !@opt[key]
          @opt[key] = value
        end
        if value = ENV[key]
          @opt[key] = value
        end
      end

      @logfile = Rake.application.options.logfile ||
        ENV["LOGFILE"] || ENV["LOG"]
      case @logfile
      when String
        if @logfile == ""
          @logfile = @opt['LOGFILE']
        end
        @logfile = Time.now.strftime(@logfile).sub("%$",Process.pid.to_s)
      else
        @logfile = nil
      end
      @opt['LOGFILE'] = @logfile

      @opt['HOSTFILE'] = @hostfile =
        Rake.application.options.hostfile ||
        ENV["HOSTFILE"] || ENV["HOSTS"]

      @opt['FILESYSTEM'] = @filesystem =
        Rake.application.options.filesystem ||
        ENV["FILESYSTEM"] || ENV["FS"]

      if n = Rake.application.options.num_threads
        @opt['NUM_THREADS'] = @num_threads = n.to_i
      end

      @opt['DISABLE_STEAL'] =
        Rake.application.options.disable_steal ||
        ENV['DISABLE_STEAL']

      @opt['DISABLE_AFFINITY'] =
        Rake.application.options.disable_affinity ||
        ENV['DISABLE_AFFINITY'] ||
        ENV['AFFINITY']=='off'

      @opt['TRACE'] = Rake.application.options.trace
      @opt['VERBOSE'] = true if Rake.verbose
      @opt['SILENT'] = true if !Rake.verbose
      @opt['DRY_RUN'] = Rake.application.options.dryrun
      #@opt['RAKEFILE'] =
      #@opt['LIBDIR'] =
      @opt['RAKELIBDIR'] = Rake.application.options.rakelib.join(':')

      @opt['WORK_DIR'].sub!('%CWD_RELATIVE_TO_HOME',cwd_relative_to_home)
    end

    def cwd_relative_to_home
      Pathname.pwd.relative_path_from(Pathname.new(Dir.home)).to_s
    end

    def cwd_relative_if_under_home
      home = Pathname.new(Dir.home).realpath
      path = pwd = Pathname.pwd.realpath
      while path != home
        if path.root?
          return pwd.to_s
        end
        path = path.parent
      end
      return pwd.relative_path_from(home).to_s
    end

    def init_pass_env
      if envs = @opt['PASS_ENV']
        pass_env = {}

        case envs
        when Array
          envs.each do |k|
            k = k.to_s
            if v = ENV[k]
              pass_env[k] = v
            end
          end
        when Hash
          envs.each do |k,v|
            k = k.to_s
            if v = ENV[k] || v
              pass_env[k] = v
            end
          end
        else
          raise "invalid option for PASS_ENV in pwrake_conf.yaml"
        end

        if pass_env.empty?
          @opt.delete('PASS_ENV')
        else
          @opt['PASS_ENV'] = pass_env
        end
      end
    end

    def init_logger
      if Rake.application.options.debug
        Log.level = Log::DEBUG
      elsif Rake.verbose.kind_of? TrueClass
        Log.level = Log::INFO
      else
        Log.level = Log::WARN
      end

      if @logfile
        logdir = File.dirname(@logfile)
        if !File.directory?(logdir)
          mkdir_p logdir
        end
        Log.open(@logfile)
        # turn trace option on
        #Rake.application.options.trace = true
      else
        Log.open($stdout)
      end
    end

    # ----- setup -----

    def setup_option
      set_hosts
      set_filesystem
    end

    def set_hosts
      if @hostfile && @num_threads
        raise "Cannot set `hostfile' and `num_threads' simultaneously"
      end
      if @hostfile
        require "socket"
        tmplist = []
        File.open(@hostfile) do |f|
          while l = f.gets
            l = $1 if /^([^#]*)#/ =~ l
            host, ncore, group = l.split
            if host
              begin
                host = Socket.gethostbyname(host)[0]
              rescue
                Log.info "FQDN not resoved : #{host}"
              end
              ncore = (ncore || 1).to_i
              group = (group || 0).to_i
              tmplist << ([host] * ncore.to_i)
              @host_group[group] ||= []
              @host_group[group] << host
            end
          end
        end
        #
        @core_list = []
        begin # alternative order
          sz = 0
          tmplist.each do |a|
            @core_list << a.shift if !a.empty?
            sz += a.size
          end
        end while sz>0
        @num_threads = @core_list.size
      else
        @num_threads = 1 if !@num_threads
        @core_list = ['localhost'] * @num_threads
      end
    end

    def set_filesystem

      if @filesystem.nil?
        if GfarmPath.gfarm2fs?
          @opt['FILESYSTEM'] = @filesystem = 'gfarm'
        end
      end

      case @filesystem
      when 'gfarm'
        GfarmPath.subdir = @opt['GFARM_SUBDIR']
        @filesystem  = 'gfarm'
        @shell_class = GfarmShell
        @shell_opt   = {
          :work_dir  => Dir.pwd,
          :pass_env  => @opt['PASS_ENV'],
          :disable_steal => @opt['DISABLE_STEAL'],
          :single_mp => @opt['GFARM_SINGLE_MP'],
          :basedir   => @opt['GFARM_BASEDIR'],
          :prefix    => @opt['GFARM_PREFIX']
        }
        @queue_class = GfarmQueue
      else
        @filesystem  = 'nfs'
        @shell_class = Shell
        @shell_opt   = {
          :work_dir  => @opt['WORK_DIR'],
          :pass_env  => @opt['PASS_ENV']
        }
        @queue_class = TaskQueue
      end
    end

    # ----- finish -----

    def finish_option
      Log.close
    end

  end
end
