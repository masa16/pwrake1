module Pwrake

  class Shell
    CHARS='0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'
    TLEN=32

    OPEN_LIST={}

    def self.nice=(nice)
      @@nice=nice
    end

    def self.reset_id
      @@current_id = 0
    end

    @@nice = "nice"
    @@shell = "sh"
    @@current_id = 0
    @@profiler = Profiler.new

    def self.profiler
      @@profiler
    end

    def initialize(host,opt={})
      @host = host || 'localhost'
      @lock = Mutex.new
      @@current_id += 1
      @id = @@current_id
      @option = opt
      @work_dir = @option[:work_dir] || Dir.pwd
      @pass_env = @option[:pass_env]
      @ssh_opt = @option[:ssh_opt]
      @gnu_time = @option[:gnu_time] # = true
      @terminator = ""
      TLEN.times{ @terminator << CHARS[rand(CHARS.length)] }
    end

    attr_reader :id
    attr_accessor :current_task

    def system_cmd(*arg)
      if ['localhost','localhost.localdomain','127.0.0.1'].include? @host
        [@@nice,@@shell].join(' ')
      else
        "ssh -x -T -q #{@ssh_opt} #{@host} #{@@nice} #{@@shell}"
      end
    end

    def start
      open(system_cmd)
      cd_work_dir
    end

    def open(cmd,path=nil)
      if path.nil?
        path = ENV['PATH']
      end
      @io = IO.popen( cmd, "r+" )
      OPEN_LIST[__id__] = self
      _execute_shell "export PATH='#{path}'"
      if @pass_env
        @pass_env.each do |k,v|
          _execute_shell "export #{k}='#{v}'"
        end
      end
    end

    attr_reader :host, :status, :profile

    def finish
      close
    end

    def close
      @lock.synchronize do
        if !@io.closed?
          @io.puts("exit")
          @io.close
        end
        OPEN_LIST.delete(__id__)
      end
    end

    def backquote(*command)
      command = command.join(' ')
      @lock.synchronize do
        _execute(command,true)
      end
    end

    def system(*command)
      command = command.join(' ')
      Log.debug "--- command=#{command.inspect}"
      @lock.synchronize do
        _execute(command)
      end
    end

    def cd_work_dir
      _execute_shell("cd #{@work_dir}")
    end


    END {
      OPEN_LIST.map do |k,v|
        v.close
      end
      Shell.profiler.close
    }

    private

    def _execute_shell(cmd)
      @io.puts(cmd)
    end

    def _execute(cmd,quote=nil)
      start_time = Time.now

      @io.puts @@profiler.command(cmd,@terminator)

      while x = @io.gets
        x.chomp!
        if x[0,TLEN] == @terminator
          status = x[TLEN+1..-1]
          break
        end
        if quote
          a << x
        else
          LOCK.synchronize do
            puts x
          end
        end
      end

      end_time = Time.now
      @status = @@profiler.profile(@current_task, cmd,
                                   start_time, end_time, status)
      if quote
        a
      else
        @status==0
      end
    end

  end

end # module Pwrake
