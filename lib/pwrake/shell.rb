require "thread"

module Pwrake

  class Shell
    CHARS='0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz!"#=~{*}?_-^@[],./'
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

    def initialize(host,opt={})
      @host = host || 'localhost'
      @lock = Mutex.new
      @@current_id += 1
      @id = @@current_id
      @option = opt
      @work_dir = @option[:work_dir] || Dir.pwd
      @pass_env = @option[:pass_env]
      @terminator = ""
      TLEN.times{ @terminator << CHARS[rand(CHARS.length)] }
    end

    attr_reader :id

    def system_cmd(*arg)
      if ['localhost','localhost.localdomain','127.0.0.1'].include? @host
        [@@nice,@@shell].join(' ')
      else
        "ssh -x -T -q #{@host} #{@@nice} #{@@shell}"
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
      system "export PATH='#{path}'"
      if @pass_env
        @pass_env.each do |k,v|
          system "export #{k}='#{v}'"
        end
      end
    end

    attr_reader :host, :status

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
        @io.puts(command)
        _get_output
      end
    end

    def system(*command)
      command = command.join(' ')
      Log.debug "--- command=#{command.inspect}"
      @lock.synchronize do
        @io.puts(command)
        _get
      end
      #Log.debug "--- command=#{command.inspect} status=#{@status}"
      @status==0
    end


    def cd_work_dir
      puts "cd #{@work_dir}"
      system("cd #{@work_dir}")
    end

    def log_execute(task)
      prereq_name = task.prerequisites[0]
      if task.kind_of? Rake::FileTask and prereq_name
        scheduled = task.location
        Pwrake.application.count( scheduled, @host )
        if scheduled and scheduled.include? @host
          compare = "=="
        else
          compare = "!="
        end
        Log.info "-- access to #{prereq_name}: file_host=#{scheduled.inspect} #{compare} exec_host=#{@host}"
      end
    end


    END {
      OPEN_LIST.map do |k,v|
        v.close
      end
    }

    private

    def _get
      @io.puts "\necho '#{@terminator}':$? "
      while x = @io.gets
        x.chomp!
        if x[0,TLEN] == @terminator
          @status = Integer(x[TLEN+1..-1])
          break
        end
        LOCK.synchronize do
          puts x
        end
      end
      @status==0
    end

    def _get_output
      @io.puts "\necho '#{@terminator}':$? "
      a = ''
      while x = @io.gets
        x.chomp!
        if x[0,TLEN] == @terminator
          @status = Integer(x[TLEN+1..-1])
          break
        end
        a << x
      end
      a
    end
  end

end # module Pwrake
