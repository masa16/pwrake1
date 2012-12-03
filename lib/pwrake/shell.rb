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
    }

    private

    def _execute_shell(cmd)
      _execute_main(cmd,false,false)
    end

    def _execute(cmd,quote=nil)
      _execute_main(cmd,quote,@gnu_time)
    end

    def _execute_main(cmd,quote,gnu_time)
      t = Time.now
      if gnu_time
        f = "%x,%e,%S,%U,%M,%t,%K,%D,%p,%X,%Z,%F,%R,%W,%c,%w,%I,%O,%r,%s,%k"
        if /\*|\?|\{|\}|\[|\]|<|>|\(|\)|\~|\&|\||\\|\$|;|`|\n/ =~ cmd
          cmd = cmd.gsub(/'/,"'\"'\"'")
          cmd = "sh -c '#{cmd}'"
        end
        @io.puts("/usr/bin/time -o /dev/stdout -f '#{@terminator}:#{f}' #{cmd}")
      else
        @io.puts(cmd)
        @io.puts("\necho '#{@terminator}':$? ")
      end

      while x = @io.gets
        x.chomp!
        if x[0,TLEN] == @terminator
          # p x
          @elap_time = Time.now - t
          stat = x[TLEN+1..-1]
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

      if gnu_time
        puts stat
        @profile = stat.split(',')
        @profile.push @elap_time
        @profile.push cmd
        #p @profile
        #p @profile.size
        @status = Integer(@profile[0])
      else
        @status = Integer(stat)
      end

      if quote
        a
      else
        @status==0
      end
    end


=begin
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
=end

  end

end # module Pwrake
