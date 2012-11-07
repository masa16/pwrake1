require "thread"

module Pwrake

  class Shell
    CHARS='0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz!"#=~{*}?_-^@[],./'
    TLEN=64

    OPEN_LIST={}

    def self.nice=(nice)
      @@nice=nice
    end

    @@nice = "nice"
    @@shell = "sh"

    def initialize(*arg)
      @host = 'localhost'
      @lock = Mutex.new
      @terminator = ""
      TLEN.times{ @terminator << CHARS[rand(CHARS.length)] }
      open(system_cmd(*arg))
    end

    def system_cmd
      [@@nice,@@shell].join(' ')
    end

    def open(cmd,path=nil)
      if path.nil?
        path = ENV['PATH']
      end
      @lock.synchronize do
        @io = IO.popen( cmd, "r+" )
        @io.puts("export PATH='#{path}'")
        _get
        OPEN_LIST[__id__] = self
      end
    end

    attr_reader :host, :status

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
      if command.kind_of? Array
        command = command.join(' ')
      end
      @lock.synchronize do
        @io.puts(command)
        _get_output
      end
    end

    def system(*command)
      if command.kind_of? Array
        command = command.join(' ')
      end
      @lock.synchronize do
        @io.puts(command)
        _get
      end
      @status==0
    end

    def cd(dir)
      system("cd #{dir}")
    end

    def cd_cwd
      system("cd #{Dir.pwd}")
    end

    def self.connect_list( hosts )
      hosts.map do |h|
        self.new
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
