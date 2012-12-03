module Pwrake

  class Profiler

    def initialize
      @lock = Mutex.new
      @separator = ","
      @gnu_time = false
      @id = 0
      @io = nil
    end

    attr_accessor :separator, :gnu_time

    def open(file,gnu_time)
      @gnu_time = gnu_time
      @lock.synchronize do
        @io.close if @io != nil
        @io = File.open(file,"w")
      end
      _puts table_header
    end

    def close
      @lock.synchronize do
        @io.close if @io != nil
        @io = nil
      end
    end

    def _puts(s)
      @lock.synchronize do
        @io.puts(s) if @io
      end
    end

    def table_header
      a = %w[id task command start end elap status]
      if @gnu_time
        a.concat %w[realtime systime usrtime maxrss averss memsz
           datasz stcksz textsz pagesz majflt minflt nswap ncswinv
           ncswvol ninp nout msgrcv msgsnd signum]
      end
      a.join(@separator)
    end

    def command(cmd,terminator)
      if @gnu_time
        if /\*|\?|\{|\}|\[|\]|<|>|\(|\)|\~|\&|\||\\|\$|;|`|\n/ =~ cmd
          cmd = cmd.gsub(/'/,"'\"'\"'")
          cmd = "sh -c '#{cmd}'"
        end
        f = %w[%x %e %S %U %M %t %K %D %p %X %Z %F %R %W %c %w %I %O %r
               %s %k].join(@separator)
        "/usr/bin/time -o /dev/stdout -f '#{terminator}:#{f}' #{cmd}"
      else
        "#{cmd}\necho '#{terminator}':$? "
      end
    end

    def format_time(t)
      t.utc.strftime("%F %T.%L").inspect
    end

    def profile(task, cmd, start_time, end_time, status)
      id = @lock.synchronize do
        id = @id
        @id += 1
        id
      end
      if @io
        _puts [id, task && task.name.inspect,
               cmd.inspect,
               format_time(start_time),
               format_time(end_time),
               "%.3f" % (end_time-start_time),
               status].join(@separator)
      end
      if @gnu_time
        /^([^,]*),/ =~ status
        Integer($1)
      else
        Integer(status)
      end
    end

  end
end
