module Pwrake

  LOCK = Mutex.new

  class Logger

    attr_accessor :level

    module Severity
      # Low-level information, mostly for developers
      DEBUG = 0
      INFO = 1
      WARN = 2
      ERROR = 3
      FATAL = 4
      UNKNOWN = 5
    end
    include Severity

    def initialize
      @level = WARN
      @out = nil
      @filename = nil
      @lock = Mutex.new
    end

    def open(file)
      close if @out
      case file
      when IO
        @out = file
        @filename = nil
      when String
        @out = File.open(file,"w")
        @filename = file
      else
        raise "file arg must be IO or String"
      end
      @start_time = Time.now
      info "LogStart=" + fmt_time(@start_time)
      info "logfile=#{@filename}" if @filename
    end

    def finish(str, start_time)
      if @out
        finish_time = Time.now
        t1 = Log.fmt_time(start_time)
        t2 = Log.fmt_time(finish_time)
        elap = finish_time - start_time
        info "#{str} : start=#{t1} end=#{t2} elap=#{elap}"
      end
    end

    def add(severity, message)
      if !severity || severity >= @level
        if @out
          @lock.synchronize do
            @out.write(message+"\n")
          end
        else
          LOCK.synchronize do
            $stderr.write(message+"\n")
          end
        end
      end
      true
    end
    alias log add

    def info(msg)
      add(INFO, msg)
    end

    def debug(msg)
      add(DEBUG, msg)
    end

    def warn(msg)
      add(WARN, msg)
    end

    def fmt_time(t)
      t.strftime("%Y-%m-%dT%H:%M:%S.%%06d") % t.usec
    end

    def timer(prefix,*args)
      Timer.new(prefix,*args)
    end

    def close
      finish "LogEnd", @start_time
      @lock.synchronize do
        @out.close if @filename
        @out=nil
      end
      @filename=nil
    end

  end # class Logger


  LOGGER = Logger.new

  module Log
    include Logger::Severity

    module_function

    def open(file)
      LOGGER.open(file)
    end

    def close
      LOGGER.close
    end

    def info(s)
      LOGGER.info(s)
    end

    def debug(s)
      LOGGER.debug(s)
    end

    def warn(s)
      LOGGER.warn(s)
    end

    def level
      LOGGER.level
    end

    def level=(x)
      LOGGER.level = x
    end

    def fmt_time(t)
      t.strftime("%Y-%m-%dT%H:%M:%S.%%06d") % t.usec
    end

    def timer(prefix,*args)
      Timer.new(prefix,*args)
    end

    def stderr_puts(message)
      LOCK.synchronize do
        $stderr.write(message+"\n")
      end
    end

    def stdout_puts(message)
      LOCK.synchronize do
        $stdout.write(message+"\n")
      end
    end

  end

end # module Pwrake
