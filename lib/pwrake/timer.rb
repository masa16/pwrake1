module Pwrake

  class Timer

    def initialize(prefix,*extra)
      @prefix = prefix
      @start_time = Time.now
      str = "%s[start]:%s %s" %
        [@prefix, Log.fmt_time(@start_time), extra.join(' ')]
      Log.info(str)
    end

    def finish(*extra)
      end_time = Time.now
      elap_time = end_time - @start_time
      str = "%s[end]:%s elap=%.3f %s" %
        [@prefix, Log.fmt_time(end_time), elap_time, extra.join(' ')]
      Log.info(str)
    end

  end
end
