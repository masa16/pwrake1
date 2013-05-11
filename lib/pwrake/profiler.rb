module Pwrake

  class Profiler

    HEADER_FOR_PROFILE =
      %w[id task desc command start end elap status host]

    HEADER_FOR_GNU_TIME =
      %w[realtime systime usrtime maxrss averss memsz
         datasz stcksz textsz pagesz majflt minflt nswap ncswinv
         ncswvol ninp nout msgrcv msgsnd signum]

    def initialize
      @lock = Mutex.new
      @separator = ","
      @re_escape = /\s#{Regexp.escape(@separator)}/
      @gnu_time = false
      @id = 0
      @io = nil
    end

    attr_accessor :separator, :gnu_time

    def open(file,gnu_time=false,plot=false)
      @file = file
      @gnu_time = gnu_time
      @plot = plot
      @lock.synchronize do
        @io.close if @io != nil
        @io = File.open(file,"w")
      end
      _puts table_header
      t = Time.now
      profile(nil,'pwrake_profile_start',t,t)
    end

    def close
      t = Time.now
      profile(nil,'pwrake_profile_end',t,t)
      @lock.synchronize do
        @io.close if @io != nil
        @io = nil
      end
      if @plot
        Profiler.plot_parallelism(@file)
      end
    end

    def _puts(s)
      @lock.synchronize do
        @io.puts(s) if @io
      end
    end

    def table_header
      a = HEADER_FOR_PROFILE
      if @gnu_time
        a += HEADER_FOR_GNU_TIME
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
    end #`

    def format_time(t)
      t.utc.strftime("%F %T.%L").inspect
    end

    def profile(task, cmd, start_time, end_time, host="", status="")
      id = @lock.synchronize do
        id = @id
        @id += 1
        id
      end
      if @io
        if task.kind_of? Rake::Task
          tname = task.name.inspect
          tdesc = task.comment
        else
          tname = ""
          tdesc = ""
        end
        host = '"'+host+'"' if @re_escape =~ host
        _puts [id, tname, tdesc, cmd.inspect,
               format_time(start_time),
               format_time(end_time),
               "%.3f" % (end_time-start_time),
               host, status].join(@separator)
      end
      if status==""
        1
      elsif @gnu_time
        /^([^,]*),/ =~ status
        Integer($1)
      else
        Integer(status)
      end
    end

    def self.parse_time(s)
      /(\d+)\D(\d+)\D(\d+)\D(\d+)\D(\d+)\D(\d+)\.(\d+)/ =~ s
      a = [$1,$2,$3,$4,$5,$6,$7].map{|x| x.to_i}
      Time.new(*a[0..5],"+00:00") + a[6]*0.001
    end

    def self.plot_parallelism(file)
      require "csv"

      base = File.basename(file,".csv")
      fout = base+".dat"

      a = []
      start_time = nil

      CSV.foreach(file) do |l|
        if l[3] == 'pwrake_profile_start'
          start_time = parse_time(l[4]+" +0000")
        elsif l[3] == 'pwrake_profile_end'
          t = parse_time(l[4]+" +0000") - start_time
          a << [t,0]
        elsif start_time
          t = parse_time(l[4]+" +0000") - start_time
          a << [t,+1]
          t = parse_time(l[5]+" +0000") - start_time
          a << [t,-1]
        end
      end

      return if a.size < 4

      a = a.sort{|x,y| x[0]<=>y[0]}

      level = 0

      n = a.size
      i = 0
      y = 0
      y_max = 0

      File.open(fout,"w") do |f|
        begin
          t = 0
          y_pre = 0
          n.times do |i|
            if a[i][0]-t > 0.001
              f.printf "%.3f %d\n", t, y_pre
              t = a[i][0]
              f.printf "%.3f %d\n", t, y
            end
            y += a[i][1]
            y_pre = y
            y_max = y if y > y_max
          end
        rescue
          p a[i]
        end
      end

      t_end = (a.last)[0]

      IO.popen("gnuplot","r+") do |f|
        f.puts "
set terminal png
set output '#{base}.png'
set rmargin 7
set xlabel 'time (sec)'
set ylabel 'parallelism'
set arrow 1 from #{t_end},#{y_max*0.5} to #{t_end},0 linecolor rgb 'green'
set label 1 at first #{t_end},#{y_max*0.5} \" #{t_end}\\n end\" textcolor rgb 'green'

plot '#{fout}' w l
"
      end

      puts "Parallelism plot: #{base}.png"
    end

  end
end
