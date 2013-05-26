module Pwrake

  class Profiler

    HEADER_FOR_PROFILE =
      %w[id task command start end elap host status]

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
          #tdesc = task.comment
        else
          tname = ""
          #tdesc = ""
        end
        host = '"'+host+'"' if @re_escape =~ host
        _puts [id, tname, cmd.inspect,
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

  class << self

    def parse_time(s)
      /(\d+)\D(\d+)\D(\d+)\D(\d+)\D(\d+)\D(\d+)\.(\d+)/ =~ s
      a = [$1,$2,$3,$4,$5,$6,$7].map{|x| x.to_i}
      Time.new(*a[0..5],"+00:00") + a[6]*0.001
    end

    def count_start_end_from_csv(file)
      require "csv"
      a = []
      start_time = nil

      CSV.foreach(file,:headers=>true) do |row|
        if row['command'] == 'pwrake_profile_start'
          start_time = parse_time(row[4]+" +0000")
        elsif row['command'] == 'pwrake_profile_end'
          t = parse_time(row['start']+" +0000") - start_time
          a << [t,0]
        elsif start_time
          t = parse_time(row['start']+" +0000") - start_time
          a << [t,+1]
          t = parse_time(row['end']+" +0000") - start_time
          a << [t,-1]
        end
      end

      a.sort{|x,y| x[0]<=>y[0]}
    end

    def exec_density(a)
      reso = 0.1
      delta = 1/reso
      t_end = (a.last)[0]
      n = (t_end/reso).to_i + 1
      i = 0
      t_next = reso
      d = (n+1).times.map{|i| [reso*i,0]}
      a.each do |x|
        while d[i+1][0] <= x[0]
          i += 1
        end
        if x[1] == 1
          d[i][1] += delta
        end
      end
      d
    end

    def plot_parallelism(file)
      a = count_start_end_from_csv(file)
      return if a.size < 4

      density = exec_density(a)

      base = File.basename(file,".csv")
      fpara = base+"_para.dat"

      n = a.size
      i = 0
      y = 0
      y_max = 0

      File.open(fpara,"w") do |f|
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
#set rmargin 10
set title '#{base}'
set xlabel 'time (sec)'
set ylabel 'parallelism'

set arrow 1 from #{t_end},#{y_max*0.5} to #{t_end},0 linecolor rgb 'blue'
set label 1 at first #{t_end},#{y_max*0.5} right \"#{t_end}\\nsec\" textcolor rgb 'blue'

plot '#{fpara}' w l axis x1y1 title 'parallelism'
"
      end

      puts "Parallelism plot: #{base}.png"
    end


    def plot_parallelism2(file)
      a = count_start_end_from_csv(file)
      return if a.size < 4

      density = exec_density(a)

      base = File.basename(file,".csv")
      fpara = base+"_para.dat"
      fdens = base+'_dens.dat'
      fimg = base+'.png'

      File.open(fdens,"w") do |f|
        density.each do |t,d|
          f.puts "#{t} #{d}"
        end
      end

      n = a.size
      i = 0
      y = 0
      y_max = 0

      File.open(fpara,"w") do |f|
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
set output '#{fimg}'
#set rmargin 10
set title '#{base}'
set xlabel 'time (sec)'
set ylabel 'parallelism'
set y2tics
set ytics nomirror
set y2label 'exec/sec'

set arrow 1 from #{t_end},#{y_max*0.5} to #{t_end},0 linecolor rgb 'blue'
set label 1 \"#{t_end}\\nsec\" at first #{t_end},#{y_max*0.5} right front textcolor rgb 'blue'

plot '#{fpara}' w l axis x1y1 title 'parallelism', '#{fdens}' w l axis x1y2 title 'exec/sec'
"
      end

      puts "Parallelism plot: #{fimg}"
      fimg
    end

  end
  end
end
