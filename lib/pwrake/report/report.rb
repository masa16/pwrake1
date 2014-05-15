module Pwrake

  class Report

    HTML_HEAD = <<EOL
<html><head><style>
<!--
h2 {
  background-color:#eee;
}
h1 {
  background-color:#0ff;
}
table {
 margin:0px;
 border-style:solid;
 border-width:1px;
}
td {
  margin:0px;
  border-style:solid;
  border-width:1px;
}
-->
</style>
</head>
<body>
EOL

    @@id = 0
    @@id_fmt = nil

    def initialize(base,pattern)
      @base = base
      @pattern = pattern

      @@id = @@id.succ
      @id = @@id

      @csv_file = base+'.csv'
      @task_file = base+'.task'
      @html_file = base+'.html'

      open(@base+".log","r").each do |s|
        if /num_cores=(\d+)/ =~ s
          @ncore = $1.to_i
          break
        end
      end

      begin
        @sh_table = CSV.read(@csv_file,:headers=>true)
      rescue
        $stderr.puts "error in reading "+@csv_file
        $stderr.puts $!, $@
        exit
      end

      h = {}
      @elap_sum = 0
      @sh_table.each do |row|
        if host = row['host']
          h[host] = true
        end
        @elap_sum += row['elap_time'].to_f
      end
      @hosts = h.keys.sort
      @start_time = Time.parse(@sh_table[0]["start_time"])
      @end_time = Time.parse(@sh_table[-1]["start_time"])
      @elap = @end_time - @start_time
      read_elap_each_cmd
      make_cmd_stat

      @stat = TaskStat.new(@task_file,@sh_table)
    end

    attr_reader :base, :ncore, :elap
    attr_reader :csv_file, :html_file
    attr_reader :cmd_elap, :cmd_stat
    attr_reader :sh_table, :task_table
    attr_reader :id

    def id_str
      if @@id_fmt.nil?
        id_len = Math.log10(@@id).floor + 1
        @@id_fmt = "#%0#{id_len}d"
      end
      @@id_fmt % @id
    end

    def read_elap_each_cmd
      @cmd_elap = {}
      @sh_table.each do |row|
        command = row['command']
        elap = row['elap_time']
        if command && elap
          elap = elap.to_f
          found = nil
          @pattern.each do |cmd,regex|
            if regex =~ command
              if a = @cmd_elap[cmd]
                a << elap
              else
                @cmd_elap[cmd] = [elap]
              end
              found = true
            end
          end
          if !found
            if cmd = get_command( command )
              if a = @cmd_elap[cmd]
                a << elap
              else
                @cmd_elap[cmd] = [elap]
              end
            end
          end
        end
      end
      @cmd_elap
    end

    def get_command(s)
      if /\(([^()]+)\)/ =~ s
        s = $1
      end
      a = s.split(/;/)
      a.each do |x|
        if /^\s*(\S+)/ =~ x
          k = $1
          next if k=='cd'
          return k
        end
      end
      nil
    end

    def make_cmd_stat
      @cmd_stat = {}
      @cmd_elap.each do |cmd,elap|
        @cmd_stat[cmd] = s = Stat.new(elap)
        if elap.size > 1
          s.make_logx_histogram(1.0/8)
        end
      end
    end

    def format_comma(x)
      x.to_s.gsub(/(?<=\d)(?=(?:\d\d\d)+(?!\d))/, ',')
    end

    def tr_count(x,y)
      sum = x+y
      xp = x*100.0/sum
      yp = y*100.0/sum
      td = "<td align='right' valign='top'>"
      m  = td + '%s<br/>''%.2f%%</td>' % [format_comma(x),xp]
      m << td + '%s<br/>''%.2f%%</td>' % [format_comma(y),yp]
      m << td + "%s</td>" % format_comma(sum)
      m
    end

    def report_html
      html = HTML_HEAD + "<body><h1>Pwrake Statistics</h1>\n"
      html << "<h2>Workflow</h2>\n"
      html << "<table>\n"
      html << "<tr><th>log file</th><td>#{@base}</td><tr>\n"
      html << "<tr><th>ncore</th><td>#{@ncore}</td><tr>\n"
      html << "<tr><th>elapsed time</th><td>%.3f sec</td><tr>\n"%[@elap]
      html << "<tr><th>cumulative process time</th><td>%.3f sec</td><tr>\n"%[@elap_sum]
      html << "<tr><th>occupancy</th><td>%.3f %%</td><tr>\n"%[@elap_sum/@elap/@ncore*100]
      html << "<tr><th>start time</th><td>#{@start_time}</td><tr>\n"
      html << "<tr><th>end time</th><td>#{@end_time}</td><tr>\n"
      html << "</table>\n"
      html << "<table>\n"
      html << "<tr><th>hosts</th><tr>\n"
      @hosts.each do |h|
        html << "<tr><td>#{h}</td><tr>\n"
      end
      html << "</table>\n"
      html << "<h2>Parallelism</h2>\n"
      fimg = Parallelism.plot_parallelism2(@sh_table,@base)
      html << "<img src='./#{File.basename(fimg)}' align='top'/></br>\n"

      html << "<h2>Parallelism by command</h2>\n"
      fimg3 = Parallelism.plot_parallelism_by_pattern(@sh_table,@base,@pattern)
      html << "<img src='./#{File.basename(fimg3)}' align='top'/></br>\n"

      html << "<h2>Parallelism by host</h2>\n"
      fimg2 = Parallelism.plot_parallelism_by_host(@sh_table,@base)
      html << "<img src='./#{File.basename(fimg2)}' align='top'/></br>\n"

      html << "<h2>Command statistics</h2>\n"
      html << "<table>\n"
      html << "<tr><th>command</th>"
      html << Stat.html_th
      html << "</tr>\n"
      @cmd_stat.each do |cmd,stat|
        html << "<tr><td>#{cmd}</td>"
        html << stat.html_td
        html << "</tr>\n"
      end
      html << "<table>\n"
      html << "<img src='./#{File.basename(histogram_plot)}' align='top'/></br>\n"

      html << "<h2>Locality statistics</h2>\n"
      html << "<table>\n"

      html << "<tr><th></th><th rowspan=3>gross elapsed time (sec)</th><th></th>"
      html << "<th colspan=6>read</th>"
      html << "<th></th>"
      html << "<th colspan=6>write</th>"
      html << "</tr>\n"


      html << "<tr><th></th><th></th>"
      html << "<th colspan=3>count</th><th colspan=3>file size (bytes)</th>"
      html << "<th></th>"
      html << "<th colspan=3>count</th><th colspan=3>file size (bytes)</th>"
      html << "</tr>\n"

      html << "<tr><th>host</th><th></th>"
      html << "<th>local</th><th>remote</th><th>total</th>"
      html << "<th>local</th><th>remote</th><th>total</th>"
      html << "<th></th>"
      html << "<th>local</th><th>remote</th><th>total</th>"
      html << "<th>local</th><th>remote</th><th>total</th>"
      html << "</tr>\n"
      @stat.exec_hosts.each do |h|
        html << "<tr><td>#{h}</td>"
        html << "<td align='right'>%.3f</td>" % @stat[h,nil,:elap]
        html << "<td></td>"
        html << tr_count(@stat[h,true,:in_num],@stat[h,false,:in_num])
        html << tr_count(@stat[h,true,:in_size],@stat[h,false,:in_size])
        html << "<td></td>"
        html << tr_count(@stat[h,true,:out_num],@stat[h,false,:out_num])
        html << tr_count(@stat[h,true,:out_size],@stat[h,false,:out_size])
        html << "</tr>\n"
      end
      html << "<tr><td>total</td>"
      html << "<td align='right'>%.3f</td>" % @stat.total(nil,:elap)
      html << "<td></td>"
      html << tr_count(@stat.total(true,:in_num),@stat.total(false,:in_num))
      html << tr_count(@stat.total(true,:in_size),@stat.total(false,:in_size))
      html << "<td></td>"
      html << tr_count(@stat.total(true,:out_num),@stat.total(false,:out_num))
      html << tr_count(@stat.total(true,:out_size),@stat.total(false,:out_size))

      html << "</tr>\n"
      html << "<table>\n"

      html << "</body></html>\n"
      File.open(@html_file,"w") do |f|
        f.puts html
      end
      #puts "generate "+@html_file

      printf "%s,%d,%d,%d,%d\n",@html_file, @stat.total(true,:in_num),@stat.total(false,:in_num),@stat.total(true,:in_size),@stat.total(false,:in_size)
    end


    def histogram_plot
      command_list = []
      @cmd_stat.each do |cmd,stat|
        if stat.n > 2
          command_list << cmd
        end
      end
      hist_image = @base+"_hist.png"
      IO.popen("gnuplot","r+") do |f|
        f.puts "
set terminal png # size 480,360
set output '#{hist_image}'
set ylabel 'histogram'
set xlabel 'Execution time (sec)'
set logscale x
set title 'histogram of elapsed time'"
        a = []

        command_list.each_with_index do |n,i|
          a << "'-' w histeps ls #{i+1} title ''"
          a << "'-' w lines ls #{i+1} title '#{n}'"
        end
        f.puts "plot "+ a.join(',')

        command_list.each do |cmd|
          stat = @cmd_stat[cmd]
          2.times do
            stat.hist_each do |x1,x2,y|
              x = Math.sqrt(x1*x2)
              f.printf "%f %d\n", x, y
            end
            f.puts "e"
          end
        end
        hist_image
      end
    end
  end

  class TaskStat

    def initialize(task_file, sh_table)
      begin
        @task_table = CSV.read(task_file,:headers=>true)
      rescue
        $stderr.puts "error in reading "+task_file
        $stderr.puts $!, $@
        exit
      end
      @count = Hash.new(0)
      task_locality
      stat_sh_table(sh_table)
    end

    attr_reader :exec_hosts

    def count(exec_host, loc, key, val)
      @count[[exec_host,loc,key]] += val
      @count[[loc,key]] += val
    end

    def total(loc,key)
      @count[[loc,key]]
    end

    def [](exec_host,loc,key)
      @count[[exec_host,loc,key]]
    end

    def task_locality
      file_size = {}
      file_host = {}
      h = {}
      @task_table.each do |row|
        name            = row['task_name']
        file_size[name] = row['file_size'].to_i
        file_host[name] = (row['file_host']||'').split('|')
        h[row['exec_host']] = true
      end
      @exec_hosts = h.keys.sort

      @task_table.each do |row|
        if row['executed']=='1'
          name      = row['task_name']
          exec_host = row['exec_host']
          loc = file_host[name].include?(exec_host)
          count(exec_host, loc, :out_num, 1)
          count(exec_host, loc, :out_size, file_size[name])

          preq_files = (row['preq']||'').split('|')
          preq_files.each do |preq|
            sz = file_size[preq]
            if sz && sz > 0
              loc = file_host[preq].include?(exec_host)
              count(exec_host, loc, :in_num, 1)
              count(exec_host, loc, :in_size, sz)
            end
          end
        end
      end
    end

    def stat_sh_table(sh_table)
      sh_table.each do |row|
        if (h = row['host']) && (t = row['elap_time'])
          count(h, nil, :elap, t.to_f)
        end
      end
    end

  end
end
