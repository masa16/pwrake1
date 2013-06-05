module Pwrake

  class Report

    class << self
      def parse_time(s)
        /(\d+)\D(\d+)\D(\d+)\D(\d+)\D(\d+)\D(\d+)\.(\d+)/ =~ s
        a = [$1,$2,$3,$4,$5,$6,$7].map{|x| x.to_i}
        Time.new(*a[0..5],"+00:00") + a[6]*0.001
      end
    end

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

    def initialize(base,ncore,pattern)
      @base = base
      @csv_file = base+'.csv'
      @task_file = base+'.task'
      @html_file = base+'.html'
      @ncore = ncore

      @sh_table = CSV.read(@csv_file,:headers=>true)
      h = {}
      @sh_table.each do |row|
        if host = row['host']
          h[host] = true
        end
      end
      @hosts = h.keys.sort
      @start_time = Report.parse_time(@sh_table[0]["start"])
      @end_time = Report.parse_time(@sh_table[-1]["start"])
      @elap = @end_time - @start_time
      @pattern = pattern
      read_elap_each_cmd
      make_cmd_stat
    end

    attr_reader :base, :ncore, :elap
    attr_reader :csv_file, :html_file
    attr_reader :cmd_elap, :cmd_stat
    attr_reader :sh_table, :task_table

    def read_elap_each_cmd
      @cmd_elap = {}
      @sh_table.each do |row|
        command = row['command']
        elap = row['elap']
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
      td = "<td align='right' valign='top'>"
      m  = td + "%s<br/>%.2f%%</td>" % [format_comma(x),x*100.0/sum]
      m << td + "%s<br/>%.2f%%</td>" % [format_comma(y),y*100.0/sum]
      m << td + "%s</td>" % format_comma(sum)
      m
    end

    def report_html
      html = HTML_HEAD + "<body><h1>Pwrake Statistics</h1>\n"
      html << "<h2>Workflow</h2>\n"
      html << "<table>\n"
      html << "<tr><th>log file</th><td>#{@base}</td><tr>\n"
      html << "<tr><th>ncore</th><td>#{@ncore}</td><tr>\n"
      html << "<tr><th>elapsed time(sec)</th><td>#{@elap}</td><tr>\n"
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
      fimg = Parallelism.plot_parallelism2(@csv_file)
      html << "<img src='#{fimg}' align='top'/></br>\n"

      html << "<h2>Parallelism by host</h2>\n"
      fimg2 = Parallelism.plot_parallelizm_by_host(@sh_table,@base)
      html << "<img src='#{fimg2}' align='top'/></br>\n"

      html << "<h2>Command statistics</h2>\n"
      html << "<table>\n"
      html << "<tr><th>command</th>"
      html << Stat.html_th
      html << "</tr>\n"
      @cmd_stat.each do |cmd,s|
        html << "<tr><td>#{cmd}</td>"
        html << s.html_td
        html << "</tr>\n"
      end
      html << "<table>\n"

      task_locality
      html << "<h2>Locality statistics</h2>\n"
      html << "<table>\n"

      html << "<tr><th></th><th rowspan=3>gross elapsed time (sec)</th><th></th>"
      html << "<th colspan=6>read</th>"
      html << "<th></th>"
      html << "<th colspan=6>write</th>"
      html << "</tr>\n"


      html << "<tr><th></th><th></th>"
      html << "<th colspan=3>num of times</th><th colspan=3>file size (bytes)</th>"
      html << "<th></th>"
      html << "<th colspan=3>num of times</th><th colspan=3>file size (bytes)</th>"
      html << "</tr>\n"

      html << "<tr><th>host</th><th></th>"
      html << "<th>local</th><th>remote</th><th>total</th>"
      html << "<th>local</th><th>remote</th><th>total</th>"
      html << "<th></th>"
      html << "<th>local</th><th>remote</th><th>total</th>"
      html << "<th>local</th><th>remote</th><th>total</th>"
      html << "</tr>\n"
      n_same_input     = 0
      size_same_input  = 0
      n_diff_input     = 0
      size_diff_input  = 0
      n_same_output    = 0
      size_same_output = 0
      n_diff_output    = 0
      size_diff_output = 0
      elap_host = 0
      @exec_hosts.each do |h|
        html << "<tr><td>#{h}</td>"
        html << "<td align='right'>%.3f</td>" % @elap_host[h]
        html << "<td></td>"
        html << tr_count(@n_same_input[h],@n_diff_input[h])
        html << tr_count(@size_same_input[h],@size_diff_input[h])
        html << "<td></td>"
        html << tr_count(@n_same_output[h],@n_diff_output[h])
        html << tr_count(@size_same_output[h],@size_diff_output[h])

        html << "</tr>\n"
        n_same_input     += @n_same_input[h]
        size_same_input  += @size_same_input[h]
        n_diff_input     += @n_diff_input[h]
        size_diff_input  += @size_diff_input[h]
        n_same_output    += @n_same_output[h]
        size_same_output += @size_same_output[h]
        n_diff_output    += @n_diff_output[h]
        size_diff_output += @size_diff_output[h]
        elap_host += @elap_host[h]
      end
      html << "<tr><td>total</td>"
      html << "<td align='right'>%.3f</td>" % elap_host
      html << "<td></td>"
      html << tr_count(n_same_input,n_diff_input)
      html << tr_count(size_same_input,size_diff_input)
      html << "<td></td>"
      html << tr_count(n_same_output,n_diff_output)
      html << tr_count(size_same_output,size_diff_output)

      html << "</tr>\n"
      html << "<table>\n"

      html << "</body></html>\n"
      File.open(@html_file,"w") do |f|
        f.puts html
      end
    end

    def task_locality
      @task_table = CSV.read(@task_file,:headers=>true)
      file_size = {}
      file_host = {}
      @task_table.each do |row|
        name = row['name']
        file_size[name] = row['file_size'].to_i
        file_host[name] = (row['file_host']||'').split('|')
      end

      @n_same_output   = Hash.new(0)
      @size_same_output= Hash.new(0)
      @n_diff_output   = Hash.new(0)
      @size_diff_output= Hash.new(0)
      @n_same_input    = Hash.new(0)
      @size_same_input = Hash.new(0)
      @n_diff_input    = Hash.new(0)
      @size_diff_input = Hash.new(0)
      h = {}
      @task_table.each do |row|
        if row['executed']=='1'
          name = row['name']
          exec_host = row['exec_host']
          h[exec_host] = true
          if file_host[name].include?(exec_host)
            @n_same_output[exec_host] += 1
            @size_same_output[exec_host] += file_size[name]
          else
            @n_diff_output[exec_host] += 1
            @size_diff_output[exec_host] += file_size[name]
          end
          preq_files = (row['preq']||'').split('|')
          preq_files.each do |preq|
            if (sz=file_size[preq]) && sz > 0
              if file_host[preq].include?(exec_host)
                @n_same_input[exec_host] += 1
                @size_same_input[exec_host] += sz
              else
                @n_diff_input[exec_host] += 1
                @size_diff_input[exec_host] += sz
              end
            end
          end
        end
      end
      @exec_hosts = h.keys.sort

      @elap_host = Hash.new(0)
      @sh_table.each do |row|
        if (h = row['host']) && (t = row['elap'])
          @elap_host[h] += t.to_f
        end
      end
    end

  end
end
