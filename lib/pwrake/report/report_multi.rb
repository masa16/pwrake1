module Pwrake

  class ReportMulti

    def initialize(list,pattern)
      @reports = list.map do |file,ncore|
        Report.new(file,ncore,pattern)
      end
      @pattern = pattern
      @elap_png = 'elap.png'
    end

    def report(stat_html)
      if true
        @reports.each do |r|
          r.report_html
        end
        plot_elap
      end
      html = Report::HTML_HEAD + "<body><h1>Pwrake Statistics</h1>\n"
      html << "<h2>Log files</h2>\n"
      html << "<table>\n"
      html << "<tr><th>log file</th><th>ncore</th><th>elapsed time(sec)</th><tr>\n"
      @reports.each do |r|
        html << "<tr><td><a href='#{r.html_file}'>#{r.base}</a></td>"
        html << "<td>#{r.ncore}</td><td>#{r.elap}</td><tr>\n"
      end
      html << "</table>\n"
      html << "<h2>Elapsed time</h2>\n"
      html << "<img src='#{@elap_png}'  align='top'/></br>\n"

      html << "<h2>Histogram of Execution time</h2>\n"
      html << report_histogram()
      html << "</body></html>\n"

      File.open(stat_html,"w") do |f|
        f.puts html
      end
    end

    def plot_elap
      a = @reports.map{|r| r.ncore * r.elap}.min
      ymin = @reports.map{|r| r.elap}.min
      ymin = 10**(Math.log10(ymin).floor)
      ymax = 10**(Math.log10(ymin).floor+2)
      IO.popen("gnuplot","r+") do |f|
        f.puts "
set terminal png size 640,480
set output '#{@elap_png}'
set xlabel 'ncore'
set ylabel 'time (sec)'
set yrange [#{ymin}:#{ymax}]
set logscale xy
plot #{a}/x,'-' w lp lw 2 ps 2 title 'elapsed time'
"
        @reports.sort_by{|r| r.ncore}.each do |r|
          f.puts "#{r.ncore} #{r.elap}"
        end
        f.puts "e"
      end
      puts "Ncore-time plot: "+@elap_png
    end

    def report_histogram
      @images = {}
      @stats = {}

      @reports.each do |r|
        r.cmd_stat.each do |cmd,stat|
          if stat.n > 2
            @stats[cmd] ||= {}
            @stats[cmd][r.ncore] = stat
          end
        end
      end

      @stats.each_key do |cmd|
        @images[cmd] = 'hist_'+cmd+'.png'
      end
      histogram_plot
      histogram_html
    end

    def histogram_html
      html = ""
      @stats.each do |cmd,stats|
        html << "<p>Statistics of Elapsed time of #{cmd}</p>\n<table>\n"
        html << "<th>ncore</th>"+Stat.html_th
        stats.each do |ncore,s|
          html << "<tr><td>%i</td>" % ncore + s.html_td + "</tr>\n"
        end
        html << "</table>\n"
        html << "<img src='./#{@images[cmd]}'/>\n"
      end
      html
    end

    def histogram_plot
      @stats.each do |cmd,stats|
        IO.popen("gnuplot","r+") do |f|
          f.puts "
set terminal png # size 480,360
set output '#{@images[cmd]}'
set ylabel 'histogram'
set xlabel 'Execution time (sec)'
set logscale x
set title '#{cmd}'"
          a = []
          ncores = stats.keys
          ncores.each_with_index{|n,i|
            a << "'-' w histeps ls #{i+1} title ''"
            a << "'-' w lines ls #{i+1} title '#{n} cores'"
          }
          f.puts "plot "+ a.join(',')

          stats.each do |ncore,s|
            2.times do
              s.hist_each do |x1,x2,y|
                x = Math.sqrt(x1*x2)
                f.printf "%f %f\n", x, y
              end
              f.puts "e"
            end
          end
        end
        puts "Histogram plot: #{@images[cmd]}"
      end
    end

    def histogram_plot2
      @stats.each do |cmd,stats|
        IO.popen("gnuplot","r+") do |f|
          f.puts "
set terminal png # size 480,360
set output '#{@images[cmd]}'
set nohidden3d
set palette rgb 33,13,10
set pm3d
set ticslevel 0
unset colorbox
set yrange [#{stats.size}:0]
set logscale x
set title '#{cmd}'"
          a = []
          ncores = stats.keys.sort
          ncores.each_with_index{|n,i|
            a << "'-' w lines ls #{i+1} title '#{n} cores'"
          }
          f.puts "splot "+ a.join(',')

          ncores.each_with_index do |ncore,i|
            s = stats[ncore]
            y = i
            s.hist_each do |x1,x2,z|
              f.printf "%g %g 0\n", x1,y
              f.printf "%g %g 0\n", x2,y
              f.printf "%g %g 0\n", x2,y
            end
            f.puts ""
            s.hist_each do |x1,x2,z|
              f.printf "%g %g %g\n", x1,y,z
              f.printf "%g %g %g\n", x2,y,z
              f.printf "%g %g 0\n", x2,y,z
            end
            f.puts ""
            y = i+1
            s.hist_each do |x1,x2,z|
              f.printf "%g %g %g\n", x1,y,z
              f.printf "%g %g %g\n", x2,y,z
              f.printf "%g %g 0\n", x2,y,z
            end
            f.puts ""
            s.hist_each do |x1,x2,z|
              f.printf "%g %g 0\n", x1,y
              f.printf "%g %g 0\n", x2,y
              f.printf "%g %g 0\n", x2,y
            end
            f.puts "e"
            i = i+1
          end
        end
        puts "Histogram plot: #{@images[cmd]}"
      end
    end

  end
end

