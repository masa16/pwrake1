module Pwrake

  module Parallelism
    module_function

    def count_start_end_from_csv(file)
      a = []
      start_time = nil

      CSV.foreach(file,:headers=>true) do |row|
        if row['command'] == 'pwrake_profile_start'
          start_time = Time.parse(row['start_time'])
        elsif row['command'] == 'pwrake_profile_end'
          t = Time.parse(row['start_time']) - start_time
          a << [t,0]
        elsif start_time
          t = Time.parse(row['start_time']) - start_time
          a << [t,+1]
          t = Time.parse(row['end_time']) - start_time
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

      base = file.sub(/\.csv$/,"")
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

      #puts "Parallelism plot: #{base}.png"
    end


    def plot_parallelism2(file)
      a = count_start_end_from_csv(file)
      return if a.size < 4

      density = exec_density(a)

      base = file.sub(/\.csv$/,"")
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

      #puts "Parallelism plot: #{fimg}"
      fimg
    end

    def read_time_by_host_from_csv(csvtable)
      a = {}
      start_time = nil

      csvtable.each do |row|
        host = row['host']
        if row['command'] == 'pwrake_profile_start'
          start_time = Time.parse(row['start_time'])
        elsif row['command'] == 'pwrake_profile_end'
          t = Time.parse(row['start_time']) - start_time
          a.each do |h,v|
            v << [t,0]
          end
        elsif start_time
          a[host] ||= []
          t = Time.parse(row['start_time']) - start_time
          a[host] << [t,+1]
          t = Time.parse(row['end_time']) - start_time
          a[host] << [t,-1]
        end
      end
      a
    end

    def timeline_to_grid(a)
      resolution = Rational(1,10)

      a = a.sort{|x,y| x[0]<=>y[0]}
      t_end = (a.last)[0]

      ngrid = (t_end/resolution).floor
      grid = [[0,0]]

      j = 0
      a.each do |x|
        i = (x[0]/resolution).floor
        while j < i
          grid[j+1] = [j*resolution,grid[j][1]]
          j += 1
        end
        grid[i][1] += x[1]
      end
      return grid
    end

    def plot_parallelizm_by_host(csvtable,base)
      fpng = base+"_para_host.png"
      data = read_time_by_host_from_csv(csvtable)
      return fpng if data.size == 0

      grid = []
      hosts = data.keys.sort
      hosts.each do |h|
        a = timeline_to_grid(data[h])
        grid << a
      end

      IO.popen("gnuplot","r+") do |f|
        f.puts "
set terminal png
set output '#{fpng}'
#set rmargin 7
set lmargin 16
set pm3d map
set pm3d corners2color c1
set xlabel 'time (sec)'
set ytics nomirror
set ticslevel 0
set format y ''
"
        hosts.each_with_index do |h,i|
          if /^([^.]+)\./ =~ h
            h = $1
          end
          f.puts "set ytics add ('#{h}' #{i+0.5})"
        end
        f.puts "splot '-' using 2:1:3 with pm3d title ''"

        grid.each_with_index do |a,j|
          a.each do |x|
            f.printf "%g %g %d\n", j, x[0], x[1]
          end
          f.printf "\n"
        end
        j = grid.size
        grid.last.each do |x|
          f.printf "%g %g %d\n", j, x[0], x[1]
        end
        f.printf "e\n"
      end
      fpng
    end

  end
end
