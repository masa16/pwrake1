require "metis"

module Pwrake

  module MCGP
    def graph_partition
      hosts = Pwrake.application.core_list.sort.uniq
      puts "hosts=#{hosts}"
      t1 = Time.now
      g = MetisGraph.new(hosts)
      g.trace
      g.part_graph
      g.set_part
      t2 = Time.now
      Pwrake::Log.info "Time for TOTAL Graph Partitioning: #{t2-t1} sec"
    end
    module_function :graph_partition
  end


  class MetisGraph

    def initialize(hosts)
      @hosts = hosts
      @n_part = @hosts.size
      @traced = {}

      @edges = []

      @vertex_name2id = {}
      @vertex_id2name = []
      @vertex_depth = {}

      @count = 0

      @depth_hist = [0]

      @gviz_nodes = []
      @gviz_edges = []
      @edge_list = []
      @input_files = {}

      @hosts.each do |host|
        push_vertex(host)
        @vertex_depth[host] = 0
      end
    end

    def trace( name = "default", target = nil )

      task = Rake.application[name]
      depth = 0

      #puts "#{task.class.inspect} #{name}"
      if task.kind_of?(Rake::FileTask)
        if File.file?(name)
          #puts "File.exist #{name}"
          @input_files[name] = (@input_files[name] || []) | [target]
          if task.location.nil?
            application.postprocess(task)
          end
          task.location.each do |host|
            if @hosts.include?(host)
              push_edge( host, target )
            end
          end
          @depth_hist[depth] += 1
          return depth
        else
          push_vertex( name )
          push_edge( name, target )
          target = name
        end
      end

      if !@traced[name]
        @traced[name] = true

        task.prerequisites.each do |prereq|
          d = trace( prereq, target )
          depth = d if d and d > depth
        end

        if task.kind_of?(Rake::FileTask)
          depth += 1
          @depth_hist[depth] = (@depth_hist[depth] || 0) + 1
        end

        @vertex_depth[name] = depth
      end

      return @vertex_depth[name]
    end

    def trim( name )
      name = name.to_s
      name = File.basename(name)
      name.sub(/H\d+/,'').sub(/object\d+/,"")
    end

    def push_vertex( name )
      if @vertex_name2id[name].nil?
        @vertex_name2id[name] = @count
        @vertex_id2name[@count] = name

        tag = "T#{@count}"
        @gviz_nodes[@count] = "#{tag} [label=\"#{trim(name)}\", shape=box, style=filled, fillcolor=\"%s\"];"

        @count += 1
      end
    end

    def push_edge( name, target )
      if target
        v1 = @vertex_name2id[name]
        v2 = @vertex_name2id[target]
        (@edges[v1] ||= []).push v2
        (@edges[v2] ||= []).push v1

        @gviz_edges.push "T#{v1} -> T#{v2};"
        @edge_list.push [v1,v2]
      end
    end

    def part_graph
      @xadj = [0]
      @adjcny = []
      @vwgt = []
      map_depth = []
      uvb = []
      c = 0
      @depth_hist.each do |x|
        if x and x>=@n_part
          map_depth << c
          c += 1
          uvb << 1 + 2.0*@n_part/x
          #uvb << ((x >= @n_part) ? 1.05 : 1.5)
        else
          map_depth << nil
        end
      end

      Pwrake::Log.info @depth_hist.inspect
      Pwrake::Log.info [c, map_depth].inspect
      Pwrake::Log.info uvb.inspect


      @count.times do |i|
        @adjcny.concat(@edges[i].sort) if @edges[i]
        @xadj.push(@adjcny.size)

        depth = @vertex_depth[@vertex_id2name[i]]
        w = Array.new(c,0)
        if j = map_depth[depth]
          w[j] = 1
        end
        @vwgt.push(w)
        #p [@vertex_id2name[i],w]
      end
      [@xadj, @adjcny, @vwgt]

      t1 = Time.now
      tpw = Array.new(@n_part,1.0/@n_part)
      sum = 0.0; tpw.each{|x| sum+=x}
      if false
        puts "@xadj.size=#{@xadj.size}"
        puts "@adjcny.size/2=#{@adjcny.size/2}"
        puts "tpw.sum=#{sum}"
        puts "@xadj=#{@xadj.inspect}"
        puts "@adjcny=#{@adjcny.inspect}"
        puts "@vwgt=#{@vwgt.inspect}"
      end
      @part = Metis.mc_part_graph_recursive2(c,@xadj,@adjcny, @vwgt,nil, tpw)
      #@part = Metis.mc_part_graph(c,@xadj,@adjcny, @vwgt,nil, [1.03]*c, @n_part)
      #@part = Metis.mc_part_graph_kway(c,@xadj,@adjcny, @vwgt,nil, [1.05]*c, @n_part)
      #@part = Metis.mc_part_graph_kway(c,@xadj,@adjcny, @vwgt,nil, uvb, @n_part)
      t2 = Time.now
      Pwrake::Log.info "Time for Graph Partitioning: #{t2-t1} sec"
      #p @part
    end


    def set_part
      @vertex_id2name.each_with_index do |name,idx|
        if idx >= @n_part
          i_part = @part[idx]
          task = Rake.application[name]
          host = @hosts[i_part]
          task.suggest_location = [host]
          #puts "task=#{task.inspect}, i_part=#{i_part}, host=#{host}"
        end
      end
      @input_files.each do |file,targets|
        host_list = {}
        targets.each do |name|
          host = @hosts[@part[@vertex_name2id[name]]]
          host_list[host] = true
        end
        host_list.each_key do |host|
          cmd="gfrep -N 1 -D #{host} #{file}"
          puts cmd
          system cmd
        end
      end
    end


    def part=(pg)
      @part=pg
    end

    def p_vertex
      @count.times do |i|
        if @vertex_weight[i] > 0
          puts "#{@vertex_weight[i]} #{@part[i]} #{@vertex_names[i]}"
        end
      end
    end

  end # class MetisGraph

end
