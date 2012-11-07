module Metis

  @@command = "pmetis"

  def self.command=(a)
    @@command=a
  end

  def self.command
    @@command
  end

  class Vertex
    def initialize( task, index )
      case task
      when Rake::FileTask
        @name = task.name
        @weight = task.weight || 0
        @kind = :file
        @e_weight = task.weight
      when Rake::Task
        @name = task.name
        @weight = task.weight || 0
        @kind = :task
        @e_weight = 0
      else
        @name = "#{task}"
        @weight = 0
        @kind = :job
        @e_weight = 0
      end
      @index = index
      @adj = []
      @task = task
      #p [@weight,@name]
    end
    attr_reader :weight, :kind, :name, :index, :adj, :task, :e_weight
    attr_reader :pre, :post

    def add_adj( vertex, weight )
      @adj.push( [vertex, weight] )
    end

    def empty_task?
      @task.kind_of?(Rake::Task) && @task.actions.empty? && !@task.prerequisites.empty?
    end

    def metis_input
      a = ["#{@weight}"]
      @adj.each{|v, wei|
        a << "#{v.index+1} #{wei}"
      }
      a.join(" ")
    end

    def inspect
      "<Vertex: '#{@name}'>"
    end
  end

  class VertexList
    include Enumerable
    def initialize # ( tasks )
      @list = []
      @index = {}
      @edges = []
      @n_edges = 0
      dag_trace
    end
    attr_accessor :edges

    def [](index)
      @list[index]
    end

    def each
      @list.each{|x| yield x}
    end

    # Trace the task if it is needed.  Prerequites are traced first.
    def dag_trace
      @dag_traced = {}
      Rake.application.top_level_tasks.each { |task_name| dag_trace_task(task_name) }
      @dag_traced = nil
    end

    def dag_trace_task( task_name )
      task = Rake.application[task_name]
      return if @dag_traced[task]
      @dag_traced[task] = true
      v_weight = 0
      task.prerequisites.each do |prereq_name|
        prereq = Rake.application[prereq_name]
        add_edge( prereq, task )
      end
      task.prerequisites.each do |prereq_name|
        dag_trace_task( prereq_name )
      end
    end

    def add_vertex( x )
      unless @index[x]
        i = @list.size
        v = Vertex.new(x,i)
        @index[x] = i
        @list << v
      end
    end

    def add_edge( src, dst )
      if src.weight>0 && dst.weight>0
        add_vertex( src )
        add_vertex( dst )
        dst_idx = @index[dst]
        src_idx = @index[src]
        dst_vtx = @list[dst_idx]
        src_vtx = @list[src_idx]
        edge_weight = (src.weight>0 && dst.weight>0) ? 1 : 0
        edge_weight = 1
        edge_weight = src.weight * dst.weight
        if edge_weight > 0
          @list[dst_idx].add_adj( @list[src_idx], edge_weight )
          @list[src_idx].add_adj( @list[dst_idx], edge_weight )
          @edges << [src_vtx,dst_vtx]
        end
      end
    end

    def partition(n)
      open("metis.graph","w") do |w|
        w.puts "#{@list.size} #{@edges.size} 11"
        @list.each do |v|
          #w.puts v.adjacent.map{|idx,wei| "#{idx+1} #{wei}"}.join(" ")
          w.puts v.metis_input
        end
      end
      t = Time.now
      r = system " #{Metis.command} metis.graph #{n}"
      puts "#{Metis.command} : #{Time.now - t} sec"
      $logger.finish("pmetis[end]",t) if $logger
      puts "exit status=#{r}"
      #raise "metis error" if r
      @part = IO.readlines("metis.graph.part.#{n}").map{|x| x.to_i}
    end

  end
end
