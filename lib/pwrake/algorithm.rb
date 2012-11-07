require "thread"
#require "pp"

module Pwrake

  class Tracer
    include Log

    def initialize
      @mutex = Mutex.new
      @fetched = {}
    end

    def available_task( root )
      @footprint = {}
      @fetched_tasks = []
      @mutex.synchronize do
        tm = timer("trace")
        status = find_task( root, [] )
        msg = [ "num_tasks=%i" % [@fetched_tasks.size] ]
        tk = @fetched_tasks[0]
        msg << "task[0]=%s" % tk.name.inspect if tk.kind_of?(Rake::Task)
        tm.finish(msg.join(' '))
        if status
          return @fetched_tasks
        else
          return nil
        end
      end
    end

    def find_task( tsk, chain )
      name = tsk.name

      if tsk.already_invoked
        return nil
      end

      if chain.include?(name)
        fail RuntimeError, "Circular dependency detected: #{chain.join(' => ')} => #{name}"
      end

      if @footprint[name] || @fetched[name]
        return :traced
      end
      @footprint[name] = true

      chain.push(name)
      prerequisites = tsk.prerequisites
      all_invoked = true
      i = 0
      while i < prerequisites.size
        prereq = tsk.application[prerequisites[i], tsk.scope]
        if find_task( prereq, chain )
          all_invoked = false
        end
        i += 1
      end
      chain.pop

      if all_invoked
        @fetched[name] = true
        if tsk.needed?
          @fetched_tasks << tsk
        else
          tsk.already_invoked = true
          return nil
        end
      end

      :fetched
    end
  end



  class Operator
    include Log

    def initialize
      Thread.abort_on_exception = true
      connections = Pwrake.manager.connection_list
      @scheduler = Pwrake.manager.scheduler_class.new
      log "@scheduler.class = #{@scheduler.class}"
      log "@scheduler.queue_class = #{@scheduler.queue_class}"
      @input_queue = @scheduler.queue_class.new(Pwrake.manager.core_list)
      @scheduler.on_start
      @workers = []
      connections.each_with_index do |conn,j|
        @workers << Thread.new(conn,j) do |c,i|
          begin
            thread_loop(c,i)
          ensure
            log "-- worker[#{i}] ensure : closing #{conn.host}"
            conn.close
          end
        end
      end
      @tracer = Tracer.new
    end

    def thread_loop(conn,i)
      Thread.current[:connection] = conn
      Thread.current[:id] = i
      host = conn.host
      standard_exception_handling do
        while tsk = @input_queue.pop(host)
          tm = timer("task","worker##{i} task=#{tsk}")
          tsk = @scheduler.on_execute(tsk)
          tsk.already_invoked = true
          @scheduler.on_task_start(tsk)
          tsk.execute #if tsk.needed?
          @scheduler.on_task_end(tsk)
          tsk.output_queue.push(tsk)
          tm.finish("worker##{i} task=#{tsk}")
        end
        # log "-- worker[#{i}] loopout : #{tsk}" if defined? tsk
      end
      # @scheduler.on_thread_end
    end

    # Provide standard execption handling for the given block.
    def standard_exception_handling
      begin
        yield
      rescue SystemExit => ex
        # Exit silently with current status
        @input_queue.stop
        raise
      rescue OptionParser::InvalidOption => ex
        # Exit silently
        @input_queue.stop
        exit(false)
      rescue Exception => ex
        # Exit with error message
        name = "pwrake"
        $stderr.puts "#{name} aborted!"
        $stderr.puts ex.message
        if Rake.application.options.trace
          $stderr.puts ex.backtrace.join("\n")
        else
          $stderr.puts ex.backtrace.find {|str| str =~ /#{@rakefile}/ } || ""
          $stderr.puts "(See full trace by running task with --trace)"
        end
        @input_queue.stop
        exit(false)
      end
    end

    def invoke(root, args)
      log "--- Task # invoke #{root.inspect}, #{args.inspect} thread=#{Thread.current.inspect}"
      if conn = Thread.current[:connection]
        j = Thread.current[:id]
        thread = Thread.new(conn,j) {|c,i|
          log "-- new worker[#{i}] created"
          thread_loop(c,i)
        }
      else
        thread = nil
      end
      output_queue = Queue.new
      while a = @tracer.available_task(root)
        a = @scheduler.on_trace(a)
        a.each{|tsk| tsk.output_queue = output_queue }
        @input_queue.push(a)
        a.each do |x|
          b = output_queue.pop
          if !a.include?(b)
            puts "b= #{b.class.inspect}"
            p b
            a.each_with_index do |x,i|
              puts "a[#{i}]="
              puts "#{a[i].class.inspect}"
              p a[i]
            end
            raise "b is not included in a"
          end
        end
      end
      log "--- End of Task # invoke #{root.inspect}, #{args.inspect} thread=#{Thread.current.inspect}"
      if thread
        @input_queue.thread_end(thread)
        thread.join
        log "-- new worker[#{j}] exit"
      end
    end

    def finish
      log "-- Operator#finish called ---"
      @scheduler.on_finish
      @input_queue.finish
      @workers.each{|t| t.join }
    end
  end


  class Manager
    def operator
      @operator ||= Operator.new
    end
  end

end # module Pwrake




