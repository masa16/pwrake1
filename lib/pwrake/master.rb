module Pwrake

  def current_shell
    Thread.current[:shell]
  end

  def current_shell=(a)
    Thread.current[:shell] = a
    Thread.current[:hint] = a.host
  end

  module_function :current_shell, :current_shell=


  class Master
    include Pwrake::Option

    attr_reader :task_queue
    attr_reader :finish_queue
    attr_reader :shell_set
    attr_reader :filesystem
    attr_reader :postprocess

    def initialize
      init_option    # Pwrake::Option
      setup_option   # Pwrake::Option
      @started = false
      @lock = Mutex.new
      @current_task_id = -1
    end

    def start
      return if @task_queue
      timer = Timer.new("start_worker")
      @finish_queue = Queue.new
      @task_queue = @queue_class.new(@core_list)
      @shell_set = []
      @core_list.each_with_index do |h,i|
        @shell_set << @shell_class.new(h,@shell_opt)
      end
      start_threads
      timer.finish
    end

    def finish
      Log.debug "-- Master#finish called"
      @task_queue.finish if @task_queue
      @threads.each{|t| t.join } if @threads
      @counter.print if @counter
      finish_option   # Pwrake::Option
    end

    def start_threads
      Thread.abort_on_exception = true
      @threads = []
      t_intvl = Pwrake.application.pwrake_options['THREAD_CREATE_INTERVAL']
      @shell_set.each do |c|
        tc0 = Time.now
        @threads << Thread.new(c) do |conn|
          Pwrake.current_shell = conn
          t0 = Time.now
          conn.start
          t = Time.now - t0
          Log.info "-- worker[#{conn.id}] connect to #{conn.host}: %.3f sec" % t
          begin
            thread_loop(conn)
          ensure
            Log.info "-- worker[#{conn.id}] ensure : closing #{conn.host}"
            conn.finish
          end
        end
        t_sleep = t_intvl - (Time.now - tc0)
        sleep t_sleep if t_sleep > 0
      end
    end

    def thread_loop(conn,last=nil)
      @task_queue.reserve(last) if last
      hint = (conn) ? conn.host : nil
      standard_exception_handling do
        while true
	  time_start = Time.now
	  t = @task_queue.deq(hint)
	  break if !t
	  time_deq = Time.now - time_start
          Log.debug "--- Master#thread_loop deq t=#{t.inspect} time=#{time_deq}sec"
          t.pw_invoke
          return if t == last
        end
      end
    end

    def task_id_counter
      @lock.synchronize do
        @current_task_id += 1
      end
    end

    # Provide standard execption handling for the given block.
    def standard_exception_handling
      begin
        yield
      rescue SystemExit => ex
        # Exit silently with current status
        @task_queue.stop
        raise
      rescue OptionParser::InvalidOption => ex
        # Exit silently
        @task_queue.stop
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
        @task_queue.stop
        exit(false)
      end
    end

  end

end # module Pwrake
