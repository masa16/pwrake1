module Pwrake

  def current_shell
    Thread.current[:shell]
  end

  def current_shell=(a)
    Thread.current[:shell] = a
  end

  module_function :current_shell, :current_shell=


  class Master
    include Pwrake::Option

    attr_reader :task_queue
    attr_reader :shell_set

    def initialize
    end

    def init
      init_option   # Pwrake::Option
    end

    def setup
      setup_option   # Pwrake::Option
    end

    def start
      @counter = Counter.new
      @task_queue = @queue_class.new(@core_list)
      @task_queue.enable_steal = !Rake.application.options.disable_steal
      @shell_set = []
      @core_list.each_with_index do |h,i|
        @shell_set << @shell_class.new(h,@shell_opt)
      end
      start_threads
    end

    def finish
      Log.debug "-- Master#finish called"
      @task_queue.finish if @task_queue
      @threads.each{|t| t.join }
      @counter.print
      finish_option   # Pwrake::Option
    end

    def start_threads
      Thread.abort_on_exception = true
      @threads = []
      @shell_set.each do |c|
        @threads << Thread.new(c) do |conn|
          Pwrake.current_shell = conn
          conn.start
          begin
            thread_loop(conn)
          ensure
            Log.info "-- worker[#{conn.id}] ensure : closing #{conn.host}"
            conn.finish
          end
        end
      end
    end

    def thread_loop(conn,last=nil)
      @task_queue.reserve(last) if last
      hint = (conn) ? conn.host : nil
      standard_exception_handling do
        while t = @task_queue.deq(hint)
          Log.debug "-- Master#thread_loop deq t=#{t.inspect}"
          t.pw_invoke
          return if t == last
        end
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
