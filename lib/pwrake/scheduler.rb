module Rake
  class Task
    attr_accessor :output_queue

    def assigned
      @assigned ||= []
    end

    def locality
      @locality ||= []
    end

    def locality=(a)
      @locality = a
    end
  end
end


module Pwrake

  class TaskQueue

    class SimpleQueue
      def initialize(hosts=nil)
        @q = []
      end
      def push(x)
        @q.push(x)
      end
      def pop(h)
        @q.shift
      end
      def pop_alt(h)
        nil
      end
      def clear
        @q.clear
      end
      def empty?
        @q.empty?
      end
    end

    def initialize(hosts=[])
      @finished = false
      @m = Mutex.new
      @q = SimpleQueue.new if !defined? @q
      @cv = ConditionVariable.new
      @th_end = []
      @enable_steal =
        !(Rake.application.options.disable_steal || ENV['DISABLE_STEAL'])
    end

    def push(tasks)
      @m.synchronize do
        tasks.each do |task|
          @q.push(task)
        end
        @cv.signal
      end
    end

    def pop(host=nil)
      @m.synchronize do
        n = 0
        loop do
          if @th_end.first == Thread.current
            @th_end.shift
            return false
          elsif @finished # no task in queue
            @cv.signal
            return false
          end

          if !@q.empty?

            if task = @q.pop(host)
              @cv.signal
              return task
            end

            if @enable_steal && n > 1
              if task = @q.pop_alt(host)
                @cv.signal
                return task
              end
            end

            n += 1
            @cv.signal
          end

          @cv.wait(@m)
        end
      end
    end

    def finish
      @finished = true
      @cv.signal
    end

    def stop
      @m.synchronize do
        @q.clear
        finish
      end
    end

    def thread_end(th)
      @th_end.push(th)
      @cv.broadcast
    end
  end


  class Scheduler
    def initialize
    end

    def on_start
    end

    def on_trace(a)
      a
    end

    def on_execute(a)
      if a.kind_of? TaskQueue
        a.task
      else
        a
      end
    end

    def on_finish
    end

    def on_task_start(t)
    end

    def on_task_end(t)
    end

    def queue_class
      TaskQueue
    end
  end

end
