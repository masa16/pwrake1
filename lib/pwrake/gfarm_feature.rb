module Pwrake

  module GfarmPath

    module_function

    def mountpoint_of_cwd
      path = Pathname.pwd
      while !path.mountpoint?
        path = path.parent
      end
      path
    end

    @@local_mountpoint = mountpoint_of_cwd
    @@fs_subdir = Pathname.new('/')

    def mountpoint=(d)
      @@local_mountpoint = Pathname.new(d)
    end

    def mountpoint
      @@local_mountpoint
    end

    def subdir=(d)
      if d
        @@fs_subdir = Pathname.new(d)
        if @@fs_subdir.relative?
          @@fs_subdir = Pathname.new('/') + @@fs_subdir
        end
      end
    end

    def subdir
      @@fs_subdir.to_s
    end

    def pwd
      Pathname.pwd.relative_path_from(@@local_mountpoint)
    end

    def gfarm2fs?(d=nil)
      d ||= @@local_mountpoint
      mount_type = nil
      open('/etc/mtab','r') do |f|
        f.each_line do |l|
          if /#{d} (?:type )?(\S+)/o =~ l
            mount_type = $1
            break
          end
        end
      end
      /gfarm2fs/ =~ mount_type
    end

    def from_local(x)
      pn = Pathname(x)
      if pn.absolute?
        pn.relative_path_from(@@local_mountpoint)
      else
        Pathname.pwd.relative_path_from(@@local_mountpoint) + pn
      end
    end

    def from_fs(x)
      Pathname(x).relative_path_from(@@fs_subdir)
    end

    def to_fs(x)
      @@fs_subdir + Pathname(x)
    end

    def to_local(x)
      @@local_mountpoint + Pathname(x)
    end

    def local_to_fs(x)
      x = from_local(x)
      x = to_fs(x)
      x.to_s
    end

    def fs_to_local(x)
      x = from_fs(x)
      x = to_local(x)
      x.to_s
    end

    def gfwhere(list)
      system "sync"
      result = {}
      count = 0
      cmd = "gfwhere"
      parse_proc = proc{|x|
        if count==1
          result[cmd[8..-1]] = x.split
        else
          x.scan(/^([^\n]+):\n([^\n]*)$/m) do |file,hosts|
            h = hosts.split
            result[file] = h if !h.empty?
          end
        end
      }

      list.each do |a|
        if a
          path = local_to_fs(a)
          if cmd.size + path.size + 1 > 20480 # 131000
            x = `#{cmd}`
            parse_proc.call(x)
            cmd = "gfwhere"
            count = 0
          end
          cmd << " "
          cmd << path
          count += 1
        end
      end
      if count > 0
        x = `#{cmd}`
        parse_proc.call(x)
      end
      result
    end

  end


  class GfarmShell < Shell

    @@core_id = {}
    @@prefix = "pwrake_#{ENV['USER']}"

    def initialize(host,opt={})
      super(host,opt)
      @single_mp = @option[:single_mp]
      @basedir   = @option[:basedir]
      @prefix    = @option[:prefix] || @@prefix
      @work_dir  = @option[:work_dir]

      @core_id = @@core_id[host] || 0
      @@core_id[host] = @core_id + 1

      if @single_mp
        @remote_mountpoint = "#{@basedir}/#{@prefix}_00"
      else
        @remote_mountpoint = "#{@basedir}/#{@prefix}_%02d" % @core_id
      end
    end

    def start
      Log.debug "--- mountpoint=#{@remote_mountpoint}"
      open(system_cmd)
      cd
      if not _system "test -d #{@remote_mountpoint}"
        _system "mkdir -p #{@remote_mountpoint}"
        subdir = GfarmPath.subdir
        if ["/","",nil].include?(subdir)
          _system "gfarm2fs #{@remote_mountpoint}"
        else
          _system "gfarm2fs -o modules=subdir,subdir=#{subdir} #{@remote_mountpoint}"
        end
      end
      path = ENV['PATH'].gsub( /#{GfarmPath.mountpoint}/, @remote_mountpoint )
      _system "export PATH=#{path}"
      cd_work_dir
    end

    def close
      if @remote_mountpoint
        cd
        _system "fusermount -u #{@remote_mountpoint}"
        _system "rmdir #{@remote_mountpoint}"
      end
      super
      self
    end

    def cd_work_dir
      # modify local work_dir -> remote work_dir
      dir = Pathname.new(@remote_mountpoint) + GfarmPath.pwd
      cd dir
    end

  end


  class GfarmQueue < LocalityAwareQueue

    def after_check(tasks)
      list = []
      tasks.each do |t|
	list << t.name if t.kind_of? Rake::FileTask
      end
      if !list.empty?
	Log.info "-- after_check: size=#{list.size} #{list.inspect}"
	gfwhere_result = GfarmPath.gfwhere(list)
	tasks.each do |t|
	  if t.kind_of? Rake::FileTask
	    t.location = gfwhere_result[GfarmPath.local_to_fs(t.name)]
	  end
	end
	#puts "'#{self.name}' exist? => #{File.exist?(self.name)} loc => #{loc}"
      end
    end

=begin
    def abr_msg(a)
      m = a[0..5].map{|x| x}.inspect
      m.sub!(/]$/,",...") if a.size > 6
      "size=#{a.size} #{m}"
    end

    def where(tasks)
      if Pwrake.application.options.dryrun ||
          Pwrake.application.options.disable_affinity
        return tasks
      end

      start_time = Time.now
      #Log.debug "--- GfarmQueue#where #{tasks.inspect}"
      #if Pwrake.manager.gfarm and Pwrake.manager.affinity
      gfwhere_result = {}
      filenames = []
      tasks.each do |t|
        if t.kind_of?(Rake::FileTask) and
            name = t.prerequisites[0] and
            !filenames.include?(name)
          filenames << name
        end
      end

      if !filenames.empty?
        gfwhere_result = GfarmPath.gfwhere(filenames)
        tasks.each do |t|
          if t.kind_of? Rake::FileTask and prereq_name = t.prerequisites[0]
            t.location = gfwhere_result[GfarmPath.local_to_fs(prereq_name)]
          end
        end
      end
      Log.info "-- GfarmQueue#where %.6fs %s" % [Time.now-start_time,abr_msg(filenames)]
      tasks
    end
=end

  end

end
