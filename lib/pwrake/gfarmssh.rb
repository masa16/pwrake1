require "pathname"

module Pwrake

  class GfarmSSH < SSH
    @@local_mp = nil

    def initialize(host,remote_mp=nil)
      @remote_mp = Pathname.new(remote_mp)
      super(host)
      if @remote_mp
        system "mkdir -p #{@remote_mp}"
        system "gfarm2fs #{@remote_mp}"
        path = ENV['PATH'].gsub( /#{self.class.mountpoint}/, @remote_mp.to_s )
        system "export PATH=#{path}"
      end
      self
    end

    def close
      if @remote_mp
        system "cd"
        system "fusermount -u #{@remote_mp}"
        system "rmdir #{@remote_mp}"
      end
      super
      self
    end

    def cd_cwd
      dir = @remote_mp + Pathname.pwd.relative_path_from(GfarmSSH.mountpath)
      system "cd #{dir}"
    end

    def cd(dir)
      path = Pathname.new(dir)
      if path.absolute?
        path = @remote_mp + path.relative_path_from(Pathname.new("/"))
      end
      system "cd #{path}"
    end

    def self.mountpoint=(d)
      @@local_mp = Pathname.new(d)
    end

    def self.gf_pwd
      "/" + Pathname.pwd.relative_path_from(GfarmSSH.mountpath).to_s
    end

    def self.gf_path(path)
      pn = Pathname(path)
      if pn.absolute?
        pn = pn.relative_path_from(GfarmSSH.mountpath)
      else
        pn = Pathname.pwd.relative_path_from(GfarmSSH.mountpath) + pn
      end
      "/" + pn.to_s
    end

    def self.local_to_gfarm_path(path)
      pn = Pathname(path)
      if pn.absolute?
        pn = pn.relative_path_from(GfarmSSH.mountpath)
      else
        pn = Pathname.pwd.relative_path_from(GfarmSSH.mountpath) + pn
      end
      "/" + pn.to_s
    end


    def self.mountpoint
      mountpath.to_s
    end

    def self.mountpath
      path = @@local_mp || ENV["GFARM_MOUNTPOINT"] || ENV["GFARM_MP"]
      if !path
        path = Pathname.new(Dir.pwd)
        while ! path.mountpoint?
          path = path.parent
        end
      end
      path
    end


    def self.gfwhere(list)
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
          path = GfarmSSH.gf_path(a)
          if cmd.size + path.size + 1 > 20480 # 131000
            x = Kernel.backquote(cmd)
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
        x = Kernel.backquote(cmd)
        parse_proc.call(x)
      end
      result
    end


    def self.connect_list( hosts )
      # GfarmSSH.set_mountpoint
      tm = Pwrake.timer("connect_gfarmssh")
      th = []
      connections = []
      hosts.each_with_index{ |h,i|
        mnt_dir = "%s%03d" % [ GfarmSSH.mountpoint, i ]
        th << Thread.new(h,mnt_dir) {|x,y|
          if Rake.application.options.single_mp
            Log.log "# create SSH to #{x}"
            ssh = GfarmSSH.new(x)
          else
            Log.log "# create SSH to #{x}:#{y}"
            ssh = GfarmSSH.new(x,y)
          end
          ssh.cd_cwd
          connections << ssh
        }
      }
      th.each{|t| t.join}
      tm.finish
      connections
    end
  end

end
