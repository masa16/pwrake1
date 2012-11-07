module FileUtils

  alias sh_orig :sh

  def sh(*cmd, &block)
    options = (Hash === cmd.last) ? cmd.pop : {}
    unless block_given?
      show_command = cmd.join(" ")
      show_command = show_command[0,42] + "..."
      block = lambda { |ok, status|
        ok or fail "Command failed with status (#{status.exitstatus}): [#{show_command}]"
      }
    end
    if RakeFileUtils.verbose_flag == :default
      options[:verbose] = true
    else
      options[:verbose] ||= RakeFileUtils.verbose_flag
    end
    options[:noop]    ||= RakeFileUtils.nowrite_flag
    rake_check_options options, :noop, :verbose
    Pwrake::Log.output_message cmd.join(" ") if options[:verbose]
    unless options[:noop]
      res,status = pwrake_system(*cmd)
      block.call(res, status)
    end
  end

  def pwrake_system(*cmd)
    cmd_log = cmd.join(" ").inspect
    tm = Pwrake::Timer.new("sh",cmd_log)

    conn = Pwrake.current_shell
    if conn.kind_of?(Pwrake::Shell)
      res    = conn.system(*cmd)
      status = Rake::PseudoStatus.new(conn.status)
    else
      res    = system(*cmd)
      status = $?
      status = Rake::PseudoStatus.new(1) if !res && status.nil?
    end

    tm.finish("status=%s cmd=%s"%[status.exitstatus,cmd_log])
    [res,status]
  end
  private :pwrake_system


  # Pwrake version of backquote command
  def pwrake_backquote(cmd)
    cmd_log = cmd.inspect
    tm = Pwrake::Timer.new("bq",cmd_log)

    conn = Pwrake.current_shell
    if conn.kind_of?(Pwrake::Shell)
      res    = conn.backquote(*cmd)
      status = conn.status
    else
      res    = `#{cmd}`
      if !res && status.nil?
        status = 1
      else
        status = $?.exitstatus
      end
    end

    tm.finish("status=%s cmd=%s"%[status,cmd_log])
    res
  end

end # module FileUtils
