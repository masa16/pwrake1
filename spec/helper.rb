class Helper

  @@spec_dir = File.absolute_path(File.dirname(__FILE__))+"/"
  @@pwrake = @@spec_dir+'../bin/pwrake'

  @@show_command = false
  @@show_result = false

  def initialize(dir=nil,args=nil)
    @dir = @@spec_dir+(dir||"")
    @args = args
  end

  attr_reader :n_files, :filelist, :result, :status
  attr_reader :elapsed_time

  def clean
    Dir.chdir(@dir) do
      `rake -q clean`
    end
    self
  end

  def run
    if @@show_command
      puts "@dir=#{@dir}"
      puts "@args=#{@args}"
    end
    Dir.chdir(@dir) do
      #system "rm -f *.dat"
      #`rake clean`
      tm = Time.now
      @result = `#{@@pwrake} -q #{@args}`
      @status = $?
      @elapsed_time = Time.now - tm
      system "touch dummy; rm dummy"
      @filelist = Dir.glob("*")
      @n_files = @filelist.size
    end
    if @@show_result
      puts @result
      puts "@status=#{@status.inspect}"
    end
    self
  end

  def success?
    @status && @status.success?
  end

  def output_lines
    @result.split("\n")
  end
end

def read_hosts(file)
  cores = []
  open(file) do |f|
    while l = f.gets
      l = $1 if /^([^#]*)#/ =~ l
      host, ncore, group = l.split
      if host
        ncore = (ncore || 1).to_i
        cores.concat( [host] * ncore )
      end
    end
  end
  cores
end
