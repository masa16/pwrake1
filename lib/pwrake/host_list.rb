module Pwrake

  class HostInfo
    def initalize(name,group=0,weight=1)
      @name = name
      @group = group
      @weight = weight
    end
    attr_reader :name, :group, :weight
  end

  class HostList
    attr_reader :group_hosts
    attr_reader :group_core_weight
    attr_reader :group_weight_sum
    attr_reader :host2group
    attr_reader :num_threads
    attr_reader :core_list
    attr_reader :host_count

    def initialize(file=nil)
      @file = file
      @group_hosts = []
      @group_core_weight = []
      @group_weight_sum = []
      @host2group = {}
      require "socket"
      if @file
        read_host(@file)
        @num_threads = @core_list.size
      else
        @num_threads = 1 if !@num_threads
        @core_list = ['localhost'] * @num_threads
      end
    end

    def size
      @num_threads
    end

    def read_host(file)
      tmplist = []
      File.open(file) do |f|
        re = /\[\[([\w\d]+)-([\w\d]+)\]\]/o
        while l = f.gets
          l = $1 if /^([^#]*)#/ =~ l
          host, ncore, weight, group = l.split
          if host
            if re =~ host
              hosts = ($1..$2).map{|i| host.sub(re,i)}
            else
              hosts = [host]
            end
            hosts.each do |host|
              begin
                host = Socket.gethostbyname(host)[0]
              rescue
                Log.info "-- FQDN not resoved : #{host}"
              end
              ncore = (ncore || 1).to_i
              weight = (weight || 1).to_f
              w = ncore * weight
              group = (group || 0).to_i
              tmplist << ([host] * ncore.to_i)
              (@group_hosts[group] ||= []) << host
              (@group_core_weight[group] ||= []) << w
              @group_weight_sum[group] = (@group_weight_sum[group]||0) + w
              @host2group[host] = group
            end
          end
        end
      end

      @core_list = []
      begin # alternative order
        sz = 0
        tmplist.each do |a|
          @core_list << a.shift if !a.empty?
          sz += a.size
        end
      end while sz>0

      @host_count = Hash.new{0}
      core_list.each{|h| @host_count[h] += 1}
    end

  end
end
