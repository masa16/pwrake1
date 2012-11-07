class Counter

  def initialize
    @same = 0
    @diff = 0
    @total = 0
    @same_hosts = {}
    @diff_hosts = {}
    @no_queue = 0
    @found_queue = 0
    @empty_queue = 0
  end

  def count(host_list, host)
    @total += 1
    if host_list and host_list.include?(host)
      @same += 1
      @same_hosts[host] = (@same_hosts[host]||0) + 1
    else
      @diff += 1
      @diff_hosts[host] = (@diff_hosts[host]||0) + 1
    end
  end

  def print
    s  = "same=#{@same}, diff=#{@diff}, total=#{@total}\n"
    s << "same_hosts = {\n"
    @same_hosts.keys.sort.each do |k|
      s << "  #{k}: #{@same_hosts[k]}\n"
    end
    s << "}\n"
    s << "different_hosts = {\n"
    @diff_hosts.keys.sort.each do |k|
      s << "  #{k}: #{@diff_hosts[k]}\n"
    end
    s << "}"
    Pwrake::Log.log s
  end

  def no_queue
    @no_queue += 1
  end

  def found_queue
    @found_queue += 1
  end

  def empty_queue
    @empty_queue += 1
  end
end
