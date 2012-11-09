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
