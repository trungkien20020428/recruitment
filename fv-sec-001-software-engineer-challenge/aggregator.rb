require 'optparse'
require 'etc'
require 'fileutils'
require 'zlib'
require 'csv'
require 'json'

# ---------- MinHeap ----------
class MinHeap
  def initialize
    @data = []
  end

  def push(val)
    @data << val
    up(@data.size - 1)
  end

  def pop
    return if @data.empty?
    swap(0, @data.size - 1)
    min = @data.pop
    down(0)
    min
  end

  def size
    @data.size
  end

  def to_a
    @data
  end

  private

  def up(i)
    while i > 0
      p = (i - 1) / 2
      break if @data[p][0] <= @data[i][0]
      swap(p, i)
      i = p
    end
  end

  def down(i)
    n = @data.size
    loop do
      l = 2*i + 1
      r = 2*i + 2
      s = i
      s = l if l < n && @data[l][0] < @data[s][0]
      s = r if r < n && @data[r][0] < @data[s][0]
      break if s == i
      swap(i, s)
      i = s
    end
  end

  def swap(i, j)
    @data[i], @data[j] = @data[j], @data[i]
  end
end

# ---------- Core logic ----------

def bucket_id(campaign_id, buckets)
  Zlib.crc32(campaign_id) % buckets
end

def aggregate_csv(path, headers: true)
  stats = Hash.new { |h, k| h[k] = [0, 0, 0.0, 0] }
  id_pool = {}

  if headers
    ci = ii = cli = si = coni = min_cols = nil
    first = true

    File.foreach(path) do |line|
      if first
        header   = line.chomp.split(",")
        ci       = header.index("campaign_id")
        ii       = header.index("impressions")
        cli      = header.index("clicks")
        si       = header.index("spend")
        coni     = header.index("conversions")
        min_cols = [ci, ii, cli, si, coni].max + 1
        first    = false
        next
      end

      f = line.chomp.split(",")
      next if f.size < min_cols

      id = id_pool[f[ci]] ||= f[ci]
      s  = stats[id]
      s[0] += f[ii].to_i
      s[1] += f[cli].to_i
      s[2] += f[si].to_f
      s[3] += f[coni].to_i
    end
  else
    File.foreach(path) do |line|
      f = line.chomp.split(",")
      next if f.size < 5

      id = id_pool[f[0]] ||= f[0]
      s  = stats[id]
      s[0] += f[1].to_i
      s[1] += f[2].to_i
      s[2] += f[3].to_f
      s[3] += f[4].to_i
    end
  end

  stats
end

def build_heaps(stats, top_k: 10)
  ctr_heap = MinHeap.new
  cpa_heap = MinHeap.new

  stats.each do |id, s|
    impressions, clicks, spend, conversions = s

    if impressions > 0
      ctr = clicks.to_f / impressions
      ctr_heap.push([ctr, id, s])
      ctr_heap.pop if ctr_heap.size > top_k
    end

    if conversions > 0
      cpa = spend / conversions
      cpa_heap.push([-cpa, id, s])
      cpa_heap.pop if cpa_heap.size > top_k
    end
  end

  [ctr_heap, cpa_heap]
end

def merge_heaps(ctr_results, cpa_results, top_k: 10)
  ctr_heap = MinHeap.new
  cpa_heap = MinHeap.new

  ctr_results.each do |entry|
    ctr_heap.push(entry)
    ctr_heap.pop if ctr_heap.size > top_k
  end

  cpa_results.each do |entry|
    cpa_heap.push(entry)
    cpa_heap.pop if cpa_heap.size > top_k
  end

  top_ctr = ctr_heap.to_a.sort.reverse
  top_cpa = cpa_heap.to_a.map { |e| [-e[0], e[1], e[2]] }.sort

  [top_ctr, top_cpa]
end

def write_results(top_ctr, top_cpa, output_dir)
  CSV.open("#{output_dir}/top10_ctr.csv", "w") do |csv|
    csv << ["campaign_id", "total_impressions", "total_clicks", "total_spend", "total_conversions", "CTR", "CPA"]
    top_ctr.each do |ctr, id, s|
      impressions, clicks, spend, conversions = s
      cpa = conversions > 0 ? (spend / conversions).round(4) : nil
      csv << [id, impressions, clicks, spend.round(2), conversions, ctr.round(4), cpa]
    end
  end

  CSV.open("#{output_dir}/top10_cpa.csv", "w") do |csv|
    csv << ["campaign_id", "total_impressions", "total_clicks", "total_spend", "total_conversions", "CTR", "CPA"]
    top_cpa.each do |cpa, id, s|
      impressions, clicks, spend, conversions = s
      ctr = impressions > 0 ? (clicks.to_f / impressions).round(4) : nil
      csv << [id, impressions, clicks, spend.round(2), conversions, ctr, cpa.round(4)]
    end
  end
end

# ---------- Entry point ----------
if __FILE__ == $0
  options = { input: "ad_data.csv", output: "results" }

  OptionParser.new do |opts|
    opts.on("--input FILE", "Input CSV file path") { |v| options[:input] = v }
    opts.on("--output DIR",  "Output directory path") { |v| options[:output] = v }
  end.parse!

  cores     = Etc.nprocessors
  total_ram = if ENV["SIMULATE_RAM_MB"]
    ENV["SIMULATE_RAM_MB"].to_i * 1024 * 1024
  else
    File.read("/proc/meminfo").match(/MemAvailable:\s+(\d+)/)[1].to_i * 1024 rescue 256 * 1024 * 1024
  end
  file_size = File.size(options[:input]) rescue 0

  p "Input:      #{options[:input]}"
  p "Output:     #{options[:output]}"
  p "CPU cores:  #{cores}"
  p "RAM avail:  #{total_ram / 1024 / 1024} MB"
  p "File size:  #{file_size / 1024 / 1024} MB"

  bucket_target = (total_ram * 0.3).to_i
  buckets = [[(file_size / bucket_target.to_f).ceil, 1].max, 256].min
  workers = [cores, buckets].min

  p "Buckets:    #{buckets}"
  p "Workers:    #{workers}"

  FileUtils.mkdir_p(options[:output])
  FileUtils.mkdir_p("buckets")

  if buckets == 1 && workers == 1
    p "\nStep 1: Streaming directly (single worker, no partition)..."
    start = Time.now

    stats = aggregate_csv(options[:input], headers: true)
    ctr_heap, cpa_heap = build_heaps(stats)

    p "  Time: #{(Time.now - start).round(2)}s"

    top_ctr = ctr_heap.to_a.sort.reverse
    top_cpa = cpa_heap.to_a.map { |e| [-e[0], e[1], e[2]] }.sort

  else
    p "\nStep 1: Partitioning..."
    start = Time.now
    row_count = 0

    bucket_files = Array.new(buckets) { |i| File.open("buckets/bucket_#{i}.csv", "w") }

    lines = File.foreach(options[:input])
    header = lines.next.chomp.split(",")
    ci   = header.index("campaign_id")
    ii   = header.index("impressions")
    cli  = header.index("clicks")
    si   = header.index("spend")
    coni = header.index("conversions")
    min_cols = [ci, ii, cli, si, coni].max + 1

    lines.each do |line|
      f = line.chomp.split(",")
      next if f.size < min_cols

      b = bucket_id(f[ci], buckets)
      bucket_files[b].puts "#{f[ci]},#{f[ii]},#{f[cli]},#{f[si]},#{f[coni]}"
      row_count += 1
    end
    bucket_files.each(&:close)

    p "  Rows processed: #{row_count}"
    p "  Time: #{(Time.now - start).round(2)}s"

    p "\nStep 2: Processing buckets..."
    start = Time.now

    all_buckets = Dir.glob("buckets/bucket_*.csv").sort
    slices      = all_buckets.each_slice((all_buckets.size.to_f / workers).ceil).to_a

    pids = slices.each_with_index.map do |slice, worker_id|
      fork do
        ctr_heap = MinHeap.new
        cpa_heap = MinHeap.new

        slice.each do |file|
          stats = aggregate_csv(file, headers: false)
          h_ctr, h_cpa = build_heaps(stats)
          h_ctr.to_a.each { |e| ctr_heap.push(e); ctr_heap.pop if ctr_heap.size > 10 }
          h_cpa.to_a.each { |e| cpa_heap.push(e); cpa_heap.pop if cpa_heap.size > 10 }
        end

        File.write("buckets/worker_#{worker_id}_ctr.json", JSON.dump(ctr_heap.to_a))
        File.write("buckets/worker_#{worker_id}_cpa.json", JSON.dump(cpa_heap.to_a))
      end
    end

    pids.each { |pid| Process.wait(pid) }
    p "  Time: #{(Time.now - start).round(2)}s"

    p "\nStep 3: Merging results..."
    start = Time.now

    ctr_results = slices.size.times.flat_map { |id| JSON.parse(File.read("buckets/worker_#{id}_ctr.json")) }
    cpa_results = slices.size.times.flat_map { |id| JSON.parse(File.read("buckets/worker_#{id}_cpa.json")) }

    top_ctr, top_cpa = merge_heaps(ctr_results, cpa_results)

    p "  Time: #{(Time.now - start).round(2)}s"
  end

  p "\nStep 4: Writing results..."
  write_results(top_ctr, top_cpa, options[:output])
  FileUtils.rm_rf("buckets")

  p "  Written: #{options[:output]}/top10_ctr.csv"
  p "  Written: #{options[:output]}/top10_cpa.csv"
  p "\nDone."
end
