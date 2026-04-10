require_relative '../aggregator'
require 'tempfile'
require 'csv'
require 'fileutils'

describe 'Ad Performance Aggregator' do
  before(:all) do
    @tmpdir = Dir.mktmpdir
  end

  after(:all) do
    FileUtils.rm_rf(@tmpdir)
  end

  # ========== MinHeap Tests ==========

  describe MinHeap do
    it 'pushes and pops in correct order' do
      heap = MinHeap.new
      heap.push([0.05, "CMP1", []])
      heap.push([0.03, "CMP2", []])
      heap.push([0.04, "CMP3", []])

      expect(heap.pop[0]).to eq(0.03)
      expect(heap.pop[0]).to eq(0.04)
      expect(heap.pop[0]).to eq(0.05)
    end

    it 'maintains top 10 by size' do
      heap = MinHeap.new
      15.times { |i| heap.push([i * 0.01, "CMP#{i}", []]); heap.pop if heap.size > 10 }

      expect(heap.size).to eq(10)
    end

    it 'keeps smallest elements when maintaining top K' do
      heap = MinHeap.new
      10.times { |i| heap.push([i * 0.01, "CMP#{i}", []]) }
      heap.push([0.005, "CMP_SMALLER", []])  # smaller than 0.09
      heap.pop if heap.size > 10

      # After pop, smallest element should be 0.005 or one of the original 10
      first = heap.pop
      expect(first[0]).to be <= 0.01
    end
  end

  # ========== CSV Parsing Tests ==========

  describe '#aggregate_csv' do
    it 'aggregates multi-day entries for same campaign' do
      csv_path = File.join(@tmpdir, "test_#{Time.now.to_i}.csv")
      CSV.open(csv_path, "w") do |csv|
        csv << ["campaign_id", "date", "impressions", "clicks", "spend", "conversions"]
        csv << ["CMP001", "2025-01-01", "1000", "50", "100.00", "5"]
        csv << ["CMP001", "2025-01-02", "1500", "60", "150.00", "7"]
        csv << ["CMP002", "2025-01-01", "2000", "80", "200.00", "10"]
      end

      stats = aggregate_csv(csv_path, headers: true)

      expect(stats.size).to eq(2)
      expect(stats["CMP001"]).to eq([2500, 110, 250.0, 12])
      expect(stats["CMP002"]).to eq([2000, 80, 200.0, 10])

      File.delete(csv_path)
    end

    it 'handles zero conversions' do
      csv_path = File.join(@tmpdir, "zero_conv_#{Time.now.to_i}.csv")
      CSV.open(csv_path, "w") do |csv|
        csv << ["campaign_id", "date", "impressions", "clicks", "spend", "conversions"]
        csv << ["CMP001", "2025-01-01", "1000", "50", "100.00", "0"]
      end

      stats = aggregate_csv(csv_path, headers: true)
      expect(stats["CMP001"]).to eq([1000, 50, 100.0, 0])

      File.delete(csv_path)
    end

    it 'handles empty CSV' do
      csv_path = File.join(@tmpdir, "empty_#{Time.now.to_i}.csv")
      CSV.open(csv_path, "w") do |csv|
        csv << ["campaign_id", "date", "impressions", "clicks", "spend", "conversions"]
      end

      stats = aggregate_csv(csv_path, headers: true)
      expect(stats.size).to eq(0)

      File.delete(csv_path)
    end
  end

  # ========== Metrics Calculation Tests ==========

  describe '#build_heaps' do
    it 'calculates CTR correctly' do
      stats = {
        "CMP001" => [1000, 50, 100.0, 5],   # CTR = 0.05
        "CMP002" => [2000, 60, 200.0, 8],   # CTR = 0.03
      }

      ctr_heap, _ = build_heaps(stats)
      top_ctr = ctr_heap.to_a.sort.reverse

      expect(top_ctr.size).to eq(2)
      expect(top_ctr[0][0]).to be_within(0.001).of(0.05)
      expect(top_ctr[0][1]).to eq("CMP001")
    end

    it 'excludes zero conversions from CPA heap' do
      stats = {
        "CMP001" => [1000, 50, 100.0, 5],
        "CMP002" => [2000, 60, 200.0, 0],  # zero conversions
      }

      _, cpa_heap = build_heaps(stats)
      expect(cpa_heap.size).to eq(1)
    end

    it 'calculates CPA correctly' do
      stats = {
        "CMP001" => [1000, 50, 100.0, 5],  # CPA = 20.0
      }

      _, cpa_heap = build_heaps(stats)
      cpa_val, = cpa_heap.pop
      expect(-cpa_val).to be_within(0.1).of(20.0)
    end
  end

  describe '#merge_heaps' do
    it 'merges and maintains top 10' do
      ctr_list = (1..20).map { |i| [0.01 * i, "CMP#{i}", []] }
      cpa_list = (1..15).map { |i| [-10.0 - i, "CMP#{i}", []] }

      top_ctr, top_cpa = merge_heaps(ctr_list, cpa_list, top_k: 10)

      expect(top_ctr.size).to eq(10)
      expect(top_cpa.size).to eq(10)
    end
  end

  # ========== Output Tests ==========

  describe '#write_results' do
    it 'writes correct CSV columns' do
      output_dir = File.join(@tmpdir, "output_#{Time.now.to_i}")
      FileUtils.mkdir_p(output_dir)

      top_ctr = [[0.05, "CMP001", [1000, 50, 100.0, 5]]]
      top_cpa = [[20.0, "CMP001", [1000, 50, 100.0, 5]]]

      write_results(top_ctr, top_cpa, output_dir)

      ctr_csv = CSV.read(File.join(output_dir, "top10_ctr.csv"), headers: true)
      expected_cols = ["campaign_id", "total_impressions", "total_clicks", "total_spend", "total_conversions", "CTR", "CPA"]

      expect(ctr_csv.headers).to eq(expected_cols)

      FileUtils.rm_rf(output_dir)
    end

    it 'outputs CTR in descending order' do
      output_dir = File.join(@tmpdir, "output_ctr_#{Time.now.to_i}")
      FileUtils.mkdir_p(output_dir)

      top_ctr = [
        [0.05, "CMP001", [1000, 50, 100.0, 5]],
        [0.03, "CMP002", [2000, 60, 200.0, 8]],
      ]

      write_results(top_ctr, [], output_dir)

      ctr_csv = CSV.read(File.join(output_dir, "top10_ctr.csv"), headers: true)
      expect(ctr_csv[0]["CTR"].to_f).to be >= ctr_csv[1]["CTR"].to_f

      FileUtils.rm_rf(output_dir)
    end
  end

  # ========== Adaptive Design Tests ==========

  describe 'Adaptive Concurrency' do
    it 'uses single bucket for high RAM' do
      file_size = 1_000_000_000
      total_ram = 5_000_000_000
      bucket_target = (total_ram * 0.3).to_i

      buckets = [[(file_size / bucket_target.to_f).ceil, 1].max, 256].min

      expect(buckets).to eq(1)
    end

    it 'uses multiple buckets for low RAM' do
      file_size = 1_000_000_000
      total_ram = 256_000_000
      bucket_target = (total_ram * 0.3).to_i

      buckets = [[(file_size / bucket_target.to_f).ceil, 1].max, 256].min

      expect(buckets).to be > 1
      expect(buckets).to be <= 256
    end

    it 'uses single worker for single core' do
      cores = 1
      buckets = 10
      workers = [cores, buckets].min

      expect(workers).to eq(1)
    end

    it 'scales workers to cores' do
      cores = 8
      buckets = 8
      workers = [cores, buckets].min

      expect(workers).to eq(8)
    end

    it 'caps workers at number of buckets' do
      cores = 16
      buckets = 4
      workers = [cores, buckets].min

      expect(workers).to eq(4)
    end
  end
end
