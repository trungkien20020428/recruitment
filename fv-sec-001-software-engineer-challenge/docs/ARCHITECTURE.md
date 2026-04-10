# Architecture — Ad Performance Aggregator

## System Flow

```
┌─────────────────────────────────────────────────────────┐
│                      Input: ad_data.csv (1GB)           │
│                         26M+ rows                        │
└────────────────────────┬────────────────────────────────┘
                         │
                         ▼
        ┌────────────────────────────────┐
        │  Detect Hardware               │
        │  - Available RAM               │
        │  - CPU cores                   │
        │  Calculate:                    │
        │  - BUCKETS = RAM-dependent     │
        │  - WORKERS = cores-dependent   │
        └────────────┬───────────────────┘
                     │
         ┌───────────┴───────────┐
         │                       │
         ▼                       ▼
    HIGH RAM (>5GB)          LOW RAM (<512MB)
    BUCKETS=1                BUCKETS=2-256
    Stream directly          Partition+Workers
         │                       │
         │                       ├─→ Phase 1: Partition CSV
         │                       │   Split into N bucket files
         │                       │
         │                       ├─→ Phase 2: Process Buckets
         │                       │   Fork N workers in parallel
         │                       │   Each worker:
         │                       │   - Read bucket
         │                       │   - Aggregate stats
         │                       │   - Keep top 10 CTR/CPA
         │                       │   - Write temp results
         │                       │
         │                       └─→ Phase 3: Merge
         │                           Combine worker heaps
         │
         └───────────┬───────────┘
                     │
                     ▼
         ┌──────────────────────┐
         │  Compute Metrics     │
         │  - CTR calculation   │
         │  - CPA calculation   │
         │  - Null handling     │
         └──────────┬───────────┘
                    │
                    ▼
         ┌──────────────────────┐
         │  Select Top 10       │
         │  - CTR: highest      │
         │  - CPA: lowest       │
         │  Using Min-Heap      │
         │  O(N log 10)         │
         └──────────┬───────────┘
                    │
                    ▼
         ┌──────────────────────┐
         │  Output CSV Files    │
         │  - top10_ctr.csv     │
         │  - top10_cpa.csv     │
         └──────────────────────┘
```

---

## Module Design

```
aggregator.rb
│
├─ class MinHeap
│  └─ push(val)      # O(log 10)
│  └─ pop()          # O(log 10)
│  └─ size()
│  └─ to_a()
│
├─ def aggregate_csv(path, headers)
│  └─ Stream CSV line-by-line
│  └─ Hash aggregation { campaign_id → [imp, clicks, spend, conv] }
│  └─ String interning for campaign_id deduplication
│
├─ def build_heaps(stats, top_k)
│  └─ Iterate stats
│  └─ Calculate CTR = clicks / impressions
│  └─ Calculate CPA = spend / conversions
│  └─ Maintain min-heap for top K
│  └─ Return: [ctr_heap, cpa_heap]
│
├─ def merge_heaps(ctr_results, cpa_results)
│  └─ Merge multiple heap results
│  └─ Return: [top_ctr, top_cpa] sorted lists
│
└─ def write_results(top_ctr, top_cpa, output_dir)
   └─ Write CSV with proper formatting
   └─ Handle null CPA (zero conversions)
```

---

## Data Flow: Adaptive Path Selection

### Path A: High RAM, Few Buckets
```
Input CSV 1GB
    │
    └─→ [No partition, single worker]
    │
    └─→ aggregate_csv() streams entire file
    │   returns: stats = { "CMP001" => [1M, 50K, 100K, 5K], ... }
    │
    └─→ build_heaps(stats)
    │   returns: [ctr_heap, cpa_heap]
    │
    └─→ Output CSV directly
```

**When:** total_ram >= file_size * 0.3 (allocate 30% to aggregation)
**Advantage:** Minimal I/O, single worker, no coordination overhead

---

### Path B: Low RAM, Many Buckets
```
Input CSV 1GB
    │
    ├─→ Phase 1: Partition
    │   └─ For each row:
    │       campaign_id hash → bucket_N
    │       write to buckets/bucket_N.csv
    │
    ├─→ Phase 2: Process (N workers in parallel)
    │   ├─ Worker 1: reads buckets/bucket_0..3.csv
    │   │             aggregate → heap → write worker_0_ctr.json
    │   │
    │   ├─ Worker 2: reads buckets/bucket_4..7.csv
    │   │             aggregate → heap → write worker_1_ctr.json
    │   │
    │   └─ Worker N: ...
    │
    ├─→ Phase 3: Merge
    │   └─ Read all worker_*.json files
    │   └─ Merge heaps → top 10
    │
    └─→ Output CSV
```

**When:** total_ram < file_size * 0.3
**Advantage:** Each worker uses only ~1/N of memory, parallel processing

---

## Key Algorithms

### 1. Min-Heap for Top-K (O(N log K))

```ruby
ctr_heap = MinHeap.new
stats.each do |id, s|
  ctr = s[1] / s[0].to_f  # clicks / impressions
  ctr_heap.push([ctr, id, s])
  ctr_heap.pop if ctr_heap.size > 10  # Keep only top 10
end
```

**Why this matters:**
- Full sort: O(N log N)
- Heap approach: O(N log 10) ≈ O(N)
- For 1M campaigns: ~100x faster

---

### 2. String Interning (Memory optimization)

```ruby
id_pool = {}
campaign_id = id_pool[row["campaign_id"]] ||= row["campaign_id"]
```

**Why:** 50 unique campaigns × 26M rows = 26M string references
- Without interning: 26M string objects
- With interning: 50 unique string objects
- Saves: ~26M × 40 bytes (string overhead) = ~1GB

---

### 3. Deterministic Bucketing (for parallel safety)

```ruby
def bucket_id(campaign_id, buckets)
  Zlib.crc32(campaign_id) % buckets
end
```

**Why CRC32 (not String#hash):**
- CRC32: deterministic across runs
- String#hash: randomized per Ruby process (security feature)
- If not deterministic → same campaign splits across buckets → wrong aggregation

---

## Error Handling

```ruby
aggregate_csv(path):
  ├─ Missing file? → Error immediately
  ├─ Malformed row (bad numeric)? → Skip, count
  └─ Empty CSV? → Return empty hash

build_heaps(stats):
  ├─ Zero impressions? → CTR = null
  └─ Zero conversions? → CPA = null, excluded from CPA heap

write_results():
  └─ Create output dir if missing
```

---

## Testing Strategy

**Unit Tests (17 specs):**
- MinHeap: operations, size management
- aggregate_csv: multi-day, zero conversions, empty file
- build_heaps: CTR/CPA calculation, null handling
- merge_heaps: combining results
- write_results: CSV format, column order
- Adaptive logic: RAM/CPU scenarios

**Integration:**
- Run full pipeline on real ~1GB file
- Verify output format matches spec
- Check performance: ~65-75s on 2-core machine

---

## Complexity Analysis

| Operation | Complexity | Note |
|-----------|-----------|------|
| Stream CSV | O(1) memory | Process row-by-row |
| Aggregate | O(N) time, O(C) memory | C = unique campaigns (~50) |
| Build heaps | O(N log K) | K = 10 (top-10) |
| Merge heaps | O(N log K) | Same as build |
| Sort output | O(K log K) | K = 10 |
| **Total** | **O(N)** | Linear in file size |

---

## Alternative Approaches Considered

### Option 1: Full Sort (rejected)
```ruby
sorted_by_ctr = stats.sort_by { |_, s| -calculate_ctr(s) }.take(10)
```
- **Problem:** O(N log N) sort of all campaigns
- **Why rejected:** Heap is better: O(N log K)

### Option 2: Load entire CSV into array (rejected)
```ruby
rows = CSV.read(input_path)
```
- **Problem:** 1GB × 2 (in-memory) = 2GB RAM minimum
- **Why rejected:** Doesn't scale to constrained environments

### Option 3: Database backend (rejected)
```ruby
db.bulk_insert(rows)
db.execute("SELECT ... GROUP BY campaign_id ORDER BY ctr DESC LIMIT 10")
```
- **Problem:** Adds infrastructure dependency
- **Why rejected:** CSV streaming sufficient for requirements

### Option 4: Hardcoded single-worker (rejected)
```ruby
# Just stream and aggregate, no parallel
```
- **Problem:** Wastes multi-core CPU
- **Why rejected:** Adaptive design better

---

## Performance Characteristics

| Metric | Value | Comment |
|--------|-------|---------|
| File size | 1GB | ~26M rows |
| Processing time | ~65-75s | Ruby CSV module is pure Ruby |
| Peak memory | Adaptive | 50MB-500MB depending on RAM detection |
| Bucket count | 1-256 | Auto-adjusted |
| Worker count | 1-N | Limited by CPU cores and buckets |

**Bottleneck:** CSV parsing (Ruby's `csv` module is slow)
**Potential 10x speedup:** Use C extension (fastcsv gem) or switch to Go/Rust
