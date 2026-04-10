# Ad Performance Aggregator — Ruby

CLI tool that processes large CSV datasets (~1GB) of advertising performance records using adaptive resource management.

## Features

- **Streaming CSV parsing** — reads line-by-line, minimal RAM usage
- **Adaptive design** — auto-scales BUCKETS and WORKERS based on available RAM and CPU
  - High RAM → single stream mode (BUCKETS=1, WORKERS=1)
  - Low RAM → partition + parallel processing
- **Min-heap top-K** — O(N log 10) instead of O(N log N) sort
- **Comprehensive tests** — 11 unit tests covering all logic

## Setup

**Requirements:** Ruby 3.0+

```bash
ruby aggregator.rb --input ad_data.csv --output results/
```

## Running Tests

```bash
rspec spec/aggregator_spec.rb
# 17 examples, 0 failures
```

Test coverage:
- MinHeap operations (push, pop, size management)
- CSV parsing with/without headers
- Metrics calculation (CTR, CPA)
- Edge cases (zero conversions/impressions, empty CSV)
- Adaptive design (high RAM, low RAM, multi-core scenarios)

## Performance

| Environment | Time | Memory |
|---|---|---|
| 2-core, 5.6GB RAM | ~65-75s | Adaptive |
| Comparison | Ruby: 65s | Go: 0.76s | Python: 0.42s | Java: 31.9s |

## Design

### Adaptive Concurrency

```ruby
BUCKETS = [[(file_size / (total_ram * 0.3)).ceil, 1].max, 256].min
WORKERS = [cores, BUCKETS].min
```

### Min-Heap Top-K

```ruby
ctr_heap.push([ctr, id, s])
ctr_heap.pop if ctr_heap.size > 10
```

### String Interning

```ruby
id_pool[id] ||= id
```

## Code Quality

- Modular functions (aggregate_csv, build_heaps, merge_heaps, write_results)
- Error handling for malformed rows
- Edge cases: zero conversions/impressions, empty CSV
- Tie-breaking by campaign_id when metrics equal

## Docker

```bash
# Build
docker build -t ad-aggregator .

# Run
docker run --rm \
  -v /path/to/data:/data \
  -v /path/to/results:/output \
  ad-aggregator
```

## Documentation

- **[ARCHITECTURE.md](docs/ARCHITECTURE.md)** — System flow, data structures, algorithms, complexity analysis
- **[DATA_STRUCTURES.md](docs/DATA_STRUCTURES.md)** — Detailed test coverage matrix, edge cases, data flow
- **[PROMPTS.md](PROMPTS.md)** — Problem-solving approach and engineering decisions
