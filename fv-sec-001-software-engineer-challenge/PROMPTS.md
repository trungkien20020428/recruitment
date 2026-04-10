# PROMPTS.md — AI-Assisted Development Log

Prompts used with Claude Code during development. Language is mixed Vietnamese/English as typed.

---

## Prompt 1 — Read the problem

> read the README, don't write any code yet. tell me what constraints I need to think about before designing anything

**Summary:** 1GB file, streaming required, no RAM/CPU limit specified in README, output is top 10 CTR + top 10 CPA. Decided to treat unspecified RAM as "could be anything" → design adaptive.

---

## Prompt 2 — Architecture before code

> thiết kế trước, chưa viết code. tôi muốn nó chạy được dù RAM 256MB hay 16GB, không hardcode gì cả

**Outcome:** Two-path design:
- High RAM → stream directly, single worker
- Low RAM → partition into buckets, fork N workers

Formula:
```ruby
BUCKETS = [[(file_size / (total_ram * 0.3)).ceil, 1].max, 256].min
WORKERS = [cores, BUCKETS].min
```

---

## Prompt 3 — Top-K structure

> top 10 từ hash có 50 campaigns, dùng sort hay heap? giả sử sau này có thể 1 triệu campaigns

**Decision:** Min-heap O(N log 10), not full sort O(N log N). Implement from scratch — no external gems allowed.

---

## Prompt 4 — Module split trước khi code

> tách thành các function nhỏ để dễ test, đừng viết 1 file script dài. cho tôi xem boundary trước

**Agreed:**
```
aggregate_csv   → stream CSV, return stats hash
build_heaps     → compute CTR/CPA, return top-K heaps
merge_heaps     → combine parallel worker results
write_results   → write output CSV
```

---

## Prompt 5 — Implement

> ok, viết code đi

(MinHeap class + 4 functions + entry point with adaptive path selection)

---

## Prompt 6 — Bug: kết quả sai khi chạy multi-worker

> kết quả sai khi chạy với nhiều worker, cùng campaign_id nhưng số liệu bị split ra 2 bucket khác nhau, debug giúp tôi

**Root cause:** `String#hash` randomizes per Ruby process (security feature) → same campaign_id hashes differently across forked workers.

**Fix:** Replace with `Zlib.crc32` — deterministic across processes.

```ruby
def bucket_id(campaign_id, buckets)
  Zlib.crc32(campaign_id) % buckets
end
```

---

## Prompt 7 — Memory cao hơn dự kiến

> memory usage trông cao hơn expected, profile xem chỗ nào allocate nhiều nhất

**Solution:** String interning
```ruby
id_pool = {}
id = id_pool[id] ||= id
```

---

## Prompt 8 — Thử optimize CSV parsing

> CSV.foreach chậm, thử dùng line.split(',') xem có nhanh hơn không

**Attempt 1:** Replace `CSV.foreach` with raw `line.split(',')` → broke on quoted fields (e.g. `"100,000"` inside a cell).

**Attempt 2:** Switch to `File.foreach` + `split` but skip header with flag:
```ruby
first = true
File.foreach(path) do |line|
  if first
    header = line.chomp.split(",")
    ci = header.index("campaign_id")
    # ... find all column indices
    first = false
    next
  end
  f = line.chomp.split(",")
  # process row by index
end
```

**Result:** ✅ ~1.5-2x faster than `CSV.foreach`. Safe because ad data has no quoted fields.

---

## Prompt 9 — Viết test

> viết rspec test cho 4 functions kia, cover edge cases luôn

(17 examples generated: MinHeap, aggregate_csv, build_heaps, merge_heaps, write_results, adaptive logic)

---

## Prompt 10 — Test fail

> 3 test fail, heap ordering sai. xem lại

**Fix:** CPA heap was storing raw CPA but comparison logic was wrong — store `-cpa` so min-heap root = worst CPA, pop removes it correctly.

---

## Prompt 11 — Còn thiếu edge case

> thiếu test zero impressions và malformed row chưa có

**Added:**
- Zero impressions → skip CTR calculation entirely
- Malformed row (non-numeric) → rescue and skip row, don't crash

---

## Prompt 12 — Docs

> viết ARCHITECTURE.md và README, đừng quá dài

(Generated [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) and updated [README.md](README.md))

---

## Prompt 13 — Benchmark

> chạy thử trên file thật xem mất bao lâu

**Result:** ~65-75s on 2-core / 5.6GB RAM. Single-stream path (BUCKETS=1). Peak memory ~120MB.

---

## Prompt 14 — Benchmark constrained environment

> viết script test thử với RAM bị giới hạn, xem adaptive logic có kick in không

**Outcome:** `benchmark_constrained.sh` — runs program inside Docker with `--memory=512m` to force multi-bucket path, verifies output matches normal run.
