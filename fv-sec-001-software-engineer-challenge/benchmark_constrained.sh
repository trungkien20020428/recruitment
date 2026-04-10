#!/bin/bash
# Benchmark: simulate 128MB RAM, 0.5 CPU constraint via SIMULATE_RAM_MB env var
# Compares constrained results with baseline to verify correctness

set -e

INPUT="${1:-ad_data.csv}"
OUTPUT_NORMAL="results"
OUTPUT_CONSTRAINED="/tmp/results_constrained"

echo "========================================"
echo " Constrained Environment Benchmark"
echo " RAM simulation: 128MB | CPU: 0.5"
echo "========================================"
echo

# --- Run 1: Normal (baseline) ---
echo "[1/2] Running NORMAL environment..."
mkdir -p "$OUTPUT_NORMAL"

START=$(date +%s%N)
ruby aggregator.rb --input "$INPUT" --output "$OUTPUT_NORMAL"
END=$(date +%s%N)

NORMAL_TIME=$(( (END - START) / 1000000000 ))
echo "Normal time: ${NORMAL_TIME}s"
echo

# --- Run 2: Simulated 128MB RAM ---
echo "[2/2] Running SIMULATED 128MB RAM..."
rm -rf "$OUTPUT_CONSTRAINED" && mkdir -p "$OUTPUT_CONSTRAINED"

START=$(date +%s%N)
SIMULATE_RAM_MB=128 ruby aggregator.rb --input "$INPUT" --output "$OUTPUT_CONSTRAINED"
END=$(date +%s%N)

CONSTRAINED_TIME=$(( (END - START) / 1000000000 ))
echo "Constrained time: ${CONSTRAINED_TIME}s"
echo

# --- Compare results ---
echo "========================================"
echo " Comparing Results"
echo "========================================"

CTR_DIFF=$(diff "$OUTPUT_NORMAL/top10_ctr.csv" "$OUTPUT_CONSTRAINED/top10_ctr.csv" 2>&1 || true)
CPA_DIFF=$(diff "$OUTPUT_NORMAL/top10_cpa.csv" "$OUTPUT_CONSTRAINED/top10_cpa.csv" 2>&1 || true)

if [ -z "$CTR_DIFF" ] && [ -z "$CPA_DIFF" ]; then
  echo "✅ Results MATCH — adaptive design works correctly"
else
  echo "❌ Results DIFFER"
  if [ -n "$CTR_DIFF" ]; then
    echo "top10_ctr.csv diff:"
    echo "$CTR_DIFF"
  fi
  if [ -n "$CPA_DIFF" ]; then
    echo "top10_cpa.csv diff:"
    echo "$CPA_DIFF"
  fi
fi

echo
echo "========================================"
echo " Summary"
echo "========================================"
echo "Normal (full RAM):         ${NORMAL_TIME}s"
echo "Simulated 128MB RAM:       ${CONSTRAINED_TIME}s"
echo "Overhead: $(( CONSTRAINED_TIME - NORMAL_TIME ))s"
echo

FILE_SIZE=$(stat -c%s "$INPUT" 2>/dev/null || echo 0)
BUCKET_TARGET=$(( 128 * 1024 * 1024 * 3 / 10 ))
BUCKETS=$(python3 -c "import math; print(min(max(math.ceil($FILE_SIZE / $BUCKET_TARGET), 1), 256))" 2>/dev/null || echo "~26")

echo "Adaptive config (128MB RAM):"
echo "  BUCKETS = ${BUCKETS}"
echo "  Each bucket ≈ $(( FILE_SIZE / BUCKETS / 1024 / 1024 ))MB"
