#!/usr/bin/env bash
# Benchmarks the baseline and optimized builds on a slice of real NASDAQ data.
#   ./scripts/bench.sh [bytes] [runs]        default: 2 GB slice, 3 runs per build
# Pinning: FEED_CORE and CONS_CORE pick the two cores (default 2 and 4, which are
# separate physical cores on most SMT machines; check with `lscpu -e`).
set -euo pipefail
bytes=${1:-2000000000}
runs=${2:-3}
feed=${FEED_CORE:-2}
cons=${CONS_CORE:-4}
if [ "$(nproc)" -le "$cons" ]; then feed=0; cons=1; fi

make -s all
f=data/12302019.itch
if [ ! -f "$f" ] || [ "$(stat -c %s "$f")" -lt "$bytes" ]; then ./scripts/get_data.sh "$bytes" > /dev/null; fi

echo "cpu:    $(lscpu | sed -n 's/^Model name: *//p')"
echo "cores:  $(nproc) online, feed thread on $feed, consumer on $cons"
echo "kernel: $(uname -r)"
echo "data:   $(stat -c %s "$f") bytes of $f"
echo
printf '%-18s %4s %14s %10s %8s %8s %10s\n' build run "M msgs/s" "avg ns" p50 p99 p99.9
for bin in itchfh_baseline itchfh; do
  for r in $(seq "$runs"); do
    out=$(./build/$bin --pin "$feed,$cons" "$f")
    echo "$out" | grep -q "0 unknown order refs" || { echo "integrity check failed"; exit 1; }
    tput=$(echo "$out" | sed -n 's/^throughput *\([0-9.]*\) M.*(\([0-9.]*\) ns.*/\1 \2/p')
    lat=$(echo "$out" | sed -n 's/^feed latency *p50 \([0-9]*\) *p99 \([0-9]*\) *p99.9 \([0-9]*\).*/\1 \2 \3/p')
    printf '%-18s %4s %14s %10s %8s %8s %10s\n' "$bin" "$r" $tput $lat
  done
done
