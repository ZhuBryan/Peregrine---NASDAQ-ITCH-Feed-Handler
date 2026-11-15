#!/usr/bin/env bash
# Differential test: replay the sample over loopback UDP while dropping every 50th
# packet, and require the deterministic part of the output to equal a file replay.
set -euo pipefail
out=$(mktemp -d)
f=${1:-data/sample_50MB.itch}
# No real data checked out (e.g. in CI): fall back to a synthetic stream from the test generator.
if [ ! -f "$f" ]; then make -s build/test; ./build/test --emit "$out/synth.itch"; f=$out/synth.itch; fi
ip=${2:-127.0.0.1}   # pass a 239.x.x.x address to test multicast
./build/itchfh --udp "$ip" 31337 AAPL SPY MSFT > "$out/udp.txt" &
rx=$!
trap 'kill $rx 2>/dev/null || true' EXIT
sleep 0.3
./build/moldsend "$f" "$ip" 31337 --pps 20000 --drop 50
wait $rx
./build/itchfh "$f" AAPL SPY MSFT > "$out/file.txt"
if diff <(grep -v -E 'wall time|throughput|latency' "$out/udp.txt") <(grep -v -E 'wall time|throughput|latency' "$out/file.txt"); then
  echo "udp test passed: output identical to file replay"
else
  echo "udp test FAILED"; exit 1
fi
