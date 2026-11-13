#!/usr/bin/env bash
# Downloads one full day of real NASDAQ TotalView-ITCH 5.0 (3.5 GB compressed) and unpacks it.
# Pass a byte count to keep only a prefix, e.g. ./scripts/get_data.sh 2000000000
set -euo pipefail
mkdir -p data
url="https://emi.nasdaq.com/ITCH/Nasdaq%20ITCH/12302019.NASDAQ_ITCH50.gz"
if [ $# -ge 1 ]; then curl -s "$url" | gunzip 2>/dev/null | head -c "$1" > data/12302019.itch || true
else curl -# "$url" | gunzip > data/12302019.itch; fi
ls -la data/12302019.itch
