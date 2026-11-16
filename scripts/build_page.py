#!/usr/bin/env python3
"""Builds the self-contained demo page: inlines docs/data/*.json into web/page.html.
  python3 scripts/build_page.py            # writes docs/index.html
Regenerate the data with:
  gunzip -c day.gz | ./build/itchfh --signal --export AAPL docs/data/day.json -
  gunzip -c day.gz | ./build/queue_audit - --priority-by-reference --json docs/data/queue.json"""
import json
import pathlib
import sys

root = pathlib.Path(__file__).resolve().parent.parent
src = pathlib.Path(sys.argv[1]) if len(sys.argv) > 1 else root / "web" / "page.html"
data = pathlib.Path(sys.argv[2]) if len(sys.argv) > 2 else root / "docs" / "data" / "day.json"
out = pathlib.Path(sys.argv[3]) if len(sys.argv) > 3 else root / "docs" / "index.html"
compact = lambda path: json.dumps(json.loads(path.read_text()), separators=(",", ":"))
page = src.read_text()
assert "/*DAY_JSON*/" in page and "/*QUEUE_JSON*/" in page
page = page.replace("/*DAY_JSON*/", compact(data)).replace("/*QUEUE_JSON*/", compact(data.with_name("queue.json")))
out.write_text(page)
print(f"wrote {out} ({out.stat().st_size} bytes)")
