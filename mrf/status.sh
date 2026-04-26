#!/usr/bin/env bash
# Quick-status helper for the tmux-hosted MRF crawl.
# Usage:
#   bash mrf/status.sh              — one-shot status dump
#   tmux attach -t mrf              — follow the live log
#   tmux send-keys -t mrf 'C-c' && tmux kill-session -t mrf  — stop cleanly

set -e
echo "=== tmux sessions ==="
tmux list-sessions 2>&1 | grep -E "^mrf:" || echo "  (no mrf session)"

echo
echo "=== tmux mrf tail ==="
tmux capture-pane -t mrf -p 2>/dev/null | tail -10 || echo "  (session not running)"

echo
echo "=== disk ==="
du -sh /data0/mrf/files 2>/dev/null
df -h /data0 | head -2

echo
echo "=== file counts by state ==="
find /data0/mrf/files -type f 2>/dev/null | awk -F/ '{print $5}' | sort | uniq -c

echo
echo "=== downloads.csv ==="
if [ -f /data0/mrf/downloads.csv ]; then
  python3 -c "
import pandas as pd
d = pd.read_csv('/data0/mrf/downloads.csv', dtype=str)
print(f'  total rows: {len(d):,}')
print('  status:')
for s, n in d['status'].value_counts().items():
    print(f'    {s:30s} {n}')
try:
    d['bytes_downloaded'] = pd.to_numeric(d['bytes_downloaded'], errors='coerce')
    total_gb = d['bytes_downloaded'].sum() / 1024**3
    print(f'  bytes downloaded: {total_gb:.2f} GB')
except Exception as e:
    pass
"
else
  echo "  (no downloads.csv yet)"
fi

echo
echo "=== latest log ==="
ls -lt /data0/mrf/logs/*.log 2>/dev/null | head -3 | awk '{print " ", $0}'
