#!/usr/bin/env bash
# run_crawl.sh
# ============
# End-to-end MRF crawl pipeline, designed to run under tmux so it
# survives SSH disconnects.
#
# Stages:
#   1. build_hospital_list.py    — fetch CMS Hospital General Info
#   2. discover_mrf_urls.py      — find each hospital's MRF URL
#   3. download_mrfs.py          — download each MRF file
#
# Resume-safe: each stage can be re-run with --resume to pick up where
# the previous run left off.
#
# Usage:
#   tmux new-session -d -s mrf 'bash mrf/run_crawl.sh'
#   tmux attach -t mrf          # to view progress
#   Ctrl-b d                    # detach
#
# To re-attach after SSH reconnect:
#   tmux attach -t mrf

set -e
cd "$(dirname "$0")/.."

VENV=${VENV:-.venv}
LOG_DIR=${LOG_DIR:-/data0/mrf/logs}
mkdir -p "$LOG_DIR"
TS=$(date -u +"%Y%m%dT%H%M%SZ")

echo "[$(date -u)] ====== MRF crawl pipeline starting ======"
echo "[$(date -u)] venv: $VENV"
echo "[$(date -u)] logs: $LOG_DIR"

echo
echo "[$(date -u)] ====== Stage 1: hospital list ======"
"$VENV/bin/python" mrf/build_hospital_list.py 2>&1 | tee "$LOG_DIR/01_hospitals_$TS.log"

echo
echo "[$(date -u)] ====== Stage 2: MRF URL discovery ======"
"$VENV/bin/python" mrf/discover_mrf_urls.py --resume --workers 8 2>&1 \
    | tee "$LOG_DIR/02_discover_$TS.log"

echo
echo "[$(date -u)] ====== Stage 3: MRF download ======"
"$VENV/bin/python" mrf/download_mrfs.py --resume --workers 8 2>&1 \
    | tee "$LOG_DIR/03_download_$TS.log"

echo
echo "[$(date -u)] ====== Pipeline complete ======"
df -h /data0 | head -2
echo
echo "MRF files tree:"
find /data0/mrf/files -type f | wc -l
du -sh /data0/mrf/files 2>/dev/null || echo "(no files yet)"
