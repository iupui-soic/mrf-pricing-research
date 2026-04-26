#!/bin/bash
# Chains: wait-for-current-concat → parse-retry → re-concat.
# Run inside tmux session `mrf-retry-chain` so it survives disconnects.
#
# Usage:
#   tmux new-session -d -s mrf-retry-chain "mrf/run_retry_chain.sh"
#
# Inspect:
#   tail -f /data0/mrf/parsed/retry_chain.log
#   tmux attach -t mrf-retry-chain   (Ctrl-b d to detach)

set -euo pipefail
cd "$(dirname "$0")/.."

LOG=/data0/mrf/parsed/retry_chain.log
echo "[chain] starting at $(date -Iseconds)" | tee -a "$LOG"

# 1. Wait until the in-flight concat session ends.
while tmux has-session -t mrf-concat 2>/dev/null; do
  echo "[chain] mrf-concat still running, sleeping 10s" | tee -a "$LOG"
  sleep 10
done
echo "[chain] mrf-concat ended at $(date -Iseconds)" | tee -a "$LOG"

# 2. Retry parse — only the 18 CCNs without done markers will be re-attempted.
echo "[chain] launching parse retry" | tee -a "$LOG"
.venv/bin/python -u mrf/parse_mrf.py 2>&1 \
  | stdbuf -oL tee -a /data0/mrf/parsed/parse.log \
  | tee -a "$LOG"

# 3. Re-run concat to fold the new parts into the unified parquets.
echo "[chain] launching re-concat at $(date -Iseconds)" | tee -a "$LOG"
.venv/bin/python -u mrf/concat_parts.py 2>&1 \
  | stdbuf -oL tee /data0/mrf/parsed/concat2.log \
  | tee -a "$LOG"

echo "[chain] done at $(date -Iseconds)" | tee -a "$LOG"
