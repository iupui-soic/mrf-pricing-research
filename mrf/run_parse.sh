#!/bin/bash
# Launch parse_mrf.py inside a tmux session so the run survives SSH /
# Claude disconnects. Logs are tee'd to /data0/mrf/parsed/parse.log so
# progress is inspectable without attaching to tmux.
#
# Usage:
#   mrf/run_parse.sh           # resume from done markers
#   mrf/run_parse.sh --restart # ignore markers, reparse everything
#
# Inspect:
#   tail -f /data0/mrf/parsed/parse.log
#   tmux attach -t mrf-parse        (Ctrl-b then d to detach)
#   tmux ls
#
# Stop:
#   tmux send-keys -t mrf-parse C-c    # graceful: parser flushes the
#                                      # current batch then exits
#   tmux kill-session -t mrf-parse     # hard stop

set -euo pipefail
cd "$(dirname "$0")/.."

SESSION=mrf-parse
LOG=/data0/mrf/parsed/parse.log
mkdir -p "$(dirname "$LOG")"

if tmux has-session -t "$SESSION" 2>/dev/null; then
  echo "Session '$SESSION' already running. Detach with Ctrl-b d, or kill it:"
  echo "  tmux kill-session -t $SESSION"
  exit 1
fi

# -u for unbuffered stdout so the log streams in real time.
# stdbuf -oL forces line-buffering on tee.
CMD=".venv/bin/python -u mrf/parse_mrf.py $* 2>&1 | stdbuf -oL tee \"$LOG\""

tmux new-session -d -s "$SESSION" "$CMD"
echo "Launched in tmux session: $SESSION"
echo "  attach:  tmux attach -t $SESSION  (Ctrl-b d to detach)"
echo "  log:     tail -f $LOG"
echo "  status:  tmux ls"
