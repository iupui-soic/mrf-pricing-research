#!/usr/bin/env bash
# download_data.sh
# ================
# Bootstrap a fresh clone with the /data0/mrf-pricing-research/ corpora
# from a remote server over SSH. Useful when standing up the pipeline on
# a new machine: clone the repo, then run this to pull the inputs that
# the upstream scripts produce (federal MRFs, Medicare fee schedules,
# crosswalk, IN census, analysis outputs, optionally CA chargemaster).
#
# What it copies, by corpus name:
#
#   crosswalk           ~ 160 KB   facilities_crosswalk.parquet + ledgers
#   medicare            ~  90 MB   CMS fee-schedule downloads + extracted
#   census              ~ 200 KB   IN ACS + CDC PLACES parquets
#   analysis            ~  50 MB   ratio panel + Wang + Chang & Psek
#   mrf                 ~  80 GB   federal HPT MRFs + parsed parquets
#   hcai-chargemasters  ~  XX GB   CA chargemaster source files (LARGE,
#                                  opt-in only — set EXPLICIT_CHARGEMASTER=1)
#
# All paths live under /data0/mrf-pricing-research/<corpus>. Override
# the root with SRC_ROOT / DST_ROOT if your layout differs.
#
# Usage:
#   SRC=user@host bash download_data.sh                  # default set
#   SRC=user@host bash download_data.sh medicare census  # selected corpora
#   SRC=user@host DRY_RUN=1 bash download_data.sh        # preview only
#   SRC=user@host EXPLICIT_CHARGEMASTER=1 bash download_data.sh hcai-chargemasters
#
# Env vars:
#   SRC          required. ssh target, e.g. user@otherhost
#   SRC_ROOT     remote root (default: /data0/mrf-pricing-research)
#   DST_ROOT     local root  (default: /data0/mrf-pricing-research)
#   SSH_OPTS     extra ssh flags (e.g. "-i ~/.ssh/id_ed25519 -p 2222")
#   DRY_RUN      set to 1 to add --dry-run
#   BWLIMIT      KB/s cap for rsync (default: unlimited)
#   EXPLICIT_CHARGEMASTER  set to 1 to permit pulling hcai-chargemasters
#
# No credentials live in this script — auth is whatever your local ssh
# config provides for $SRC (key file, agent, ~/.ssh/config Host alias).
# rsync is resumable: re-run after a disconnect to continue.

set -euo pipefail
cd "$(dirname "$0")"

: "${SRC:?set SRC=user@host}"
SRC_ROOT=${SRC_ROOT:-/data0/mrf-pricing-research}
DST_ROOT=${DST_ROOT:-/data0/mrf-pricing-research}
SSH_OPTS=${SSH_OPTS:-}
DRY_RUN=${DRY_RUN:-0}
BWLIMIT=${BWLIMIT:-0}
EXPLICIT_CHARGEMASTER=${EXPLICIT_CHARGEMASTER:-0}

CORPORA=("$@")
if [ ${#CORPORA[@]} -eq 0 ]; then
    # Default: everything except the multi-TB chargemaster source.
    # Order is tiny → big so quick wins land first.
    CORPORA=(crosswalk medicare census analysis mrf)
fi

RSYNC_FLAGS=(-aHv --partial --info=progress2,stats2 --human-readable)
[ "$DRY_RUN" = "1" ] && RSYNC_FLAGS+=(--dry-run)
[ "$BWLIMIT" != "0" ] && RSYNC_FLAGS+=(--bwlimit="$BWLIMIT")

# SSH connection multiplexing: first rsync prompts for the password and
# opens a master socket; subsequent rsyncs reuse it without re-prompting.
# Master persists 12h after last client disconnects.
CM_DIR="$HOME/.ssh/controlmasters"
mkdir -p "$CM_DIR"
chmod 700 "$CM_DIR"
SSH_OPTS="-o ControlMaster=auto -o ControlPath=${CM_DIR}/cm-%r@%h:%p -o ControlPersist=12h ${SSH_OPTS}"
SSH_CMD="ssh ${SSH_OPTS}"

pull() {
    local name=$1
    local src="${SRC}:${SRC_ROOT}/${name}/"
    local dst="${DST_ROOT}/${name}/"

    echo
    echo "[$(date -u +%FT%TZ)] ===== ${name} ====="
    echo "  source: ${src}"
    echo "  dest  : ${dst}"

    if [ ! -d "$dst" ]; then
        echo "  creating $dst"
        mkdir -p "$dst"
    fi

    rsync "${RSYNC_FLAGS[@]}" -e "$SSH_CMD" "$src" "$dst"
    echo "[$(date -u +%FT%TZ)] ${name} done."
}

for c in "${CORPORA[@]}"; do
    case "$c" in
        mrf|medicare|crosswalk|census|analysis) ;;
        hcai-chargemasters)
            if [ "$EXPLICIT_CHARGEMASTER" != "1" ]; then
                echo "refusing: hcai-chargemasters is large (multi-TB); rerun with EXPLICIT_CHARGEMASTER=1 to opt in" >&2
                exit 2
            fi
            ;;
        *)
            echo "unknown corpus: $c" >&2
            echo "  expected one of: crosswalk | medicare | census | analysis | mrf | hcai-chargemasters" >&2
            exit 2 ;;
    esac
done

echo "[$(date -u +%FT%TZ)] ===== plan ====="
echo "  src  : ${SRC}:${SRC_ROOT}"
echo "  dst  : ${DST_ROOT}"
echo "  pull : ${CORPORA[*]}"
[ "$DRY_RUN" = "1" ] && echo "  mode : DRY-RUN"

for c in "${CORPORA[@]}"; do
    pull "$c"
done

echo
echo "[$(date -u +%FT%TZ)] ===== summary ====="
for c in "${CORPORA[@]}"; do
    if [ -d "${DST_ROOT}/${c}" ]; then
        printf "  %-12s " "$c"
        du -sh "${DST_ROOT}/${c}" 2>/dev/null | awk '{print $1}'
    fi
done

echo
echo "verify expected ledgers:"
for f in \
    "${DST_ROOT}/mrf/hospitals.csv" \
    "${DST_ROOT}/mrf/downloads.csv" \
    "${DST_ROOT}/mrf/mrf_urls.csv" \
    "${DST_ROOT}/mrf/parsed/mrf_gross.parquet" \
    "${DST_ROOT}/mrf/parsed/mrf_negotiated.parquet" \
    "${DST_ROOT}/medicare/downloads.csv" \
    "${DST_ROOT}/crosswalk/facilities_crosswalk.parquet" \
    "${DST_ROOT}/census/in_zip_demographics.parquet" \
    "${DST_ROOT}/analysis/ratios_hospital_code.parquet" ; do
    if [ -e "$f" ]; then echo "  ok  $f"; else echo "  MISS $f"; fi
done
