#!/usr/bin/env bash
# Zero-Downtime DAG / Wave Orchestrator for the Maximum-Parallelism DRP Campaign
# (USDF submit -> LANCS remote execution)
#
# Features:
#   - Tracks the exact upstream YAML prerequisites of every step and triggers `bps submit`
#     the instant a step's prerequisites reach SUCCEEDED (no waiting for slower peer jobs
#     in the same wave).
#   - Runs `bps submit` (QuantumGraph generation) asynchronously in the background (throttled
#     by --max-parallel-submits, default 2) so QuantumGraph generation for one YAML never
#     blocks polling or submitting other ready YAMLs.
#   - Retry logic:
#     * First try: uses the first extraQgraphOptions line (without --skip-existing-in ... --clobber-outputs)
#       and keeps the second line commented out with "# ".
#     * Retries: comments out the first extraQgraphOptions line with "# " and uncomments the second line
#       (--skip-existing-in {output} --clobber-outputs) so valid outputs are not recomputed.
#   - Persists state in `campaign_state.tsv` so you can stop, restart, or recover mid-campaign.
#   - Supports `--tmux [session_name]` to launch directly inside a detached tmux session.
#   - Supports `--adopt <yaml>=<submit_dir>` and `--mark-done <yaml1>[,<yaml2>...]` to attach
#     to already-running or already-completed steps.

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR" || exit 1

STATE_DIR="${SCRIPT_DIR}/.campaign_state"
STATE_FILE="${STATE_DIR}/campaign_state.tsv"
LOG_FILE="${STATE_DIR}/campaign.log"

POLL_INTERVAL=30
MAX_PARALLEL_SUBMITS=2
RESUBMIT_RETRIES=0
INCLUDE_HIPS=false
DRY_RUN=false
RESET_STATE=false
TMUX_SESSION=""

declare -A ADOPT_MAP
declare -A MARK_DONE_MAP
declare -A SKIP_MAP
declare -A FORCE_RETRY_MAP

usage() {
    cat <<EOF
Usage: $0 [OPTIONS]

Zero-downtime wave/DAG orchestrator for parallel_campaign/*.yaml (USDF -> LANCS).

Options:
  --tmux [SESSION]             Launch this script inside a tmux session (default name: drp_parallel).
  --adopt <yaml>=<submit_dir>  Adopt an already-running BPS submit directory for <yaml> (repeatable).
  --mark-done <yaml[,yaml2]>   Mark one or more YAMLs as already SUCCEEDED (comma-separated or repeatable).
  --skip <yaml[,yaml2]>        Skip one or more YAMLs (and anything that depends on them).
  --retry <yaml[,yaml2]>       Mark one or more YAMLs to run in retry mode (--skip-existing-in ... --clobber-outputs).
  --include-hips               Include USDF-local HiPS steps (step3c-step3d-hips-usdf.yaml and
                               step4e-step4f-hips-usdf.yaml). Excluded by default for remote campaigns.
  --max-parallel-submits <N>   Max concurrent 'bps submit' (QuantumGraph build) processes (default: 2).
  --poll-interval <SEC>        Seconds between 'bps status' polls (default: 30).
  --resubmit-retries <N>       Auto-resubmit a FAILED step up to N times using retry extraQgraphOptions (default: 0).
  --reset                      Clear saved state in .campaign_state/, restore first-try extraQgraphOptions, and start fresh.
  --dry-run                    Simulate 'bps submit' and 'bps status' to verify wave ordering.
  -h, --help                   Show this help message.

Examples:
  # Start full campaign inside a tmux session named 'drp_lancs':
  $0 --tmux drp_lancs

  # Attach to an already-running step1a-step1b job and auto-submit Waves 2..9:
  $0 --adopt step1a-step1b.yaml=/sdf/group/rubin/.../submit/.../20261020T120000Z

  # Resume after step1a-step1b and step2c already succeeded, while step1c1 is running:
  $0 --mark-done step1a-step1b.yaml,step2c.yaml --adopt step1c1-isolated-stars.yaml=/sdf/.../20261020T140000Z
EOF
}

ORIG_ARGS=("$@")
while [[ $# -gt 0 ]]; do
    case "$1" in
        --tmux)
            if [[ $# -gt 1 && "$2" != --* ]]; then
                TMUX_SESSION="$2"
                shift 2
            else
                TMUX_SESSION="drp_parallel"
                shift 1
            fi
            ;;
        --adopt)
            kv="$2"
            k="${kv%%=*}"
            v="${kv#*=}"
            ADOPT_MAP["$k"]="$v"
            shift 2
            ;;
        --mark-done)
            IFS=',' read -r -a items <<< "$2"
            for item in "${items[@]}"; do
                MARK_DONE_MAP["$item"]=1
            done
            shift 2
            ;;
        --skip)
            IFS=',' read -r -a items <<< "$2"
            for item in "${items[@]}"; do
                SKIP_MAP["$item"]=1
            done
            shift 2
            ;;
        --retry)
            IFS=',' read -r -a items <<< "$2"
            for item in "${items[@]}"; do
                FORCE_RETRY_MAP["$item"]=1
            done
            shift 2
            ;;
        --include-hips)
            INCLUDE_HIPS=true
            shift 1
            ;;
        --max-parallel-submits)
            MAX_PARALLEL_SUBMITS="$2"
            shift 2
            ;;
        --poll-interval)
            POLL_INTERVAL="$2"
            shift 2
            ;;
        --resubmit-retries)
            RESUBMIT_RETRIES="$2"
            shift 2
            ;;
        --reset)
            RESET_STATE=true
            shift 1
            ;;
        --dry-run)
            DRY_RUN=true
            if [ "$POLL_INTERVAL" -eq 30 ]; then
                POLL_INTERVAL=1
            fi
            shift 1
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1" >&2
            usage
            exit 1
            ;;
    esac
done

# If --tmux was requested and we are not already inside that tmux session, launch tmux
if [ -n "$TMUX_SESSION" ] && [ "${IN_CAMPAIGN_TMUX:-}" != "1" ]; then
    filtered_args=()
    skip_next=false
    for ((i=0; i<${#ORIG_ARGS[@]}; i++)); do
        if $skip_next; then
            skip_next=false
            continue
        fi
        arg="${ORIG_ARGS[$i]}"
        if [ "$arg" = "--tmux" ]; then
            if (( i+1 < ${#ORIG_ARGS[@]} )) && [[ "${ORIG_ARGS[$((i+1))]}" != --* ]]; then
                skip_next=true
            fi
            continue
        fi
        filtered_args+=("$arg")
    done
    cmd_str="IN_CAMPAIGN_TMUX=1 \"$0\""
    for a in "${filtered_args[@]}"; do
        cmd_str+=" $(printf '%q' "$a")"
    done
    cmd_str+="; exec bash"
    if tmux has-session -t "$TMUX_SESSION" 2>/dev/null; then
        echo "tmux session '$TMUX_SESSION' already exists. Attach with: tmux attach -t $TMUX_SESSION"
        exit 1
    fi
    tmux new-session -d -s "$TMUX_SESSION" -c "$SCRIPT_DIR" "$cmd_str"
    echo "Started campaign orchestrator in tmux session '$TMUX_SESSION'."
    echo "Attach with:  tmux attach -t $TMUX_SESSION"
    exit 0
fi

mkdir -p "$STATE_DIR"
if [ "$RESET_STATE" = "true" ]; then
    rm -f "$STATE_FILE" "$STATE_DIR"/*.submitting "$STATE_DIR"/*.submit_dir "$STATE_DIR"/*.submit_rc
fi

log() {
    local msg="[$(date '+%Y-%m-%d %H:%M:%S')] $*"
    echo "$msg" | tee -a "$LOG_FILE"
}

# Comment out the first-try extraQgraphOptions line ("# extraQgraphOptions: ...")
# and uncomment the second retry line ("extraQgraphOptions: ... --skip-existing-in ... --clobber-outputs").
# Ensures uncommented extraQgraphOptions appears only once in the file.
enable_retry_qgraph_options() {
    local yaml="$1"
    [ ! -f "$yaml" ] && return 1
    if [ "$DRY_RUN" = "true" ]; then
        log "[RETRY-CONFIG] (dry-run) Would comment first extraQgraphOptions line and uncomment retry line in $yaml"
        return 0
    fi
    awk '
    {
        lines[NR] = $0
    }
    END {
        done_retry = 0
        for (i = 1; i <= NR; i++) {
            # Case 1: Active first-try line (uncommented, without --skip-existing-in)
            if (lines[i] ~ /^[[:space:]]*extraQgraphOptions:/ && lines[i] !~ /--skip-existing-in/) {
                base_line = lines[i]
                sub(/^[[:space:]]*/, "", base_line)
                print "# " base_line
                if (i < NR && lines[i+1] ~ /^[[:space:]]*#[[:space:]]*extraQgraphOptions:.*--skip-existing-in.*--clobber-outputs/) {
                    retry_line = lines[i+1]
                    sub(/^[[:space:]]*#[[:space:]]*/, "", retry_line)
                    print retry_line
                    i++
                } else {
                    retry_line = base_line
                    sub(/"[[:space:]]*$/, " --skip-existing-in {output} --clobber-outputs\"", retry_line)
                    print retry_line
                }
                done_retry = 1
            }
            # Case 2: Already active retry line (uncommented, with --skip-existing-in ... --clobber-outputs)
            else if (lines[i] ~ /^[[:space:]]*extraQgraphOptions:.*--skip-existing-in.*--clobber-outputs/) {
                if (done_retry == 0) {
                    print lines[i]
                    done_retry = 1
                } else {
                    sub_line = lines[i]
                    sub(/^[[:space:]]*/, "", sub_line)
                    print "# " sub_line
                }
            }
            # Case 3: Retry-only line with no preceding first-try line (e.g. step3b.yaml)
            else if (done_retry == 0 && lines[i] ~ /^[[:space:]]*#[[:space:]]*extraQgraphOptions:.*--skip-existing-in.*--clobber-outputs/ && (i == 1 || lines[i-1] !~ /extraQgraphOptions:/)) {
                retry_line = lines[i]
                sub(/^[[:space:]]*#[[:space:]]*/, "", retry_line)
                print retry_line
                done_retry = 1
            }
            else {
                print lines[i]
            }
        }
    }
    ' "$yaml" > "${yaml}.tmp" && mv "${yaml}.tmp" "$yaml"
    log "[RETRY-CONFIG] Enabled retry extraQgraphOptions (--skip-existing-in ... --clobber-outputs) in $yaml"
}

# Ensure the first-try extraQgraphOptions line is uncommented and the second retry line is commented with "# ".
enable_first_try_qgraph_options() {
    local yaml="$1"
    [ ! -f "$yaml" ] && return 1
    [ "$DRY_RUN" = "true" ] && return 0
    awk '
    {
        lines[NR] = $0
    }
    END {
        for (i = 1; i <= NR; i++) {
            if (lines[i] ~ /^[[:space:]]*extraQgraphOptions:.*--skip-existing-in.*--clobber-outputs/) {
                if (i > 1 && lines[i-1] ~ /^[[:space:]]*#[[:space:]]*extraQgraphOptions:/ && lines[i-1] !~ /--skip-existing-in/) {
                    sub(/^[[:space:]]*#[[:space:]]*/, "", lines[i-1])
                    sub(/^[[:space:]]*/, "", lines[i])
                    lines[i] = "# " lines[i]
                } else if (lines[i] ~ /^[[:space:]]*extraQgraphOptions:[[:space:]]*"--skip-existing-in/) {
                    sub(/^[[:space:]]*/, "", lines[i])
                    lines[i] = "# " lines[i]
                } else {
                    base_line = lines[i]
                    sub(/^[[:space:]]*/, "", base_line)
                    sub(/[[:space:]]+--skip-existing-in[[:space:]]+[^[:space:]"]+[[:space:]]+--clobber-outputs/, "", base_line)
                    sub(/^[[:space:]]*/, "", lines[i])
                    lines[i] = base_line "\n# " lines[i]
                }
            }
        }
        for (i = 1; i <= NR; i++) {
            print lines[i]
        }
    }
    ' "$yaml" > "${yaml}.tmp" && mv "${yaml}.tmp" "$yaml"
}

# Ordered list of steps (ordered by critical-path priority within each wave)
STEPS=(
    # Wave 1
    "step1a-step1b.yaml"
    # Wave 2 (critical-path step1c1 & step2c first, then step1d1)
    "step1c1-isolated-stars.yaml"
    "step2c.yaml"
    "step1d1-visit-tables.yaml"
    # Wave 3 (critical-path step2a first, then non-blocking QA leaf)
    "step2a.yaml"
    "step1c2-step1d2-qa.yaml"
    # Wave 4
    "step2d.yaml"
    # Wave 5 (critical-path warps & visit reprocess first, then ephemerides & 2e-2f)
    "step3a1-warps.yaml"
    "step4a1-reprocess-skycorr.yaml"
    "step4b.yaml"
    "step2e-step2f.yaml"
    # Wave 6 (critical-path coadd images first, then source QA)
    "step3a2-coadd-images.yaml"
    "step4c2-source-qa.yaml"
    # Wave 7 (critical-path diffim & multiband objects first, then pretty coadds & optional USDF HiPS)
    "step4a2-diffim.yaml"
    "step3a3-multiband-objects.yaml"
    "step4c1-pretty-coadds.yaml"
    "step3c-step3d-hips-usdf.yaml"
    # Wave 8 (DIA association/forced phot & Object forced phot first, then terminal leaves)
    "step4c3-dia-assoc-forced.yaml"
    "step4c4-object-forced.yaml"
    "step3b.yaml"
    "step4a3-qa-star-object-match.yaml"
    "step4e-step4f-hips-usdf.yaml"
    # Wave 9 (terminal global leaf)
    "step4d.yaml"
)

declare -A WAVE_LABEL=(
    ["step1a-step1b.yaml"]="Wave1"
    ["step1c1-isolated-stars.yaml"]="Wave2"
    ["step2c.yaml"]="Wave2"
    ["step1d1-visit-tables.yaml"]="Wave2"
    ["step2a.yaml"]="Wave3"
    ["step1c2-step1d2-qa.yaml"]="Wave3-QA"
    ["step2d.yaml"]="Wave4"
    ["step3a1-warps.yaml"]="Wave5"
    ["step4a1-reprocess-skycorr.yaml"]="Wave5"
    ["step4b.yaml"]="Wave5"
    ["step2e-step2f.yaml"]="Wave5"
    ["step3a2-coadd-images.yaml"]="Wave6"
    ["step4c2-source-qa.yaml"]="Wave6"
    ["step4a2-diffim.yaml"]="Wave7"
    ["step3a3-multiband-objects.yaml"]="Wave7"
    ["step4c1-pretty-coadds.yaml"]="Wave7"
    ["step3c-step3d-hips-usdf.yaml"]="Wave7-HiPS-USDF"
    ["step4c3-dia-assoc-forced.yaml"]="Wave8"
    ["step4c4-object-forced.yaml"]="Wave8"
    ["step3b.yaml"]="Wave8-Global"
    ["step4a3-qa-star-object-match.yaml"]="Wave8-QA"
    ["step4e-step4f-hips-usdf.yaml"]="Wave8-HiPS-USDF"
    ["step4d.yaml"]="Wave9-Global"
)

# Exact direct upstream YAML prerequisites for each step
declare -A DEPS=(
    ["step1a-step1b.yaml"]=""
    ["step1c1-isolated-stars.yaml"]="step1a-step1b.yaml"
    ["step1d1-visit-tables.yaml"]="step1a-step1b.yaml"
    ["step2c.yaml"]="step1a-step1b.yaml"
    ["step2a.yaml"]="step1a-step1b.yaml,step1c1-isolated-stars.yaml"
    ["step1c2-step1d2-qa.yaml"]="step1c1-isolated-stars.yaml"
    ["step2d.yaml"]="step1a-step1b.yaml,step1c1-isolated-stars.yaml,step2a.yaml,step2c.yaml"
    ["step2e-step2f.yaml"]="step1d1-visit-tables.yaml,step2d.yaml"
    ["step4b.yaml"]="step1d1-visit-tables.yaml,step2d.yaml"
    ["step4a1-reprocess-skycorr.yaml"]="step1a-step1b.yaml,step2d.yaml"
    ["step3a1-warps.yaml"]="step1a-step1b.yaml,step2d.yaml"
    ["step3a2-coadd-images.yaml"]="step3a1-warps.yaml"
    ["step4c2-source-qa.yaml"]="step2e-step2f.yaml,step4a1-reprocess-skycorr.yaml"
    ["step3a3-multiband-objects.yaml"]="step3a2-coadd-images.yaml"
    ["step4a2-diffim.yaml"]="step3a2-coadd-images.yaml,step4a1-reprocess-skycorr.yaml"
    ["step4c1-pretty-coadds.yaml"]="step3a2-coadd-images.yaml,step4a1-reprocess-skycorr.yaml"
    ["step3c-step3d-hips-usdf.yaml"]="step3a2-coadd-images.yaml"
    ["step4c3-dia-assoc-forced.yaml"]="step4a2-diffim.yaml,step4b.yaml"
    ["step4c4-object-forced.yaml"]="step3a3-multiband-objects.yaml,step4a2-diffim.yaml"
    ["step3b.yaml"]="step3a2-coadd-images.yaml,step3a3-multiband-objects.yaml"
    ["step4a3-qa-star-object-match.yaml"]="step2e-step2f.yaml,step3a3-multiband-objects.yaml"
    ["step4e-step4f-hips-usdf.yaml"]="step4c1-pretty-coadds.yaml"
    ["step4d.yaml"]="step4c2-source-qa.yaml,step4c3-dia-assoc-forced.yaml"
)

declare -A STATUS
declare -A SUBMIT_DIR
declare -A RETRIES_LEFT
declare -A IS_RETRY

for s in "${STEPS[@]}"; do
    STATUS["$s"]="PENDING"
    SUBMIT_DIR["$s"]="-"
    RETRIES_LEFT["$s"]="$RESUBMIT_RETRIES"
    IS_RETRY["$s"]=0
    if [ "$RESET_STATE" = "true" ]; then
        enable_first_try_qgraph_options "$s"
    fi
done

# Load existing state if present
if [ -f "$STATE_FILE" ]; then
    while IFS=$'\t' read -r s st sdir rleft isret; do
        [[ -z "$s" || "$s" == \#* ]] && continue
        STATUS["$s"]="$st"
        SUBMIT_DIR["$s"]="$sdir"
        RETRIES_LEFT["$s"]="${rleft:-$RESUBMIT_RETRIES}"
        IS_RETRY["$s"]="${isret:-0}"
        if [ "$st" = "SUBMITTING" ]; then
            STATUS["$s"]="PENDING"
        elif [ "$st" = "FAILED" ]; then
            # Re-running after a failed step automatically queues that step as a retry
            STATUS["$s"]="PENDING"
            IS_RETRY["$s"]=1
            RETRIES_LEFT["$s"]="$RESUBMIT_RETRIES"
            enable_retry_qgraph_options "$s"
        elif [ "$st" = "BLOCKED" ]; then
            STATUS["$s"]="PENDING"
        fi
    done < "$STATE_FILE"
fi

# Apply HiPS default skip unless --include-hips
if [ "$INCLUDE_HIPS" != "true" ]; then
    for hs in "step3c-step3d-hips-usdf.yaml" "step4e-step4f-hips-usdf.yaml"; do
        if [ "${STATUS[$hs]}" = "PENDING" ]; then
            STATUS["$hs"]="SKIPPED"
        fi
    done
fi

# Apply CLI overrides (--retry, --skip, --mark-done, --adopt)
for s in "${!FORCE_RETRY_MAP[@]}"; do
    IS_RETRY["$s"]=1
    enable_retry_qgraph_options "$s"
done
for s in "${!SKIP_MAP[@]}"; do
    STATUS["$s"]="SKIPPED"
done
for s in "${!MARK_DONE_MAP[@]}"; do
    STATUS["$s"]="SUCCEEDED"
    [ "${SUBMIT_DIR[$s]}" = "-" ] && SUBMIT_DIR["$s"]="MARKED_DONE"
done
for s in "${!ADOPT_MAP[@]}"; do
    STATUS["$s"]="RUNNING"
    SUBMIT_DIR["$s"]="${ADOPT_MAP[$s]}"
done

save_state() {
    {
        echo -e "#YAML\tSTATUS\tSUBMIT_DIR\tRETRIES_LEFT\tIS_RETRY"
        for s in "${STEPS[@]}"; do
            echo -e "${s}\t${STATUS[$s]}\t${SUBMIT_DIR[$s]}\t${RETRIES_LEFT[$s]}\t${IS_RETRY[$s]}"
        done
    } > "${STATE_FILE}.tmp"
    mv "${STATE_FILE}.tmp" "$STATE_FILE"
}

save_state

extract_submit_dir() {
    awk '/Submitting from directory:/ {dir=$NF} /^Submit dir:/ {dir=$NF} END {print dir}'
}

get_bps_status() {
    local target_dir="$1"
    if [ "$DRY_RUN" = "true" ]; then
        echo "SUCCEEDED"
        return
    fi
    local out
    out=$(bps status --id "$target_dir" 2>&1)
    local st
    st=$(echo "$out" | awk '/status:/ {s=$NF} END {print s}')
    if [ -z "$st" ]; then
        st=$(echo "$out" | awk 'NF {s=$NF} END {print s}')
    fi
    echo "${st:-UNKNOWN}"
}

start_async_submit() {
    local yaml="$1"
    if [ "${IS_RETRY[$yaml]:-0}" -eq 1 ]; then
        enable_retry_qgraph_options "$yaml"
    else
        enable_first_try_qgraph_options "$yaml"
    fi
    STATUS["$yaml"]="SUBMITTING"
    save_state
    rm -f "${STATE_DIR}/${yaml}.submit_dir" "${STATE_DIR}/${yaml}.submit_rc"
    touch "${STATE_DIR}/${yaml}.submitting"

    local mode_str="first-try"
    [ "${IS_RETRY[$yaml]:-0}" -eq 1 ] && mode_str="retry (--skip-existing-in --clobber-outputs)"
    log "[${WAVE_LABEL[$yaml]}] Launching bps submit $yaml [$mode_str] ..."
    (
        if [ "$DRY_RUN" = "true" ]; then
            sleep 1
            echo "/dry_run/submit/${yaml%.yaml}_$(date '+%H%M%S')" > "${STATE_DIR}/${yaml}.submit_dir"
            echo "0" > "${STATE_DIR}/${yaml}.submit_rc"
        else
            bps submit "$yaml" > "${STATE_DIR}/${yaml}.submit.log" 2>&1
            rc=$?
            sdir=$(extract_submit_dir < "${STATE_DIR}/${yaml}.submit.log")
            echo "$sdir" > "${STATE_DIR}/${yaml}.submit_dir"
            echo "$rc" > "${STATE_DIR}/${yaml}.submit_rc"
        fi
        rm -f "${STATE_DIR}/${yaml}.submitting"
    ) &
}

deps_satisfied() {
    local yaml="$1"
    local dep_str="${DEPS[$yaml]}"
    [ -z "$dep_str" ] && return 0
    IFS=',' read -r -a dlist <<< "$dep_str"
    for d in "${dlist[@]}"; do
        if [ "${STATUS[$d]}" != "SUCCEEDED" ]; then
            return 1
        fi
    done
    return 0
}

deps_blocked() {
    local yaml="$1"
    local dep_str="${DEPS[$yaml]}"
    [ -z "$dep_str" ] && return 1
    IFS=',' read -r -a dlist <<< "$dep_str"
    for d in "${dlist[@]}"; do
        if [ "${STATUS[$d]}" = "FAILED" ] || [ "${STATUS[$d]}" = "BLOCKED" ] || [ "${STATUS[$d]}" = "SKIPPED" ]; then
            return 0
        fi
    done
    return 1
}

log "=================================================================="
log "Starting Zero-Downtime DRP Campaign Orchestrator (parallel_campaign)"
log "State file: $STATE_FILE | Log file: $LOG_FILE"
log "Max parallel submits: $MAX_PARALLEL_SUBMITS | Poll interval: ${POLL_INTERVAL}s | Include HiPS: $INCLUDE_HIPS"
log "=================================================================="

while true; do
    state_changed=false

    # 1. Check any completed background `bps submit` processes
    active_submits=0
    for s in "${STEPS[@]}"; do
        if [ "${STATUS[$s]}" = "SUBMITTING" ]; then
            if [ -f "${STATE_DIR}/${s}.submitting" ]; then
                active_submits=$((active_submits + 1))
            else
                rc=$(cat "${STATE_DIR}/${s}.submit_rc" 2>/dev/null || echo 1)
                sdir=$(cat "${STATE_DIR}/${s}.submit_dir" 2>/dev/null || echo "")
                if [ "$rc" -eq 0 ] && [ -n "$sdir" ]; then
                    STATUS["$s"]="RUNNING"
                    SUBMIT_DIR["$s"]="$sdir"
                    log "[${WAVE_LABEL[$s]}] SUBMITTED $s -> $sdir"
                else
                    STATUS["$s"]="FAILED"
                    log "[${WAVE_LABEL[$s]}] ERROR: bps submit failed for $s (see ${STATE_DIR}/${s}.submit.log)"
                fi
                state_changed=true
            fi
        fi
    done

    # 2. Poll all RUNNING steps via `bps status`
    for s in "${STEPS[@]}"; do
        if [ "${STATUS[$s]}" = "RUNNING" ]; then
            sdir="${SUBMIT_DIR[$s]}"
            st=$(get_bps_status "$sdir")
            if [ "$st" = "SUCCEEDED" ]; then
                STATUS["$s"]="SUCCEEDED"
                log "[${WAVE_LABEL[$s]}] SUCCEEDED: $s ($sdir)"
                state_changed=true
            elif [ "$st" = "FAILED" ] || [ "$st" = "UNKNOWN" ] || [ "$st" = "DELETED" ] || [ "$st" = "PRUNED" ]; then
                IS_RETRY["$s"]=1
                enable_retry_qgraph_options "$s"
                if [ "${RETRIES_LEFT[$s]}" -gt 0 ]; then
                    RETRIES_LEFT["$s"]=$((RETRIES_LEFT["$s"] - 1))
                    log "[${WAVE_LABEL[$s]}] WARNING: $s reported $st. Re-queueing submission in retry mode (${RETRIES_LEFT[$s]} retries left)..."
                    STATUS["$s"]="PENDING"
                else
                    STATUS["$s"]="FAILED"
                    log "[${WAVE_LABEL[$s]}] FAILED ($st): $s ($sdir). YAML updated to retry mode for next run."
                fi
                state_changed=true
            fi
        fi
    done

    # 3. Mark any PENDING steps whose upstream failed as BLOCKED, and immediately
    #    launch any PENDING steps whose upstream prerequisites are all SUCCEEDED!
    for s in "${STEPS[@]}"; do
        if [ "${STATUS[$s]}" = "PENDING" ]; then
            if deps_blocked "$s"; then
                STATUS["$s"]="BLOCKED"
                log "[${WAVE_LABEL[$s]}] BLOCKED (upstream failed/skipped): $s"
                state_changed=true
            elif deps_satisfied "$s"; then
                if [ "$active_submits" -lt "$MAX_PARALLEL_SUBMITS" ]; then
                    start_async_submit "$s"
                    active_submits=$((active_submits + 1))
                    state_changed=true
                fi
            fi
        fi
    done

    save_state

    # 4. Summarize campaign progress
    n_succ=0; n_run=0; n_sub=0; n_pend=0; n_fail=0; n_blk=0; n_skip=0
    running_list=()
    for s in "${STEPS[@]}"; do
        case "${STATUS[$s]}" in
            SUCCEEDED)  n_succ=$((n_succ + 1)) ;;
            RUNNING)    n_run=$((n_run + 1)); running_list+=("$s") ;;
            SUBMITTING) n_sub=$((n_sub + 1)); running_list+=("$s(submitting)") ;;
            PENDING)    n_pend=$((n_pend + 1)) ;;
            FAILED)     n_fail=$((n_fail + 1)) ;;
            BLOCKED)    n_blk=$((n_blk + 1)) ;;
            SKIPPED)    n_skip=$((n_skip + 1)) ;;
        esac
    done

    log "Progress: SUCCEEDED=$n_succ RUNNING=$n_run SUBMITTING=$n_sub PENDING=$n_pend SKIPPED=$n_skip FAILED=$n_fail BLOCKED=$n_blk | Active: ${running_list[*]:-none}"

    # Exit if no steps are PENDING, SUBMITTING, or RUNNING
    if [ "$n_run" -eq 0 ] && [ "$n_sub" -eq 0 ] && [ "$n_pend" -eq 0 ]; then
        break
    fi

    # If a step just finished and another is still waiting for a submit slot, loop faster
    if [ "$state_changed" = "true" ] && [ "$active_submits" -gt 0 ]; then
        sleep 2
    else
        sleep "$POLL_INTERVAL"
    fi
done

log "=================================================================="
if [ "$n_fail" -eq 0 ] && [ "$n_blk" -eq 0 ]; then
    log "CAMPAIGN COMPLETED SUCCESSFULLY! ($n_succ succeeded, $n_skip skipped)"
    exit 0
else
    log "CAMPAIGN FINISHED WITH ERRORS ($n_succ succeeded, $n_fail failed, $n_blk blocked, $n_skip skipped)."
    log "Failed step YAML(s) have been switched to retry mode (--skip-existing-in ... --clobber-outputs)."
    log "Re-run $0 to automatically retry failed step(s) and continue."
    exit 1
fi
