#!/usr/bin/env bash
# Zero-Downtime DAG / Wave Orchestrator for the Baseline Campaign YAMLs in pipelines_campaign/
# (USDF submit -> LANCS remote execution)
#
# Submits the existing unsplit YAMLs in 8 waves with zero downtime between dependent steps,
# using step3b.yaml by default (since HiPS step3c/step3d are not generated at remote sites).
#
# Retry behavior:
# - First try: uses the first extraQgraphOptions line (without --skip-existing-in ... --clobber-outputs)
#   and keeps the second line commented out with "# ".
# - Retries: comments out the first extraQgraphOptions line with "# " and uncomments the second line
#   (--skip-existing-in {output} --clobber-outputs) so valid outputs are not recomputed.

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR" || exit 1

STATE_DIR="${SCRIPT_DIR}/.campaign_state"
STATE_FILE="${STATE_DIR}/campaign_state.tsv"
LOG_FILE="${STATE_DIR}/campaign.log"

POLL_INTERVAL=30
MAX_PARALLEL_SUBMITS=2
RESUBMIT_RETRIES=0
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

Zero-downtime wave/DAG orchestrator for baseline pipelines_campaign/*.yaml (USDF -> LANCS).

Options:
  --tmux [SESSION]             Launch this script inside a tmux session (default name: drp_baseline).
  --adopt <yaml>=<submit_dir>  Adopt an already-running BPS submit directory for <yaml> (repeatable).
  --mark-done <yaml[,yaml2]>   Mark one or more YAMLs as already SUCCEEDED.
  --skip <yaml[,yaml2]>        Skip one or more YAMLs.
  --retry <yaml[,yaml2]>       Mark one or more YAMLs to run in retry mode (--skip-existing-in ... --clobber-outputs).
  --max-parallel-submits <N>   Max concurrent 'bps submit' processes (default: 2).
  --poll-interval <SEC>        Seconds between 'bps status' polls (default: 30).
  --resubmit-retries <N>       Auto-resubmit a FAILED step up to N times using retry extraQgraphOptions (default: 0).
  --reset                      Clear saved state in .campaign_state/, restore first-try extraQgraphOptions, and start fresh.
  --dry-run                    Simulate 'bps submit' and 'bps status' to verify wave ordering.
  -h, --help                   Show this help message.
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
                TMUX_SESSION="drp_baseline"
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
            for item in "${items[@]}"; do MARK_DONE_MAP["$item"]=1; done
            shift 2
            ;;
        --skip)
            IFS=',' read -r -a items <<< "$2"
            for item in "${items[@]}"; do SKIP_MAP["$item"]=1; done
            shift 2
            ;;
        --retry)
            IFS=',' read -r -a items <<< "$2"
            for item in "${items[@]}"; do FORCE_RETRY_MAP["$item"]=1; done
            shift 2
            ;;
        --max-parallel-submits)
            MAX_PARALLEL_SUBMITS="$2"; shift 2 ;;
        --poll-interval)
            POLL_INTERVAL="$2"; shift 2 ;;
        --resubmit-retries)
            RESUBMIT_RETRIES="$2"; shift 2 ;;
        --reset)
            RESET_STATE=true; shift 1 ;;
        --dry-run)
            DRY_RUN=true
            [ "$POLL_INTERVAL" -eq 30 ] && POLL_INTERVAL=1
            shift 1
            ;;
        -h|--help)
            usage; exit 0 ;;
        *)
            echo "Unknown option: $1" >&2; usage; exit 1 ;;
    esac
done

if [ -n "$TMUX_SESSION" ] && [ "${IN_CAMPAIGN_TMUX:-}" != "1" ]; then
    filtered_args=()
    skip_next=false
    for ((i=0; i<${#ORIG_ARGS[@]}; i++)); do
        if $skip_next; then skip_next=false; continue; fi
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
    for a in "${filtered_args[@]}"; do cmd_str+=" $(printf '%q' "$a")"; done
    cmd_str+="; exec bash"
    if tmux has-session -t "$TMUX_SESSION" 2>/dev/null; then
        echo "tmux session '$TMUX_SESSION' already exists. Attach with: tmux attach -t $TMUX_SESSION"
        exit 1
    fi
    tmux new-session -d -s "$TMUX_SESSION" -c "$SCRIPT_DIR" "$cmd_str"
    echo "Started baseline campaign orchestrator in tmux session '$TMUX_SESSION'."
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

STEPS=(
    "step1a-step1b.yaml"
    "step1c_step1d.yaml"
    "step2c.yaml"
    "step2a.yaml"
    "step2d.yaml"
    "step3a1-warps.yaml"
    "step2e-step2f.yaml"
    "step4b.yaml"
    "step3a2-coadds.yaml"
    "step4a.yaml"
    "step3b.yaml"
    "step4c.yaml"
)

declare -A WAVE_LABEL=(
    ["step1a-step1b.yaml"]="Wave1"
    ["step1c_step1d.yaml"]="Wave2"
    ["step2c.yaml"]="Wave2"
    ["step2a.yaml"]="Wave3"
    ["step2d.yaml"]="Wave4"
    ["step3a1-warps.yaml"]="Wave5"
    ["step2e-step2f.yaml"]="Wave5"
    ["step4b.yaml"]="Wave5"
    ["step3a2-coadds.yaml"]="Wave6"
    ["step4a.yaml"]="Wave7"
    ["step3b.yaml"]="Wave7-Global"
    ["step4c.yaml"]="Wave8"
)

declare -A DEPS=(
    ["step1a-step1b.yaml"]=""
    ["step1c_step1d.yaml"]="step1a-step1b.yaml"
    ["step2c.yaml"]="step1a-step1b.yaml"
    ["step2a.yaml"]="step1a-step1b.yaml,step1c_step1d.yaml"
    ["step2d.yaml"]="step1a-step1b.yaml,step1c_step1d.yaml,step2a.yaml,step2c.yaml"
    ["step3a1-warps.yaml"]="step1a-step1b.yaml,step2d.yaml"
    ["step2e-step2f.yaml"]="step1c_step1d.yaml,step2d.yaml"
    ["step4b.yaml"]="step1c_step1d.yaml,step2d.yaml"
    ["step3a2-coadds.yaml"]="step3a1-warps.yaml"
    ["step3b.yaml"]="step3a2-coadds.yaml"
    ["step4a.yaml"]="step2e-step2f.yaml,step3a2-coadds.yaml"
    ["step4c.yaml"]="step4a.yaml,step4b.yaml"
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

for s in "${!FORCE_RETRY_MAP[@]}"; do
    IS_RETRY["$s"]=1
    enable_retry_qgraph_options "$s"
done
for s in "${!SKIP_MAP[@]}"; do STATUS["$s"]="SKIPPED"; done
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
    if [ "$DRY_RUN" = "true" ]; then echo "SUCCEEDED"; return; fi
    local out
    out=$(bps status --id "$target_dir" 2>&1)
    local st
    st=$(echo "$out" | awk '/status:/ {s=$NF} END {print s}')
    [ -z "$st" ] && st=$(echo "$out" | awk 'NF {s=$NF} END {print s}')
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
        [ "${STATUS[$d]}" != "SUCCEEDED" ] && return 1
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
log "Starting Zero-Downtime DRP Campaign Orchestrator (baseline 8-wave)"
log "=================================================================="

while true; do
    state_changed=false
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
                    log "[${WAVE_LABEL[$s]}] ERROR: bps submit failed for $s"
                fi
                state_changed=true
            fi
        fi
    done

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
                    log "[${WAVE_LABEL[$s]}] WARNING: $s reported $st. Re-queueing in retry mode (${RETRIES_LEFT[$s]} retries left)..."
                    STATUS["$s"]="PENDING"
                else
                    STATUS["$s"]="FAILED"
                    log "[${WAVE_LABEL[$s]}] FAILED ($st): $s ($sdir). YAML updated to retry mode for next run."
                fi
                state_changed=true
            fi
        fi
    done

    for s in "${STEPS[@]}"; do
        if [ "${STATUS[$s]}" = "PENDING" ]; then
            if deps_blocked "$s"; then
                STATUS["$s"]="BLOCKED"
                log "[${WAVE_LABEL[$s]}] BLOCKED: $s"
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

    if [ "$n_run" -eq 0 ] && [ "$n_sub" -eq 0 ] && [ "$n_pend" -eq 0 ]; then
        break
    fi

    if [ "$state_changed" = "true" ] && [ "$active_submits" -gt 0 ]; then
        sleep 2
    else
        sleep "$POLL_INTERVAL"
    fi
done

[ "$n_fail" -eq 0 ] && [ "$n_blk" -eq 0 ] && exit 0 || exit 1
