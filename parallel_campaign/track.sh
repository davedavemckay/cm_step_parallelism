#!/usr/bin/env bash
# Upgraded BPS dependency tracker:
# - Backward-compatible with: ./track.sh <submit_dir> <next_yaml> [headless] [resubmit_retries]
# - Also supports comma-separated multiple upstream submit_dirs (waits for ALL to SUCCEED)
#   and comma-separated multiple next_yamls (submits ALL once upstream dirs succeed):
#     ./track.sh <dir1>[,<dir2>,...] <yaml1>[,<yaml2>,...] [headless] [resubmit_retries]
# - Retry logic: when resubmit_retries > 0 and an upstream job fails, comments out the first
#   extraQgraphOptions line with "# ", uncomments the second line (--skip-existing-in ... --clobber-outputs),
#   and resubmits the YAML via `bps submit`.

set -u

submit_dirs_arg="${1:-}"
next_yamls_arg="${2:-}"
headless="${3:-false}"
resubmit_retries="${4:-0}"
poll_interval="${POLL_INTERVAL:-30}"

usage() {
    echo "Usage: $0 <submit_dir[,submit_dir2,...]> <next_yaml[,next_yaml2,...]> [headless] [resubmit_retries]"
    echo ""
    echo "Arguments:"
    echo "  <submit_dir>        Run directory (or comma-separated list of directories) of running BPS job(s)."
    echo "                      Can also be specified as <yaml>=<submit_dir> for explicit retry YAML mapping."
    echo "  <next_yaml>         BPS YAML (or comma-separated list of YAMLs) to submit once all <submit_dir>s SUCCEED."
    echo "  [headless]          Optional boolean ('true' or 'false', default: false). In headless mode,"
    echo "                      only the newly submitted run directory(ies) are printed to stdout."
    echo "  [resubmit_retries]  Optional integer (default: 0). Number of times to resubmit a tracked job"
    echo "                      (after switching its extraQgraphOptions to --skip-existing-in ... --clobber-outputs)"
    echo "                      if it fails due to transient remote site infrastructure issues."
    echo ""
    echo "Examples:"
    echo "  # Wait for 1 job, then submit 3 parallel Wave 2 jobs:"
    echo "  $0 /path/to/submit/step1a_1b step1c1-isolated-stars.yaml,step1d1-visit-tables.yaml,step2c.yaml"
    echo ""
    echo "  # Wait for BOTH step2a and step2c to finish, then submit step2d:"
    echo "  $0 /path/to/submit/step2a,/path/to/submit/step2c step2d.yaml"
}

log_msg() {
    if [ "$headless" != "true" ]; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
    fi
}

extract_submit_dir() {
    awk '/Submitting from directory:/ {dir=$NF} /^Submit dir:/ {dir=$NF} END {print dir}'
}

get_bps_status() {
    local target_dir="$1"
    local out
    out=$(bps status --id "$target_dir" 2>&1)
    local st
    st=$(echo "$out" | awk '/status:/ {s=$NF} END {print s}')
    if [ -z "$st" ]; then
        st=$(echo "$out" | awk 'NF {s=$NF} END {print s}')
    fi
    echo "${st:-UNKNOWN}"
}

# Comment out the first-try extraQgraphOptions line ("# extraQgraphOptions: ...")
# and uncomment the second retry line ("extraQgraphOptions: ... --skip-existing-in ... --clobber-outputs").
enable_retry_qgraph_options() {
    local yaml="$1"
    [ ! -f "$yaml" ] && return 1
    awk '
    {
        lines[NR] = $0
    }
    END {
        done_retry = 0
        for (i = 1; i <= NR; i++) {
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
}

resolve_upstream_yaml() {
    local dir="$1"
    local explicit_yaml="$2"
    if [ -n "$explicit_yaml" ] && [ -f "$explicit_yaml" ]; then
        echo "$explicit_yaml"
        return 0
    fi
    # Check if BPS copied the submitted step*.yaml into the submit directory
    for f in "$dir"/step*.yaml; do
        if [ -f "$f" ] && [[ "$f" != *_config.yaml ]]; then
            local base
            base="$(basename "$f")"
            if [ -f "$base" ]; then
                echo "$base"
                return 0
            fi
        fi
    done
    return 1
}

if [[ "$submit_dirs_arg" != */* ]] || [[ "$next_yamls_arg" != *yaml* ]]; then
    usage
    exit 1
fi

IFS=',' read -r -a RAW_SUBMIT_ITEMS <<< "$submit_dirs_arg"
IFS=',' read -r -a NEXT_YAMLS <<< "$next_yamls_arg"

declare -a SUBMIT_DIRS
declare -a UPSTREAM_YAMLS
declare -a RETRIES_LEFT

for i in "${!RAW_SUBMIT_ITEMS[@]}"; do
    item="${RAW_SUBMIT_ITEMS[$i]}"
    if [[ "$item" == *=* ]]; then
        UPSTREAM_YAMLS[$i]="${item%%=*}"
        SUBMIT_DIRS[$i]="${item#*=}"
    else
        UPSTREAM_YAMLS[$i]=""
        SUBMIT_DIRS[$i]="$item"
    fi
    RETRIES_LEFT[$i]="$resubmit_retries"
done

for y in "${NEXT_YAMLS[@]}"; do
    if [ ! -f "$y" ]; then
        echo "ERROR: Next YAML file not found: $y" >&2
        exit 1
    fi
done

log_msg "Tracking ${#SUBMIT_DIRS[@]} upstream job(s): ${SUBMIT_DIRS[*]}"
log_msg "Will submit ${#NEXT_YAMLS[@]} downstream YAML(s) upon completion: ${NEXT_YAMLS[*]}"

while true; do
    all_succeeded=true
    status_summary=()

    for i in "${!SUBMIT_DIRS[@]}"; do
        dir="${SUBMIT_DIRS[$i]}"
        status=$(get_bps_status "$dir")
        status_summary+=("$(basename "$dir"):$status")

        if [ "$status" = "SUCCEEDED" ]; then
            continue
        elif [ "$status" = "FAILED" ]; then
            if [ "${RETRIES_LEFT[$i]}" -gt 0 ]; then
                RETRIES_LEFT[$i]=$((RETRIES_LEFT[$i] - 1))
                uyaml=$(resolve_upstream_yaml "$dir" "${UPSTREAM_YAMLS[$i]}")
                if [ -n "$uyaml" ] && [ -f "$uyaml" ]; then
                    log_msg "WARNING: $dir FAILED. Enabling retry extraQgraphOptions in $uyaml and resubmitting (${RETRIES_LEFT[$i]} retries remaining)..."
                    enable_retry_qgraph_options "$uyaml"
                    resub_out=$(bps submit "$uyaml" 2>&1)
                    resub_rc=$?
                    new_dir=$(echo "$resub_out" | extract_submit_dir)
                    if [ $resub_rc -ne 0 ] || [ -z "$new_dir" ]; then
                        echo "ERROR: bps submit retry failed for $uyaml: $resub_out" >&2
                        exit 1
                    fi
                    SUBMIT_DIRS[$i]="$new_dir"
                    log_msg "Resubmitted $uyaml -> $new_dir"
                else
                    echo "ERROR: Upstream job FAILED ($dir) and could not resolve upstream YAML to resubmit." >&2
                    exit 1
                fi
                all_succeeded=false
            else
                echo "ERROR: Upstream job FAILED: $dir" >&2
                exit 1
            fi
        elif [ "$status" = "UNKNOWN" ] || [ "$status" = "DELETED" ] || [ "$status" = "PRUNED" ]; then
            echo "ERROR: Upstream job entered terminal error state ($status): $dir" >&2
            exit 1
        else
            all_succeeded=false
        fi
    done

    log_msg "Status: ${status_summary[*]}"

    if [ "$all_succeeded" = "true" ]; then
        break
    fi

    sleep "$poll_interval"
done

log_msg "All upstream jobs SUCCEEDED. Submitting downstream YAML(s): ${NEXT_YAMLS[*]}"

submitted_dirs=()
for y in "${NEXT_YAMLS[@]}"; do
    log_msg "Submitting $y ..."
    sub_out=$(bps submit "$y" 2>&1)
    sub_rc=$?
    next_dir=$(echo "$sub_out" | extract_submit_dir)
    if [ $sub_rc -ne 0 ] || [ -z "$next_dir" ]; then
        echo "ERROR: Failed to submit $y (exit code $sub_rc):" >&2
        echo "$sub_out" >&2
        exit 1
    fi
    log_msg "Submitted $y -> $next_dir"
    submitted_dirs+=("$next_dir")
done

# Print comma-separated list of newly submitted directories (for headless chaining)
IFS=','
echo "${submitted_dirs[*]}"
exit 0
