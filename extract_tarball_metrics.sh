#!/usr/bin/env bash
# ==============================================================================
# extract_tarball_metrics.sh
#
# Extracts 5 metrics from all 25 v6 summary tarballs:
#   1. GC time (ms)          — from performance_summary.txt
#   2. RAM ramp-up speed (s) — seconds until full YARN allocation reached
#   3. Active resource duration (s) — seconds YARN containers allocated > 0
#   4. Peak system RAM vs YARN RAM gap — from yarn_summary.csv
#   5. CPU efficiency (%)    — from /usr/bin/time -v block in each job log
#
# Output files (all in same directory as this script):
#   metrics_gc_time.csv
#   metrics_rampup.csv
#   metrics_active_duration.csv
#   metrics_ram_gap.csv
#   metrics_cpu_efficiency.csv
#
# Usage:
#   chmod +x extract_tarball_metrics.sh
#   ./extract_tarball_metrics.sh
# ==============================================================================

ARCHIVE_DIR="/home/hadoop/Cloud/results/analysis/summary_light_archives"
WORK_DIR="/tmp/tarball_extract_v6"
OUT_DIR="$ARCHIVE_DIR"

rm -rf "$WORK_DIR" && mkdir -p "$WORK_DIR"

# Output CSV headers
echo "dataset,method,param,gc_time_ms"                                          > "$OUT_DIR/metrics_gc_time.csv"
echo "dataset,method,param,rampup_seconds,peak_yarn_mb"                         > "$OUT_DIR/metrics_rampup.csv"
echo "dataset,method,param,total_job_seconds,active_yarn_seconds,active_pct"    > "$OUT_DIR/metrics_active_duration.csv"
echo "dataset,method,param,peak_yarn_gb,peak_system_gb,gap_gb,gap_pct"          > "$OUT_DIR/metrics_ram_gap.csv"
echo "dataset,method,param,cpu_pct"                                             > "$OUT_DIR/metrics_cpu_efficiency.csv"

echo "Processing tarballs..."
echo

for tarball in "$ARCHIVE_DIR"/final_exp_*_v6_summary_light.tar.gz; do
    # Extract dataset name from tarball filename
    basename_t=$(basename "$tarball")
    # e.g. final_exp_ds-12-1p1gb-2495files-gutenberg_v6_summary_light.tar.gz
    # -> ds-12-1p1gb-2495files-gutenberg
    DATASET=$(echo "$basename_t" | sed 's/final_exp_//' | sed 's/_v6_summary_light\.tar\.gz//')

    echo "=== $DATASET ==="

    EXTRACT_DIR="$WORK_DIR/$DATASET"
    mkdir -p "$EXTRACT_DIR"
    tar -xzf "$tarball" -C "$EXTRACT_DIR" 2>/dev/null

    # Find the summary dir inside (it may be nested one level)
    SUMMARY_DIR=$(find "$EXTRACT_DIR" -maxdepth 2 -name "yarn_summary.csv" | head -1 | xargs dirname)
    if [ -z "$SUMMARY_DIR" ]; then
        echo "  WARNING: could not find summary dir, skipping"
        continue
    fi

    LOG_DIR="$SUMMARY_DIR/logs"
    MONITOR_DIR="$SUMMARY_DIR/yarn_monitor"

    # ------------------------------------------------------------------
    # METRIC 1: GC time (ms)
    # Source: performance_summary.txt — line "GC time elapsed (ms)=XXXX"
    # For Hadoop jobs this is the MR counter from the job tracker.
    # For Spark jobs it appears in executor logs if present.
    # ------------------------------------------------------------------
    PERF="$SUMMARY_DIR/performance_summary.txt"
    if [ -f "$PERF" ]; then
        current_log=""
        while IFS= read -r line; do
            # Detect which log file we're in
            if echo "$line" | grep -q "^--- "; then
                current_log=$(echo "$line" | sed 's/^--- //' | sed 's/ ---$//' | sed "s/${DATASET}_//")
            fi
            # Extract GC line
            if echo "$line" | grep -qE "GC time elapsed \(ms\)="; then
                gc_ms=$(echo "$line" | grep -oP '\d+$')
                # Parse method and param from log filename
                # e.g. hadoop-base-r1.log -> method=hadoop-base param=r1
                logbase=$(echo "$current_log" | sed 's/\.log$//')
                # method is everything except last token after last -
                param=$(echo "$logbase" | grep -oP '[rp]\d+$')
                method=$(echo "$logbase" | sed "s/-${param}$//")
                echo "$DATASET,$method,$param,$gc_ms" >> "$OUT_DIR/metrics_gc_time.csv"
            fi
        done < "$PERF"
    fi

    # ------------------------------------------------------------------
    # METRIC 2: RAM ramp-up speed
    # METRIC 3: Active resource duration
    # Source: yarn_monitor/*.csv
    # Each CSV has timestamped rows with used_yarn_mb per node.
    # We sum across nodes per timestamp to get cluster-total YARN MB.
    # Ramp-up = first timestamp where summed yarn_mb reaches its max.
    # Active = count of timestamps where summed yarn_mb > 0 * 5s interval.
    # ------------------------------------------------------------------
    for mon_csv in "$MONITOR_DIR"/*.csv; do
        [ -f "$mon_csv" ] || continue
        job=$(basename "$mon_csv" .csv)

        # Parse method/param from job name (e.g. java-spark-p8, hadoop-base-r1, sequential)
        if echo "$job" | grep -qP '[rp]\d+$'; then
            param=$(echo "$job" | grep -oP '[rp]\d+$')
            method=$(echo "$job" | sed "s/-${param}$//")
        else
            param="local"
            method="sequential-python"
        fi

        python3 << PYEOF
import csv, sys

job = "$job"
dataset = "$DATASET"
method = "$method"
param = "$param"
mon_csv = "$mon_csv"
out_rampup = "$OUT_DIR/metrics_rampup.csv"
out_active = "$OUT_DIR/metrics_active_duration.csv"

rows = []
try:
    with open(mon_csv) as f:
        reader = csv.DictReader(f)
        for r in reader:
            try:
                yarn_mb = int(r.get('used_yarn_mb', 0) or 0)
            except:
                yarn_mb = 0
            rows.append(yarn_mb)
except:
    sys.exit(0)

if not rows:
    sys.exit(0)

# Sum is already per-node in our monitor (one row per node per tick)
# Group by tick index (every 3 rows = 3 nodes = one 5s tick)
# Actually each row is one node, so group by position // num_nodes
# Simpler: re-read and group by timestamp
tick_sums = {}
try:
    with open(mon_csv) as f:
        reader = csv.DictReader(f)
        for i, r in enumerate(reader):
            ts = r.get('timestamp','')
            try:
                yarn_mb = int(r.get('used_yarn_mb', 0) or 0)
            except:
                yarn_mb = 0
            tick_sums[ts] = tick_sums.get(ts, 0) + yarn_mb
except:
    sys.exit(0)

ticks = list(tick_sums.values())
if not ticks:
    sys.exit(0)

peak_yarn = max(ticks)

# Ramp-up: seconds until first tick where value == peak
rampup_s = 0
for i, v in enumerate(ticks):
    if v >= peak_yarn:
        rampup_s = i * 5
        break

# Active duration: ticks where yarn > 0
active_ticks = sum(1 for v in ticks if v > 0)
total_ticks = len(ticks)
active_s = active_ticks * 5
total_s = total_ticks * 5
active_pct = round(active_s / total_s * 100, 1) if total_s > 0 else 0

with open(out_rampup, 'a') as f:
    f.write(f"{dataset},{method},{param},{rampup_s},{peak_yarn}\n")

with open(out_active, 'a') as f:
    f.write(f"{dataset},{method},{param},{total_s},{active_s},{active_pct}\n")
PYEOF

    done

    # ------------------------------------------------------------------
    # METRIC 4: Peak system RAM vs YARN RAM gap
    # Source: yarn_summary.csv inside each tarball
    # Columns: job, max_yarn_allocated_gb, system_used_max_gb
    # ------------------------------------------------------------------
    YARN_SUM="$SUMMARY_DIR/yarn_summary.csv"
    if [ -f "$YARN_SUM" ]; then
        tail -n +2 "$YARN_SUM" | while IFS=',' read -r job yarn_gb sys_gb; do
            [ -z "$job" ] && continue
            if echo "$job" | grep -qP '[rp]\d+$'; then
                param=$(echo "$job" | grep -oP '[rp]\d+$')
                method=$(echo "$job" | sed "s/-${param}$//")
            else
                param="local"
                method="sequential-python"
            fi
            gap=$(python3 -c "print(round(${sys_gb:-0} - ${yarn_gb:-0}, 4))")
            gap_pct=$(python3 -c "print(round((${sys_gb:-0} - ${yarn_gb:-0}) / max(${sys_gb:-0.001},0.001) * 100, 1))")
            echo "$DATASET,$method,$param,$yarn_gb,$sys_gb,$gap,$gap_pct" \
                >> "$OUT_DIR/metrics_ram_gap.csv"
        done
    fi

    # ------------------------------------------------------------------
    # METRIC 5: CPU efficiency (%)
    # Source: individual log files — /usr/bin/time -v block
    # Line: "Percent of CPU this job got: XX%"
    # ------------------------------------------------------------------
    for log in "$LOG_DIR"/*.log; do
        [ -f "$log" ] || continue
        logbase=$(basename "$log" .log | sed "s/^${DATASET}_//")

        if echo "$logbase" | grep -qP '[rp]\d+$'; then
            param=$(echo "$logbase" | grep -oP '[rp]\d+$')
            method=$(echo "$logbase" | sed "s/-${param}$//")
        else
            param="local"
            method="sequential-python"
        fi

        cpu_pct=$(grep "Percent of CPU" "$log" | tail -1 | grep -oP '\d+')
        if [ -n "$cpu_pct" ]; then
            echo "$DATASET,$method,$param,$cpu_pct" \
                >> "$OUT_DIR/metrics_cpu_efficiency.csv"
        fi
    done

    # Clean up extracted dir to save space
    rm -rf "$EXTRACT_DIR"
    echo "  done"
done

echo
echo "============================================================"
echo "DONE. Output files:"
for f in metrics_gc_time metrics_rampup metrics_active_duration metrics_ram_gap metrics_cpu_efficiency; do
    fpath="$OUT_DIR/${f}.csv"
    lines=$(wc -l < "$fpath")
    echo "  $fpath  ($lines rows)"
done
echo "============================================================"
