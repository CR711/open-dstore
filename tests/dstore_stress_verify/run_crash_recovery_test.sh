#!/bin/bash
# Crash Recovery + DFX Page Verification Test
#
# Tests that after a crash (kill -9), the storage engine can recover
# via WAL redo and all pages pass DFX verification.
#
# Phase 1: Bootstrap + prepare data (LIGHT verify)
# Phase 2: Run write-only OLTP for 3s, then kill -9
# Phase 3: Restart -> WAL redo recovery -> HEAVY verify + 5s OLTP
#
# Usage:
#   # 在 Docker 容器内执行
#   cd /opt/project/dstore/tmp_build/bin
#   bash ../../tests/dstore_stress_verify/run_crash_recovery_test.sh
#
#   # 或从宿主机一行执行
#   docker exec dstore-dev bash -c \
#       "source /opt/project/dstore/buildenv && \
#        cd /opt/project/dstore/tmp_build/bin && \
#        bash ../../tests/dstore_stress_verify/run_crash_recovery_test.sh"
#
#   # 或通过 CMake target
#   cd /opt/project/dstore/tmp_build && make run_dstore_crash_recovery_test
#
# Exit code: 0 = PASS, 1 = FAIL

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BIN_DIR="$(pwd)"
STRESS_BIN="./dstore_stress_verify"
TMPOUT=$(mktemp /tmp/crash_recovery_XXXXXX.log)
trap 'rm -f $TMPOUT' EXIT

if [ ! -f "$STRESS_BIN" ]; then
    echo "ERROR: $STRESS_BIN not found. Run from tmp_build/bin/"
    exit 1
fi

echo "=========================================="
echo "  DFX Crash Recovery Verification Test"
echo "=========================================="

# Phase 1: Prepare (bootstrap + create tables + load data)
echo ""
echo "[Phase 1] Preparing data..."
cat > config.json << 'EOF'
{
    "tables":            1,
    "table_size":     1000,
    "threads":           4,
    "time":              5,
    "warmup_time":       0,
    "report_interval":   5,
    "range_size":       50,
    "point_selects":     2,
    "simple_ranges":     0,
    "sum_ranges":        0,
    "order_ranges":      0,
    "distinct_ranges":   0,
    "index_updates":     2,
    "non_index_updates": 2,
    "delete_inserts":    1,
    "skip_trx":          0,
    "secondary":         0,
    "auto_inc":          1,
    "mode":    "read_write",
    "command":       "prepare",
    "verify_level":   "LIGHT",
    "verify_modules": "heap,index,undo",
    "verify_on_write": true,
    "verify_on_read":  true,
    "fault_inject_enabled": false,
    "fault_inject_time":    0,
    "fault_type":           "crc_error",
    "fault_target_module":  "heap",
    "fault_target_page":    0
}
EOF

rm -rf stress_verify_dir
$STRESS_BIN > "$TMPOUT" 2>&1 || true
grep -v -E "WARNING:.*Parse|ERROR:.*could not parse|ERROR:.*Parse tenant" "$TMPOUT"
if grep -q "Result: PASS" "$TMPOUT"; then
    echo "[Phase 1] Prepare completed successfully."
elif grep -q "Result: FAIL" "$TMPOUT"; then
    echo "[Phase 1] FAILED — DFX verification error detected during prepare"
    exit 1
else
    # Check for known benign abort (StopAcceptNewConnection) — data was prepared
    if grep -q "Create.*success" "$TMPOUT" && grep -q "Loaded table" "$TMPOUT"; then
        echo "[Phase 1] Prepare completed (shutdown abort is cosmetic)."
    else
        echo "[Phase 1] FAILED — unexpected output"
        exit 1
    fi
fi

# Phase 2: Run OLTP for a few seconds, then kill -9
echo ""
echo "[Phase 2] Running OLTP workload (will crash after 3s)..."
cat > config.json << 'EOF'
{
    "tables":            1,
    "table_size":     1000,
    "threads":           4,
    "time":             30,
    "warmup_time":       0,
    "report_interval":  30,
    "range_size":       50,
    "point_selects":     2,
    "simple_ranges":     0,
    "sum_ranges":        0,
    "order_ranges":      0,
    "distinct_ranges":   0,
    "index_updates":     2,
    "non_index_updates": 2,
    "delete_inserts":    1,
    "skip_trx":          0,
    "secondary":         0,
    "auto_inc":          1,
    "mode":    "write_only",
    "command":       "run",
    "verify_level":   "LIGHT",
    "verify_modules": "heap,index,undo",
    "verify_on_write": true,
    "verify_on_read":  true,
    "fault_inject_enabled": false,
    "fault_inject_time":    0,
    "fault_type":           "crc_error",
    "fault_target_module":  "heap",
    "fault_target_page":    0
}
EOF

$STRESS_BIN > /dev/null 2>&1 &
STRESS_PID=$!
echo "  PID: $STRESS_PID"
sleep 3
echo "  Sending kill -9 to simulate crash..."
kill -9 $STRESS_PID 2>/dev/null || true
wait $STRESS_PID 2>/dev/null || true
echo "[Phase 2] Crash simulated."

# Phase 3: Restart and verify (crash recovery with HEAVY DFX)
echo ""
echo "[Phase 3] Restarting after crash (with HEAVY verification)..."
cat > config.json << 'EOF'
{
    "tables":            1,
    "table_size":     1000,
    "threads":           4,
    "time":              5,
    "warmup_time":       0,
    "report_interval":   5,
    "range_size":       50,
    "point_selects":     5,
    "simple_ranges":     0,
    "sum_ranges":        0,
    "order_ranges":      0,
    "distinct_ranges":   0,
    "index_updates":     1,
    "non_index_updates": 1,
    "delete_inserts":    0,
    "skip_trx":          0,
    "secondary":         0,
    "auto_inc":          1,
    "mode":    "read_write",
    "command":       "run",
    "verify_level":   "HEAVY",
    "verify_modules": "heap,index,undo",
    "verify_on_write": true,
    "verify_on_read":  true,
    "fault_inject_enabled": false,
    "fault_inject_time":    0,
    "fault_type":           "crc_error",
    "fault_target_module":  "heap",
    "fault_target_page":    0
}
EOF

$STRESS_BIN > "$TMPOUT" 2>&1 || true
grep -v -E "WARNING:.*Parse|ERROR:.*could not parse|ERROR:.*Parse tenant|LockTuple err" "$TMPOUT"

echo ""
echo "=========================================="
if grep -q "Result: PASS" "$TMPOUT"; then
    echo "  CRASH RECOVERY TEST: PASS"
    echo "=========================================="
    exit 0
elif grep -q "Result: FAIL" "$TMPOUT"; then
    echo "  CRASH RECOVERY TEST: FAIL (DFX verification errors)"
    echo "=========================================="
    exit 1
else
    echo "  CRASH RECOVERY TEST: INCONCLUSIVE (no result line)"
    echo "=========================================="
    exit 1
fi
