#!/bin/bash

# Define paths relative to the script location (inside the container)
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DSTORE_ROOT="/opt/project/dstore"
TMP_BUILD="${DSTORE_ROOT}/tmp_build"
SYSBENCH_BIN="${TMP_BUILD}/bin/sysbenchtest"

# Source environment
source ${DSTORE_ROOT}/buildenv
export LOCAL_LIB_PATH=/opt/project/local_libs
export LD_LIBRARY_PATH=${DSTORE_ROOT}/utils/output/lib:${LOCAL_LIB_PATH}/lib:$LD_LIBRARY_PATH

scenarios=("read_only" "read_write" "write_only")

echo "=========================================================="
echo "      Dstore Sysbench Scenario Verification"
echo "=========================================================="

for scenario in "${scenarios[@]}"; do
    echo ""
    echo "[Scenario: ${scenario}]"
    
    # 1. Update config.json in the run directory
    cp "${SCRIPT_DIR}/config_${scenario}.json" "${TMP_BUILD}/config.json"
    
    # 2. Run test and capture output
    cd "${TMP_BUILD}"
    rm -rf sysbenchdir
    
    output=$("${SYSBENCH_BIN}" 2>&1)
    
    # 3. Parse and display results
    tps=$(echo "$output" | grep "transactions:" | awk '{print $2}')
    qps=$(echo "$output" | grep "queries:" | awk '{print $2}')
    lat=$(echo "$output" | grep "avg:" | head -n 1 | awk '{print $2}')
    
    # Calculate QPS/TPS ratio (if tps is not zero)
    if [ ! -z "$tps" ] && [ "$tps" != "0" ]; then
        ratio=$(echo "scale=2; $qps / $tps" | bc)
    else
        ratio="N/A"
    fi
    
    echo "  TPS:   ${tps:-Error}"
    echo "  QPS:   ${qps:-Error}"
    echo "  Ratio: ${ratio} (Expected: RO ~12.0, RW ~20.0, WO ~5.0)"
    echo "  Avg Latency: ${lat:-N/A} ms"
done

echo ""
echo "=========================================================="
echo "Verification Complete."
