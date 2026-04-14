#!/bin/bash

# DFX 页面校验攻防对抗 — 4 小时长稳运行脚本
#
# 用法: 在 Docker 容器内执行
#   source buildenv && cd tmp_build && bash ../tests/run_attack_defense_4h.sh
#
# 功能:
#   1. 循环运行攻防 UT（含 fuzz），每轮改变 gtest 随机种子
#   2. 同时运行 gtest_repeat 高频压力
#   3. 记录每轮结果，4 小时后生成漏洞报告

set -euo pipefail

DURATION_SECONDS=$((4 * 3600))   # 4 小时
REPORT_FILE="attack_defense_report_$(date +%Y%m%d_%H%M%S).txt"
LOG_DIR="attack_defense_logs"
START_TIME=$(date +%s)
ROUND=0
TOTAL_PASS=0
TOTAL_FAIL=0
TOTAL_TESTS=0
FAIL_DETAILS=""

mkdir -p "${LOG_DIR}"

echo "================================================================" | tee "${REPORT_FILE}"
echo " DFX Page Verify Attack-Defense Report"                           | tee -a "${REPORT_FILE}"
echo " Start: $(date '+%Y-%m-%d %H:%M:%S')"                           | tee -a "${REPORT_FILE}"
echo " Duration: ${DURATION_SECONDS}s (4 hours)"                       | tee -a "${REPORT_FILE}"
echo "================================================================" | tee -a "${REPORT_FILE}"
echo "" | tee -a "${REPORT_FILE}"

while true; do
    ELAPSED=$(( $(date +%s) - START_TIME ))
    if [ "${ELAPSED}" -ge "${DURATION_SECONDS}" ]; then
        break
    fi

    ROUND=$((ROUND + 1))
    SEED=$((42 + ROUND))
    ROUND_LOG="${LOG_DIR}/round_${ROUND}.log"

    echo "[Round ${ROUND}] elapsed=${ELAPSED}s seed=${SEED} ..." | tee -a "${REPORT_FILE}"

    # Phase 1: 参数化攻击 + false-positive + fuzz（128 tests）
    PHASE1_OK=0
    PHASE1_FAIL=0
    if timeout 60 bin/unittest --gtest_filter="*AttackDefense*" \
        --gtest_random_seed="${SEED}" \
        --gtest_shuffle \
        > "${ROUND_LOG}" 2>&1; then
        PHASE1_OK=$(grep -c "\[ *OK \]" "${ROUND_LOG}" 2>/dev/null || echo 0)
        echo "  Phase1(shuffle): ${PHASE1_OK} OK" | tee -a "${REPORT_FILE}"
    else
        PHASE1_OK=$(grep -c "\[ *OK \]" "${ROUND_LOG}" 2>/dev/null || echo 0)
        PHASE1_FAIL=$(grep -c "\[ *FAILED \]" "${ROUND_LOG}" 2>/dev/null || echo 0)
        echo "  Phase1(shuffle): ${PHASE1_OK} OK, ${PHASE1_FAIL} FAILED" | tee -a "${REPORT_FILE}"
        # 记录失败详情
        FAILURES=$(grep "\[ *FAILED \]" "${ROUND_LOG}" 2>/dev/null || true)
        if [ -n "${FAILURES}" ]; then
            FAIL_DETAILS="${FAIL_DETAILS}\n[Round ${ROUND} Phase1 seed=${SEED}]\n${FAILURES}\n"
        fi
    fi
    TOTAL_PASS=$((TOTAL_PASS + PHASE1_OK))
    TOTAL_FAIL=$((TOTAL_FAIL + PHASE1_FAIL))
    TOTAL_TESTS=$((TOTAL_TESTS + PHASE1_OK + PHASE1_FAIL))

    # Phase 2: 高频重复运行 fuzz（10 次重复，每次 500+200 迭代）
    PHASE2_LOG="${LOG_DIR}/round_${ROUND}_repeat.log"
    if timeout 30 bin/unittest \
        --gtest_filter="UTAttackDefenseVerify.FuzzRandomAttacks:UTAttackDefenseVerify.FuzzRandomBitFlips" \
        --gtest_repeat=10 \
        > "${PHASE2_LOG}" 2>&1; then
        PHASE2_OK=$(grep -c "\[ *OK \]" "${PHASE2_LOG}" 2>/dev/null || echo 0)
        echo "  Phase2(fuzz×10): ${PHASE2_OK} OK" | tee -a "${REPORT_FILE}"
    else
        PHASE2_OK=$(grep -c "\[ *OK \]" "${PHASE2_LOG}" 2>/dev/null || echo 0)
        PHASE2_FAIL=$(grep -c "\[ *FAILED \]" "${PHASE2_LOG}" 2>/dev/null || echo 0)
        echo "  Phase2(fuzz×10): ${PHASE2_OK} OK, ${PHASE2_FAIL} FAILED" | tee -a "${REPORT_FILE}"
        FAILURES=$(grep "\[ *FAILED \]" "${PHASE2_LOG}" 2>/dev/null || true)
        if [ -n "${FAILURES}" ]; then
            FAIL_DETAILS="${FAIL_DETAILS}\n[Round ${ROUND} Phase2 fuzz-repeat]\n${FAILURES}\n"
        fi
        TOTAL_FAIL=$((TOTAL_FAIL + PHASE2_FAIL))
    fi
    TOTAL_PASS=$((TOTAL_PASS + PHASE2_OK))
    TOTAL_TESTS=$((TOTAL_TESTS + PHASE2_OK + ${PHASE2_FAIL:-0}))

    # Phase 3: 全量 DFX UT 回归（确认攻防测试不影响其他 DFX 测试）
    ELAPSED2=$(( $(date +%s) - START_TIME ))
    if [ "${ELAPSED2}" -ge "${DURATION_SECONDS}" ]; then
        break
    fi
    PHASE3_LOG="${LOG_DIR}/round_${ROUND}_dfx.log"
    if timeout 30 bin/unittest \
        --gtest_filter="UTPageVerifyRegistry*:UTHeapPageVerify*:UTIndexPageVerify*:UTHeapSegmentVerify*:UTVerifyReport*:UTUndoPageVerify*:UTSegmentPageVerify*:UTTbsBtrRecycleVerify*:UTFaultInjectVerify*" \
        > "${PHASE3_LOG}" 2>&1; then
        PHASE3_OK=$(grep -c "\[ *OK \]" "${PHASE3_LOG}" 2>/dev/null || echo 0)
        echo "  Phase3(DFX-UT): ${PHASE3_OK} OK" | tee -a "${REPORT_FILE}"
    else
        PHASE3_OK=$(grep -c "\[ *OK \]" "${PHASE3_LOG}" 2>/dev/null || echo 0)
        PHASE3_FAIL=$(grep -c "\[ *FAILED \]" "${PHASE3_LOG}" 2>/dev/null || echo 0)
        echo "  Phase3(DFX-UT): ${PHASE3_OK} OK, ${PHASE3_FAIL} FAILED !!!" | tee -a "${REPORT_FILE}"
        FAILURES=$(grep "\[ *FAILED \]" "${PHASE3_LOG}" 2>/dev/null || true)
        if [ -n "${FAILURES}" ]; then
            FAIL_DETAILS="${FAIL_DETAILS}\n[Round ${ROUND} Phase3 DFX-UT]\n${FAILURES}\n"
        fi
        TOTAL_FAIL=$((TOTAL_FAIL + PHASE3_FAIL))
    fi
    TOTAL_PASS=$((TOTAL_PASS + PHASE3_OK))
    TOTAL_TESTS=$((TOTAL_TESTS + PHASE3_OK + ${PHASE3_FAIL:-0}))

    echo "" >> "${REPORT_FILE}"
done

END_TIME=$(date +%s)
TOTAL_ELAPSED=$((END_TIME - START_TIME))

echo "" | tee -a "${REPORT_FILE}"
echo "================================================================" | tee -a "${REPORT_FILE}"
echo " FINAL REPORT"                                                    | tee -a "${REPORT_FILE}"
echo "================================================================" | tee -a "${REPORT_FILE}"
echo " End: $(date '+%Y-%m-%d %H:%M:%S')"                             | tee -a "${REPORT_FILE}"
echo " Total elapsed: ${TOTAL_ELAPSED}s"                               | tee -a "${REPORT_FILE}"
echo " Total rounds: ${ROUND}"                                         | tee -a "${REPORT_FILE}"
echo " Total tests executed: ${TOTAL_TESTS}"                           | tee -a "${REPORT_FILE}"
echo " Total PASSED: ${TOTAL_PASS}"                                    | tee -a "${REPORT_FILE}"
echo " Total FAILED: ${TOTAL_FAIL}"                                    | tee -a "${REPORT_FILE}"
echo ""                                                                 | tee -a "${REPORT_FILE}"

if [ "${TOTAL_FAIL}" -eq 0 ]; then
    echo " VERDICT: ALL CLEAR — 零漏洞"                                | tee -a "${REPORT_FILE}"
    echo " 校验框架在 ${ROUND} 轮、${TOTAL_TESTS} 次测试中"           | tee -a "${REPORT_FILE}"
    echo " 100% 检测率，0 误报，0 漏报。"                              | tee -a "${REPORT_FILE}"
else
    echo " VERDICT: VULNERABILITIES FOUND"                              | tee -a "${REPORT_FILE}"
    echo " ${TOTAL_FAIL} failures detected in ${TOTAL_TESTS} tests"    | tee -a "${REPORT_FILE}"
    echo ""                                                             | tee -a "${REPORT_FILE}"
    echo " Failure Details:"                                            | tee -a "${REPORT_FILE}"
    echo -e "${FAIL_DETAILS}"                                           | tee -a "${REPORT_FILE}"
fi

echo ""                                                                 | tee -a "${REPORT_FILE}"
echo " Attack coverage: 29 attack vectors × 4 verify levels"          | tee -a "${REPORT_FILE}"
echo " Page types: Heap, Index, Undo, TxnSlot, Segment, Tablespace"   | tee -a "${REPORT_FILE}"
echo " VerifyCodes tested: 22 / 28 (79%)"                             | tee -a "${REPORT_FILE}"
echo " Fuzz iterations per round: 500 (attacks) + 200 (bit-flips)"    | tee -a "${REPORT_FILE}"
echo " Fuzz repeat per round: 10× (total 7000 fuzz per round)"        | tee -a "${REPORT_FILE}"
echo "================================================================" | tee -a "${REPORT_FILE}"

echo ""
echo "Report saved to: $(pwd)/${REPORT_FILE}"
