#!/bin/bash

# Copyright (C) 2026 Huawei Technologies Co.,Ltd.
#
# dstore is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# dstore is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. if not, see <https://www.gnu.org/licenses/>.

# DFX 页面校验长稳验证工具 — 编译与运行脚本
#
# 用法:
#   source buildenv && cd tests
#   bash build_and_run_stress_verify.sh          # 编译+运行
#   bash build_and_run_stress_verify.sh -c       # 崩溃恢复验证
#   bash build_and_run_stress_verify.sh -r false  # 跳过编译直接运行
#   bash build_and_run_stress_verify.sh -a ON    # ASan 模式
#   bash build_and_run_stress_verify.sh -h       # 查看帮助

set -e

current_file_path="$(cd "$(dirname "$0")" && pwd)"
project_root="${current_file_path}/.."
buidcache_dir="${project_root}/tmp_build"
stress_bin_dir="${buidcache_dir}/bin"
cpus_num=$(grep -w processor /proc/cpuinfo | wc -l)

usage()
{
    echo "DFX 页面校验长稳验证工具"
    echo ""
    echo "用法: bash $(basename "$0") [选项]"
    echo ""
    echo "选项:"
    echo "  -c, --crash_test               崩溃恢复验证 (kill -9 + WAL redo + HEAVY 校验)"
    echo "  -a, --asan_mode <ON|OFF>       ASan 模式 (默认: OFF)"
    echo "  -r, --rebuild <true|false>     是否重新编译 (默认: true)"
    echo "  -t, --local_lib <path>         第三方库路径 (默认: 从 buildenv 读取)"
    echo "  -u, --utils_path <path>        utils 路径 (默认: <project>/utils/output)"
    echo "  -h, --help                     显示帮助"
    echo ""
    echo "示例:"
    echo "  bash $(basename "$0")              # 编译+运行长稳验证"
    echo "  bash $(basename "$0") -c           # 编译+运行崩溃恢复验证"
    echo "  bash $(basename "$0") -r false     # 跳过编译，直接运行"
    echo "  bash $(basename "$0") -a ON        # ASan 模式"
    echo ""
    echo "前置条件: 在 Docker 容器内执行，且已 source buildenv"
    echo "运行参数: 通过 tmp_build/bin/config.json 配置"
}

check_param()
{
    if [ -z "${local_lib}" ]; then
        echo "Error: local_lib 未设置。请先 source buildenv 或通过 -t 指定。"
        exit 1
    fi

    if [ -z "${utils_path}" ]; then
        echo "Error: utils_path 未设置。请先编译 utils 或通过 -u 指定。"
        exit 1
    fi
}

build_stress_verify()
{
    if [ -d "${buidcache_dir}" ]; then
        rm -rf "${buidcache_dir}"
    fi
    mkdir -p "${buidcache_dir}"
    cd "${buidcache_dir}" || exit

    if [ "${asan_mode}" = "ON" ]; then
        cmake .. -DCMAKE_BUILD_TYPE=memcheck   \
                 -DLOCAL_LIB_PATH="${local_lib}"  \
                 -DUTILS_PATH="${utils_path}"     \
                 -DDSTORE_TEST_TOOL=ON
    else
        cmake .. -DCMAKE_BUILD_TYPE=Release       \
                 -DLOCAL_LIB_PATH="${local_lib}"  \
                 -DUTILS_PATH="${utils_path}"     \
                 -DDSTORE_TEST_TOOL=ON
    fi

    make -j${cpus_num} install
}

run_stress_verify()
{
    export LD_LIBRARY_PATH=${utils_path}/lib:$LD_LIBRARY_PATH
    export LD_LIBRARY_PATH=${local_lib}/lib:$LD_LIBRARY_PATH

    if [ ! -d "${stress_bin_dir}" ]; then
        echo "Error: ${stress_bin_dir} 不存在，请先编译"
        exit 1
    fi

    local stress_bin="${stress_bin_dir}/dstore_stress_verify"
    if [ ! -f "${stress_bin}" ]; then
        echo "Error: ${stress_bin} 不存在，请先编译"
        exit 1
    fi

    cd "${stress_bin_dir}" || exit

    if [ "${crash_test}" = "true" ]; then
        echo "run dstore crash recovery + DFX verification test."
        bash "${current_file_path}/dstore_stress_verify/run_crash_recovery_test.sh"
    else
        echo "run dstore page verification stress test."
        rm -rf stress_verify_dir
        "${stress_bin}"
    fi
}

main()
{
    # 默认从环境变量读取（source buildenv 后自动设置）
    local_lib="${LOCAL_LIB_PATH:-}"
    utils_path="${project_root}/utils/output"
    asan_mode="OFF"
    rebuild="true"
    crash_test="false"

    getopt_cmd=$(getopt -o t:u:a:r:ch -l local_lib:,utils_path:,asan_mode:,rebuild:,crash_test,help \
                        -n "$(basename "$0")" -- "$@")
    eval set -- "$getopt_cmd"

    while [ -n "${1}" ]; do
        case "${1}" in
        -t|--local_lib)
            local_lib="${2}"
            shift 2
        ;;
        -u|--utils_path)
            utils_path="${2}"
            shift 2
        ;;
        -a|--asan_mode)
            asan_mode="${2}"
            shift 2
        ;;
        -r|--rebuild)
            rebuild="${2}"
            shift 2
        ;;
        -c|--crash_test)
            crash_test="true"
            shift
        ;;
        -h|--help)
            usage
            shift
            exit 0
        ;;
        --)
            shift
            break
        ;;
        *)
            echo "Error: ${1}"
            usage
            exit 1
        esac
    done

    check_param

    if [ "${rebuild}" = "true" ]; then
        build_stress_verify
    fi

    run_stress_verify
    result=$?
    return ${result}
}

main "$@"
