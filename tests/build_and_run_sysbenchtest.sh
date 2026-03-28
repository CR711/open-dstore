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

current_file_path="$(cd "$(dirname "$0")" && pwd)"
buidcache_dir="${current_file_path}/../tmp_build"
sysbench_bin_dir="${buidcache_dir}/bin"
cpus_num=$(grep -w processor /proc/cpuinfo | wc -l)

usage()
{
    echo "Usage:"
    echo " $(basename "$0") -t|--local_lib <path> -u|--utils_path <path>"
    echo "Options:"
    echo " -t, --local_lib <path>         The local_lib path."
    echo " -u, --utils_path <path>        The utils output path."
    echo " -h, --help                     Get help info."
    echo " -a, --asan_mode <ON|OFF>       Open asan (default: OFF)."
    echo " -r, --rebuild <true|false>     Always rebuild before run (default: true)."
    echo ""
    echo "Note: This script MUST be run inside the 'dstore_env' Docker container."
    echo "Example command to run from host:"
    echo "  docker exec dstore_env bash -c \"source /etc/bash.bashrc && source /opt/project/dstore/buildenv && cd /opt/project/dstore/tests && bash build_and_run_sysbenchtest.sh -t /opt/project/local_libs -u /opt/project/dstore/utils/output\""
}

check_param()
{
    if [ -z "${local_lib}" ]; then
        echo "Local lib path empty!"
        exit 1
    fi

    if [ -z "${utils_path}" ]; then
        echo "The utils path empty!"
        exit 1
    fi
}

build_sysbenchtest()
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

    # Increase buffer size for benchmark workloads
    sed -i 's/"buffer": 300000/"buffer": 655360/g' guc.json
}

run_sysbenchtest()
{
    export LD_LIBRARY_PATH=${utils_path}/lib:$LD_LIBRARY_PATH
    export LD_LIBRARY_PATH=${local_lib}/lib:$LD_LIBRARY_PATH

    echo "$LD_LIBRARY_PATH" | tr ':' '\n'

    if [ ! -d "${sysbench_bin_dir}" ]; then
        echo "Error: Directory ${sysbench_bin_dir} does not exist"
        exit 1
    fi

    local sysbench_bin="${sysbench_bin_dir}/sysbenchtest"
    if [ ! -f "${sysbench_bin}" ]; then
        echo "Error: Binary ${sysbench_bin} does not exist"
        exit 1
    fi

    # Clear previous data directory
    rm -rf sysbenchdir
    sleep 0.1s

    echo "run dstore sysbench simulation test."
    "${sysbench_bin}"
}

main()
{
    local_lib=""
    utils_path=""
    asan_mode="OFF"
    rebuild="true"

    getopt_cmd=$(getopt -o t:u:a:r:h -l local_lib:,utils_path:,asan_mode:,rebuild:,help \
                        -n "$(basename "$0")" -- "$@")
    eval set -- "$getopt_cmd"

    while [ -n "${1}" ]; do
        case "${1}" in
        -t|--local_lib)
            local_lib="${2}"
            echo "build_run_sysbenchtest:local_lib:${local_lib}"
            shift 2
        ;;
        -u|--utils_path)
            utils_path="${2}"
            echo "build_run_sysbenchtest:utils_path:${utils_path}"
            shift 2
        ;;
        -a|--asan_mode)
            asan_mode="${2}"
            echo "build_run_sysbenchtest:asan_mode:${asan_mode}"
            shift 2
        ;;
        -r|--rebuild)
            rebuild="${2}"
            echo "build_run_sysbenchtest:rebuild:${rebuild}"
            shift 2
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
        build_sysbenchtest
    fi

    run_sysbenchtest
    result=$?
    return ${result}
}

main "$@"
