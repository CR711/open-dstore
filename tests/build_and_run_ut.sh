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
dstore_ut_bin_dir="${buidcache_dir}/bin"
cpus_num=$(grep -w processor /proc/cpuinfo|wc -l)

function usage()
{
    echo "Usage:"
    echo " $(basename "$0") -t|--third_lib <path> -u|utils_path <path>"
    echo "Options:"
    echo " -t, --third_lib <path>         The third lib path."
    echo " -u, --utils_path <path>        The utils output path."
    echo " -h, --help                     Get help info."
    echo " -a, --asan_mode                Open asan: ON(default), OFF."
    echo " -r, --rebuild                  Always rebuild before run ut, default is true."
    echo " -g, --gtest_filter             Test case filter, e.g. -g UTBtree*.*,UTWal*.*"
}

function check_param()
{
    if [ -z "${third_lib}" ]; then
        echo "Third lib path empty!"
        exit 1
    fi

    if [ -z "${utils_path}" ]; then
        echo "The utils path empty!"
        exit 1
    fi
}

function build_ut()
{
    if [ -d "${buidcache_dir}" ]; then
        rm -rf "${buidcache_dir}"
    fi
    mkdir -p "${buidcache_dir}"
    cd "${buidcache_dir}" || exit
    if [ ${asan_mode} == "ON" ]; then
        cmake .. -DCMAKE_BUILD_TYPE=memcheck       \
                 -DLOCAL_LIB_PATH="${third_lib}"   \
                 -DUTILS_PATH="${utils_path}"      \
                 -DENABLE_UT=ON
    else
        cmake .. -DLOCAL_LIB_PATH="${third_lib}"   \
                 -DUTILS_PATH="${utils_path}"      \
                 -DENABLE_UT=ON
    fi
    make -j${cpus_num} install
}

function run_ut()
{
    cd "${dstore_ut_bin_dir}" || exit 1
    if [ $# -eq 0 ]; then
        echo "run all dstore ut."
         "${dstore_ut_bin}" 
    else
        echo "run some dstore ut: $1"
        "${dstore_ut_bin}" --gtest_filter="$1"
    fi
}

function main()
{
    dstore_ut_bin="${dstore_ut_bin_dir}/unittest"
    log_path=""
    root_db_type="dcc"
    env_type="debug"
    third_lib=""
    gausshome=""
    dstorehome=""
    asan_mode="ASAN"
    rebuild="true"
    gtest_filter=""
    getopt_cmd=$(getopt -o t:u:a:r:g:hl: -l third_lib:,utils_path:,asan_mode:,rebuild:,gtest_filter:,help -n "$(basename "$0")" -- "$@")
    eval set -- "$getopt_cmd"
    while [ -n "${1}" ]
    do
        case "${1}" in
        -t|--third_lib)
            third_lib="${2}"
            echo "build_run_ut:third_lib:${third_lib}"
            shift 2
        ;;
        -u|--utils_path)
            utils_path="${2}"
            echo "build_run_ut:utils_path:${utils_path}"
            shift 2
        ;;
        -a|--asan_mode)
            asan_mode="${2}"
            echo "build_run_ut:asan_mode:${asan_mode}"
            shift 2
        ;;
        -r|--rebuild)
            rebuild="${2}"
            echo "build_run_ut:rebuild:${rebuild}"
            shift 2
        ;;
        -g|--gtest_filter)
            gtest_filter="${2}"
            echo "build_run_ut:gtest_filter:${gtest_filter}"
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
    if [ "${rebuild}" == "true" ]; then
        build_ut
    fi
    run_ut ${gtest_filter}
    result=$?
    return ${result}
}

main "$@"
