#!/usr/bin/env bash
# docker_deploy.sh — Build the dstore image (if needed) and start the dev container
# with the source tree bind-mounted at /opt/project/dstore.
#
# Usage:
#   bash docker_deploy.sh          # start container (builds image if missing)
#   bash docker_deploy.sh --build  # force rebuild image before starting
#   bash docker_deploy.sh --shell  # start container and exec into it
#   bash docker_deploy.sh --stop   # stop and remove the container
#   bash docker_deploy.sh --help   # show this help

set -euo pipefail

# --------------------------------------------------------------------------
# Config — override with env vars if needed
# --------------------------------------------------------------------------
IMAGE_NAME="${DSTORE_IMAGE:-dstore:latest}"
CONTAINER_NAME="${DSTORE_CONTAINER:-dstore-dev}"
# Absolute path to the repo root (directory containing this script)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HOST_SRC="${SCRIPT_DIR}"
# Mount point inside the container (must match Dockerfile / buildenv)
CONTAINER_SRC="/opt/project/dstore"

# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------
log()  { echo "[docker_deploy] $*"; }
die()  { echo "[docker_deploy] ERROR: $*" >&2; exit 1; }

image_exists()     { docker image inspect "${IMAGE_NAME}" &>/dev/null; }
container_exists() { docker container inspect "${CONTAINER_NAME}" &>/dev/null; }
container_running(){ [ "$(docker container inspect -f '{{.State.Running}}' "${CONTAINER_NAME}" 2>/dev/null)" = "true" ]; }

build_image() {
    log "Building Docker image '${IMAGE_NAME}' …"
    log "This may take 20-40 minutes the first time (compiles GCC 7.3 from source)."
    docker build -t "${IMAGE_NAME}" "${SCRIPT_DIR}"
    log "Image built successfully."
}

start_container() {
    if container_running; then
        log "Container '${CONTAINER_NAME}' is already running."
        return
    fi

    if container_exists; then
        log "Starting existing container '${CONTAINER_NAME}' …"
        docker start "${CONTAINER_NAME}"
    else
        log "Creating and starting container '${CONTAINER_NAME}' …"
        docker run -d \
            --name "${CONTAINER_NAME}" \
            --hostname dstore-dev \
            -v "${HOST_SRC}:${CONTAINER_SRC}:cached" \
            -e BUILD_ROOT="${CONTAINER_SRC}" \
            -e LOCAL_LIB_PATH="/opt/project/local_libs" \
            -e GCC_VERNAME="gcc7.3" \
            -e GCC_VERSION="7.3.0" \
            -e GCCFOLDER="/opt/project/local_libs/buildtools/gcc7.3" \
            -e CC="/opt/project/local_libs/buildtools/gcc7.3/gcc/bin/gcc" \
            -e CXX="/opt/project/local_libs/buildtools/gcc7.3/gcc/bin/g++" \
            -e PATH="/opt/project/local_libs/buildtools/gcc7.3/gcc/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin" \
            -e LD_LIBRARY_PATH="/opt/project/local_libs/buildtools/gcc7.3/gcc/lib64:/opt/project/local_libs/buildtools/gcc7.3/isl/lib:/opt/project/local_libs/buildtools/gcc7.3/mpc/lib:/opt/project/local_libs/buildtools/gcc7.3/mpfr/lib:/opt/project/local_libs/buildtools/gcc7.3/gmp/lib:/opt/project/local_libs/lz4/lib:/opt/project/local_libs/cjson/lib:/opt/project/local_libs/secure/lib" \
            "${IMAGE_NAME}"
    fi

    log "Container '${CONTAINER_NAME}' is running."
    log ""
    log "  Enter shell : docker exec -it ${CONTAINER_NAME} bash"
    log "  Full build  : docker exec -it ${CONTAINER_NAME} bash -c 'source \${BUILD_ROOT}/buildenv && cd \${BUILD_ROOT}/utils && bash build.sh -m debug && cd \${BUILD_ROOT} && mkdir -p tmp_build && cd tmp_build && cmake .. -DCMAKE_BUILD_TYPE=debug -DUTILS_PATH=../utils/output -DENABLE_UT=ON && make -sj$(($(nproc)-2)) install'"
    log "  Quick make  : docker exec -it ${CONTAINER_NAME} bash -c 'cd \${BUILD_ROOT}/tmp_build && make install -j\$(nproc)'"
    log "  Alias shortcuts (inside container): dstore-build  |  dstore-rebuild"
}

stop_container() {
    if container_running; then
        log "Stopping container '${CONTAINER_NAME}' …"
        docker stop "${CONTAINER_NAME}"
    fi
    if container_exists; then
        log "Removing container '${CONTAINER_NAME}' …"
        docker rm "${CONTAINER_NAME}"
    fi
    log "Done."
}

exec_shell() {
    if ! container_running; then
        die "Container '${CONTAINER_NAME}' is not running. Start it first without --shell."
    fi
    exec docker exec -it "${CONTAINER_NAME}" bash
}

# --------------------------------------------------------------------------
# Argument parsing
# --------------------------------------------------------------------------
FORCE_BUILD=false
OPEN_SHELL=false
STOP=false

for arg in "$@"; do
    case "${arg}" in
        --build)  FORCE_BUILD=true ;;
        --shell)  OPEN_SHELL=true  ;;
        --stop)   STOP=true        ;;
        --help|-h)
            sed -n '2,10p' "$0" | sed 's/^# \?//'
            exit 0 ;;
        *) die "Unknown argument: ${arg}" ;;
    esac
done

# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------
command -v docker &>/dev/null || die "'docker' not found — please install Docker."

if "${STOP}"; then
    stop_container
    exit 0
fi

if "${FORCE_BUILD}" || ! image_exists; then
    build_image
fi

start_container

if "${OPEN_SHELL}"; then
    exec_shell
fi
