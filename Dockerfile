# Dstore build environment

FROM ubuntu:20.04

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# --------------------------------------------------------------------------
# Environment
# --------------------------------------------------------------------------
ENV USER=jon

# --------------------------------------------------------------------------
# System packages
# --------------------------------------------------------------------------
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        build-essential cmake \
        wget curl git ca-certificates \
        tar gzip bzip2 xz-utils \
        m4 flex bison texinfo \
        zlib1g-dev libssl-dev libaio-dev \
        python3 file gdb vim \
    && rm -rf /var/lib/apt/lists/*

# --------------------------------------------------------------------------
# Path layout
# --------------------------------------------------------------------------
ENV PROJECT_ROOT=/opt/project
ENV BUILD_ROOT=${PROJECT_ROOT}/dstore
ENV LOCAL_LIB_PATH=${PROJECT_ROOT}/local_libs
ENV GCC_VERNAME=gcc7.3
ENV GCC_VERSION=7.3.0
ENV GCCFOLDER=${LOCAL_LIB_PATH}/buildtools/${GCC_VERNAME}

RUN mkdir -p \
        ${PROJECT_ROOT}/dstore \
        ${GCCFOLDER} \
        ${LOCAL_LIB_PATH}/secure/include \
        ${LOCAL_LIB_PATH}/secure/lib \
        ${LOCAL_LIB_PATH}/lz4/include \
        ${LOCAL_LIB_PATH}/lz4/lib \
        ${LOCAL_LIB_PATH}/cjson/include \
        ${LOCAL_LIB_PATH}/cjson/lib \
        ${LOCAL_LIB_PATH}/gtest/include \
        ${LOCAL_LIB_PATH}/gtest/lib \
        ${LOCAL_LIB_PATH}/mockcpp/3rdparty

# --------------------------------------------------------------------------
# GCC prerequisites
# --------------------------------------------------------------------------
WORKDIR /tmp/prereqs

RUN wget -q --tries=3 --timeout=30 https://gcc.gnu.org/pub/gcc/infrastructure/gmp-6.1.0.tar.bz2 && \
    wget -q --tries=3 --timeout=30 https://gcc.gnu.org/pub/gcc/infrastructure/mpfr-3.1.4.tar.bz2 && \
    wget -q --tries=3 --timeout=30 https://gcc.gnu.org/pub/gcc/infrastructure/mpc-1.0.3.tar.gz && \
    wget -q --tries=3 --timeout=30 https://gcc.gnu.org/pub/gcc/infrastructure/isl-0.16.1.tar.bz2 && \
    tar xf gmp-6.1.0.tar.bz2 && \
    tar xf mpfr-3.1.4.tar.bz2 && \
    tar xf mpc-1.0.3.tar.gz && \
    tar xf isl-0.16.1.tar.bz2

RUN mkdir -p /tmp/build-gmp && cd /tmp/build-gmp && \
    /tmp/prereqs/gmp-6.1.0/configure \
        --prefix=${GCCFOLDER}/gmp \
        --enable-shared --disable-static --with-pic && \
    make -j$(nproc) && make install

RUN mkdir -p /tmp/build-mpfr && cd /tmp/build-mpfr && \
    LD_LIBRARY_PATH=${GCCFOLDER}/gmp/lib \
    /tmp/prereqs/mpfr-3.1.4/configure \
        --prefix=${GCCFOLDER}/mpfr \
        --with-gmp=${GCCFOLDER}/gmp \
        --enable-shared --disable-static && \
    make -j$(nproc) && make install

RUN mkdir -p /tmp/build-mpc && cd /tmp/build-mpc && \
    LD_LIBRARY_PATH=${GCCFOLDER}/gmp/lib:${GCCFOLDER}/mpfr/lib \
    /tmp/prereqs/mpc-1.0.3/configure \
        --prefix=${GCCFOLDER}/mpc \
        --with-gmp=${GCCFOLDER}/gmp \
        --with-mpfr=${GCCFOLDER}/mpfr \
        --enable-shared --disable-static && \
    make -j$(nproc) && make install

RUN mkdir -p /tmp/build-isl && cd /tmp/build-isl && \
    LD_LIBRARY_PATH=${GCCFOLDER}/gmp/lib \
    /tmp/prereqs/isl-0.16.1/configure \
        --prefix=${GCCFOLDER}/isl \
        --with-gmp-prefix=${GCCFOLDER}/gmp \
        --enable-shared --disable-static && \
    make -j$(nproc) && make install

RUN printf "${GCCFOLDER}/gmp/lib\n${GCCFOLDER}/mpfr/lib\n${GCCFOLDER}/mpc/lib\n${GCCFOLDER}/isl/lib\n" \
        > /etc/ld.so.conf.d/gcc-prereqs.conf && \
    ldconfig

# --------------------------------------------------------------------------
# GCC 7.3.0
# --------------------------------------------------------------------------
WORKDIR /tmp/gcc-src

RUN wget -q --tries=3 --timeout=30 https://ftp.gnu.org/gnu/gcc/gcc-7.3.0/gcc-7.3.0.tar.gz && \
    tar xf gcc-7.3.0.tar.gz

RUN mkdir -p /tmp/build-gcc && cd /tmp/build-gcc && \
    /tmp/gcc-src/gcc-7.3.0/configure \
        --prefix=${GCCFOLDER}/gcc \
        --with-gmp=${GCCFOLDER}/gmp \
        --with-mpfr=${GCCFOLDER}/mpfr \
        --with-mpc=${GCCFOLDER}/mpc \
        --with-isl=${GCCFOLDER}/isl \
        --enable-languages=c,c++ \
        --disable-multilib \
        --disable-bootstrap \
        --disable-libsanitizer && \
    make -j$(nproc) && make install

RUN echo "${GCCFOLDER}/gcc/lib64" > /etc/ld.so.conf.d/gcc73.conf && ldconfig && \
    find /usr/lib/$(uname -m)-linux-gnu -name "libstdc++.so.6*" \
         -exec cp -f {} ${GCCFOLDER}/gcc/lib64/ \; && \
    ldconfig

# Activate GCC
ENV PATH=${GCCFOLDER}/gcc/bin:${PATH}
ENV LD_LIBRARY_PATH=${GCCFOLDER}/gcc/lib64:${GCCFOLDER}/isl/lib:${GCCFOLDER}/mpc/lib:${GCCFOLDER}/mpfr/lib:${GCCFOLDER}/gmp/lib
ENV CC=${GCCFOLDER}/gcc/bin/gcc
ENV CXX=${GCCFOLDER}/gcc/bin/g++

# --------------------------------------------------------------------------
# LZ4
# --------------------------------------------------------------------------
WORKDIR /tmp

RUN wget -q --tries=3 --timeout=30 https://github.com/lz4/lz4/archive/refs/tags/v1.10.0.tar.gz -O lz4.tar.gz && \
    tar xf lz4.tar.gz && cd lz4-* && \
    make -j$(nproc) CC=${CC} && \
    make install PREFIX=${LOCAL_LIB_PATH}/lz4

# --------------------------------------------------------------------------
# cJSON
# --------------------------------------------------------------------------
RUN wget -q --tries=3 --timeout=30 https://github.com/DaveGamble/cJSON/archive/refs/tags/v1.7.17.tar.gz -O cjson.tar.gz && \
    tar xf cjson.tar.gz && cd cJSON-* && mkdir build && cd build && \
    cmake .. \
        -DCMAKE_INSTALL_PREFIX=${LOCAL_LIB_PATH}/cjson \
        -DCMAKE_C_COMPILER=${CC} \
        -DCMAKE_CXX_COMPILER=${CXX} \
        -DENABLE_CJSON_TEST=OFF \
        -DBUILD_SHARED_LIBS=ON && \
    make -j$(nproc) && make install

# --------------------------------------------------------------------------
# Google Test
# --------------------------------------------------------------------------
RUN wget -q --tries=3 --timeout=30 https://github.com/google/googletest/archive/refs/tags/release-1.10.0.tar.gz -O gtest.tar.gz && \
    tar xf gtest.tar.gz && cd googletest-* && mkdir build && cd build && \
    cmake .. \
        -DCMAKE_INSTALL_PREFIX=${LOCAL_LIB_PATH}/gtest \
        -DCMAKE_BUILD_TYPE=Debug \
        -DBUILD_SHARED_LIBS=OFF \
        -DCMAKE_CXX_COMPILER=${CXX} \
        -DCMAKE_C_COMPILER=${CC} && \
    make -j$(nproc) && make install

# --------------------------------------------------------------------------
# securec
# --------------------------------------------------------------------------
RUN git clone --depth=1 https://github.com/openeuler-mirror/libboundscheck.git /tmp/securec && \
    cd /tmp/securec && \
    make CC=${CC} -j$(nproc) && \
    ar rcs lib/libboundscheck.a obj/*.o && \
    cp include/*.h ${LOCAL_LIB_PATH}/secure/include/ && \
    cp lib/libboundscheck.a ${LOCAL_LIB_PATH}/secure/lib/libsecurec.a && \
    ln -sf libsecurec.a ${LOCAL_LIB_PATH}/secure/lib/libboundscheck.a

# --------------------------------------------------------------------------
# mockcpp
# --------------------------------------------------------------------------
RUN git clone --depth=1 https://github.com/sinojelly/mockcpp.git /tmp/mockcpp && \
    mkdir /tmp/mockcpp/build && cd /tmp/mockcpp/build && \
    cmake .. \
        -DCMAKE_INSTALL_PREFIX=${LOCAL_LIB_PATH}/mockcpp \
        -DCMAKE_C_COMPILER=${CC} \
        -DCMAKE_CXX_COMPILER=${CXX} \
        -DBUILD_SHARED_LIBS=OFF && \
    make -j$(nproc) && make install && \
    cp -r /tmp/mockcpp/3rdparty/. ${LOCAL_LIB_PATH}/mockcpp/3rdparty/

RUN rm -rf /tmp/*

# --------------------------------------------------------------------------
# Build project
# --------------------------------------------------------------------------

# Add build-dstore alias/function for root user
# Add build-dstore and make-dstore aliases for root
RUN echo "alias dstore-build='source ${BUILD_ROOT}/buildenv && cd ${BUILD_ROOT}/utils && bash build.sh -m debug && cd ${BUILD_ROOT} && mkdir -p tmp_build && cd tmp_build && cmake .. -DCMAKE_BUILD_TYPE=debug -DUTILS_PATH=../utils/output -DENABLE_UT=ON && make -sj$(($(nproc)-2)) install'" \
    >> /root/.bashrc && \
    echo "alias dstore-rebuild='cd ${BUILD_ROOT}/tmp_build && make install -j\$(nproc)'" \
    >> /root/.bashrc

# --------------------------------------------------------------------------
# Runtime
# --------------------------------------------------------------------------
WORKDIR ${BUILD_ROOT}
CMD ["tail", "-f", "/dev/null"]