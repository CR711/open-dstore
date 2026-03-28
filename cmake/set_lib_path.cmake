set(LOCAL_LIB_PATH $ENV{LOCAL_LIB_PATH})

if(NOT EXISTS "${LOCAL_LIB_PATH}")
    message(FATAL_ERROR "LOCAL_LIB_PATH(${LOCAL_LIB_PATH}) Not EXISTS! Please check local_libs exists")
endif()
message(STATUS "LOCAL_LIB_PATH is ${LOCAL_LIB_PATH}")
message(STATUS "COMPILE_PLATFORM is ${PLATFORM}")

if(${CMAKE_HOST_SYSTEM_PROCESSOR} STREQUAL "x86_64")
    set(HOST_TUPLE x86_64-pc-linux-gnu)
else()
    set(HOST_TUPLE aarch64-unknown-linux-gnu)
endif()

set(DSTORE_VERSION_TYPE ${CMAKE_BUILD_TYPE})

#############################################################################
# gcc library
#############################################################################
set(GCC_INSTALL_HOME $ENV{GCC_INSTALL_HOME})
if("${GCC_INSTALL_HOME}" STREQUAL "")
    set(GCC_INSTALL_HOME ${LOCAL_LIB_PATH}/buildtools/gcc${GCC_VERSION_LIT}/gcc)
    set(GCC_LIB64_PATH ${LOCAL_LIB_PATH}/buildtools/gcc${GCC_VERSION_LIT}/gcc/lib64)
    set(ASAN_LIB_PATH ${MEMCHECK_HOME}/gcc${GCC_VERSION}/lib)
else()
    set(GCC_LIB64_PATH ${GCC_INSTALL_HOME}/lib64)
    execute_process(COMMAND bash -c "cat /etc/os-release \| grep \"PRETTY_NAME\" \| awk -F\'\"\' \'{print $2}\' \| grep \"^Euler\"" OUTPUT_VARIABLE EULER_OS_PRETTY_NAME)
    if(NOT ${EULER_OS_PRETTY_NAME} STREQUAL "")
        string(REGEX MATCH "SP[0-9]*" EULER_OS_VERSION ${EULER_OS_PRETTY_NAME})
        string(REGEX MATCH "[0-9]+" EULER_OS_VERSION ${EULER_OS_VERSION})
        if(${EULER_OS_VERSION} GREATER 10 AND "${ENABLE_PRIVATEGAUSS}_${ENABLE_LITE_MODE}" STREQUAL "ON_ON")
            message("EULER_OS_VERSION:${EULER_OS_VERSION}")
            set(GCC_LIB64_PATH "/usr/lib64")
        endif()
    endif()
    set(MEMCHECK_LIB_PATH ${GCC_LIB64_PATH})
endif()
message("GCC_LIB64_PATH:${GCC_LIB64_PATH}")

#############################################################################
# coverage
#############################################################################
set(LCOV_HOME ${LOCAL_LIB_PATH}/buildtools/gcc${GCC_VERSION_LIT}/gcc/lib/gcc/${HOST_TUPLE})
set(LCOV_LIB_PATH ${LCOV_HOME}/${GCC_VERSION})

############################################################################
# gtest component
############################################################################
set(GTEST_HOME ${LOCAL_LIB_PATH}/gtest)
set(GTEST_INCLUDE_PATH ${GTEST_HOME}/include)
set(GTEST_LIB_PATH ${GTEST_HOME}/lib)
message("-- GTEST_HOME: ${GTEST_HOME}")

#############################################################################
# secure component
#############################################################################
set(SECURE_HOME ${LOCAL_LIB_PATH}/secure)
set(SECURE_INCLUDE_PATH ${SECURE_HOME}/include)
set(SECURE_LIB_PATH ${SECURE_HOME}/lib)
message("-- SECURE_HOME: ${SECURE_HOME}")

############################################################################
# cjson component
############################################################################
set(CJSON_HOME ${LOCAL_LIB_PATH}/cjson)
set(CJSON_INCLUDE_PATH ${CJSON_HOME}/include)
set(CJSON_LIB_PATH ${CJSON_HOME}/lib)
message("-- CJSON_HOME: ${CJSON_HOME}")

#############################################################################
# lz4 component
#############################################################################
set(LZ4_HOME ${LOCAL_LIB_PATH}/lz4)
set(LZ4_INCLUDE_PATH ${LZ4_HOME}/include)
set(LZ4_LIB_PATH ${LZ4_HOME}/lib)
set(LZ4_BIN_PATH ${LZ4_HOME}/bin)
message("-- LZ4_HOME: ${LZ4_HOME}")

#############################################################################
# mockcpp component
#############################################################################
set(MOCKCPP_HOME ${LOCAL_LIB_PATH}/mockcpp)
set(MOCKCPP_INCLUDE_PATH ${MOCKCPP_HOME}/include)
set(MOCKCPP_LIB_PATH ${MOCKCPP_HOME}/lib)
set(MOCKCPP_3RDPARTY_PATH ${MOCKCPP_HOME}/include/fake_boost)
message("-- MOCKCPP_HOME: ${MOCKCPP_HOME}")

############################################################################
# mockcpp component
############################################################################
set(MOCKCPP_HOME ${LOCAL_LIB_PATH}/mockcpp)
set(MOCKCPP_INCLUDE_PATH ${MOCKCPP_HOME}/include)
set(MOCKCPP_LIB_PATH ${MOCKCPP_HOME}/lib)
set(MOCKCPP_3RDPARTY_PATH ${MOCKCPP_HOME}/3rdparty)
message("-- MOCKCPP_HOME: ${MOCKCPP_HOME}")

############################################################################
# utils lib new component
############################################################################
set(UTILS_HOME ${CMAKE_SOURCE_DIR}/utils/output)

if(NOT EXISTS "${UTILS_HOME}")
    MESSAGE(FATAL_ERROR "UTILS_HOME(${UTILS_HOME}) not exist! Please check utils directory")
endif()
set(UTILS_LIB_PATH ${UTILS_HOME}/lib)
set(UTILS_INCLUDE_PATH ${UTILS_HOME}/include)
message("-- UTILS_HOME: ${UTILS_HOME}")