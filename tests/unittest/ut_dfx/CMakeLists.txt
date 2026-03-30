FILE(GLOB_RECURSE ut_dfx_src_list RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)

set(ut_dfx_compile_options ${PROTECT_OPTIONS} ${CHECK_OPTIONS} ${BIN_SECURE_OPTIONS})
set(ut_dfx_link_options ${BIN_LINK_OPTIONS})
set(ut_dfx_macro_options ${MACRO_OPTIONS})
set(ut_dfx_include_directories
    ${PROJECT_SOURCE_DIR}/interface
    ${PROJECT_SOURCE_DIR}/include
    ${PROJECT_SOURCE_DIR}/tests/unittest/include
    ${SECURE_INCLUDE_PATH}
    ${UTILS_INCLUDE_PATH}
    ${CJSON_INCLUDE_PATH}
    ${GTEST_INCLUDE_PATH}
    ${MOCKCPP_INCLUDE_PATH}
    ${MOCKCPP_3RDPARTY_PATH}
)
set(ut_dfx_link_libs utilities gsutils ${UT_LIBS} mockcpp dfx securec -lrt -rdynamic -lstdc++ m)

if (ut_dfx_src_list)
    add_bintarget(ut_dfx ut_dfx_src_list ut_dfx_include_directories "${ut_dfx_macro_options}" "${ut_dfx_compile_options}"
        "${ut_dfx_link_options}" "${ut_dfx_link_libs}")
    target_link_directories(ut_dfx PRIVATE ${GTEST_LIB_PATH} ${MOCKCPP_LIB_PATH} ${LIBEVENT_LIB_PATH})
    add_dependencies(ut_dfx dfx utilities vfs_config start_config)
endif()
