set(dstore_verify_src_list
    ${CMAKE_CURRENT_SOURCE_DIR}/dstore_verify_main.cpp
)

set(dstore_verify_compile_options ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${OPTIMIZE_OPTIONS} ${CHECK_OPTIONS} ${BIN_SECURE_OPTIONS})
set(dstore_verify_link_options ${BIN_LINK_OPTIONS})
set(dstore_verify_link_libs dfx securec lz4 cjson z gsutils dl)
set(dstore_verify_macro_options ${MACRO_OPTIONS})
set(dstore_verify_include_directories
    ${PROJECT_SOURCE_DIR}/interface
    ${PROJECT_SOURCE_DIR}/include
    ${SECURE_INCLUDE_PATH}
    ${UTILS_INCLUDE_PATH}
    ${CJSON_INCLUDE_PATH}
)
set(dstore_verify_link_directories
    ${CJSON_LIB_PATH} ${SECURE_LIB_PATH}
    ${UTILS_LIB_PATH} ${LIBEVENT_LIB_PATH}
)

add_bintarget(dstore_verify dstore_verify_src_list dstore_verify_include_directories "${dstore_verify_macro_options}"
    "${dstore_verify_compile_options}" "${dstore_verify_link_options}" "${dstore_verify_link_libs}")
target_link_directories(dstore_verify PRIVATE ${dstore_verify_link_directories})
add_dependencies(dstore_verify dfx)
