set(CMAKE_SKIP_RPATH FALSE)

# Source files
FILE(GLOB_RECURSE stress_verify_src_list RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp)

set(stress_verify_compile_options ${PROTECT_OPTIONS} ${CHECK_OPTIONS} ${BIN_SECURE_OPTIONS})

set(stress_verify_link_options ${LIB_LINK_OPTIONS})

set(stress_verify_link_libs dstore securec lz4 cjson z gsutils utilities)

set(stress_verify_macro_options ${MACRO_OPTIONS})

set(stress_verify_include_directories
    ${PROJECT_SOURCE_DIR}/interface
    ${PROJECT_SOURCE_DIR}/include
    ${SECURE_INCLUDE_PATH}
    ${UTILS_INCLUDE_PATH}
    ${CJSON_INCLUDE_PATH}
    include
)

set(stress_verify_link_directories
    ${CJSON_LIB_PATH} ${SECURE_LIB_PATH}
    ${UTILS_LIB_PATH} ${LIBEVENT_LIB_PATH}
)

add_bintarget(dstore_stress_verify stress_verify_src_list stress_verify_include_directories "${stress_verify_macro_options}" "${stress_verify_compile_options}" "${stress_verify_link_options}" "${stress_verify_link_libs}")
target_link_directories(dstore_stress_verify PRIVATE ${stress_verify_link_directories})
add_dependencies(dstore_stress_verify dstore utilities)

# VFS config path
set(VFSCONFIGPATH ${UTILS_LIB_PATH}/libvfslinuxadapter.so)

execute_process(COMMAND sed -i "s#\"clientLibPath\":\\s*\"[^\"]*\"#\"clientLibPath\": \"${VFSCONFIGPATH}\"#" ${CMAKE_CURRENT_SOURCE_DIR}/../../build_script/dstore_conf.json)

set(STARTCONFIGPATH ${CMAKE_CURRENT_SOURCE_DIR}/../../build_script/dstore_conf.json)

execute_process(COMMAND sed -i "s#\"startConfigPath\":\\s*\"[^\"]*\"#\"startConfigPath\": \"${STARTCONFIGPATH}\"#" ${CMAKE_CURRENT_SOURCE_DIR}/../../build_script/guc.json)

# Install config files
install(FILES
    "${CMAKE_CURRENT_SOURCE_DIR}/config.json"
    "${CMAKE_CURRENT_SOURCE_DIR}/../../build_script/guc.json"
    "${CMAKE_CURRENT_SOURCE_DIR}/../../build_script/dstore_conf.json"
    DESTINATION "${CMAKE_BINARY_DIR}"
)