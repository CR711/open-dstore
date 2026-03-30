execute_process(
    COMMAND perl  generate-wait_state_types.pl  wait_state_names.txt 
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/common/instrument/wait_event
)

execute_process(
    COMMAND cp ${CMAKE_CURRENT_SOURCE_DIR}/common/instrument/wait_event/wait_state.h 
    ${CMAKE_CURRENT_SOURCE_DIR}/../interface/framework/dstore_wait_state.h
)

execute_process(
    COMMAND perl  generate-io_event_types.pl  io_event_names.txt 
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/common/instrument/wait_event
)

execute_process(
    COMMAND cp ${CMAKE_CURRENT_SOURCE_DIR}/common/instrument/wait_event/io_event.h 
    ${CMAKE_CURRENT_SOURCE_DIR}/../interface/framework/dstore_io_event.h
)

add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/common/instrument/trace)

# see the macro definition in macro_utils.cmake
SUBSRCLIST(dstore_src_list ${CMAKE_CURRENT_SOURCE_DIR})
list(FILTER dstore_src_list EXCLUDE REGEX ".*/flashback/.*")

if("${CMAKE_HOST_SYSTEM_PROCESSOR}" STREQUAL "aarch64")
    set(CMAKE_ASM_FLAGS "${CMAKE_ASM_FLAGS} -march=armv8-a+crc+crypto")
endif()

list(APPEND OPTIMIZE_OPTIONS -Wl,-Bsymbolic)
set(dstore_compile_options ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${OPTIMIZE_OPTIONS} ${CHECK_OPTIONS} ${LIB_SECURE_OPTIONS})
set(dstore_link_options ${LIB_LINK_OPTIONS})
set(dstore_link_libs securec lz4 cjson z gsutils dl)

set(dstore_macro_options ${MACRO_OPTIONS})
set(dstore_include_directories
    ${SECURE_INCLUDE_PATH}
    ${CJSON_INCLUDE_PATH}
    ${PROJECT_SOURCE_DIR}/interface
    ${PROJECT_SOURCE_DIR}/include
    ${UTILS_INCLUDE_PATH}
    ${LZ4_INCLUDE_PATH}
)
set(dstore_link_directories
    ${CJSON_LIB_PATH} ${SECURE_LIB_PATH}
    ${UTILS_LIB_PATH} ${LZ4_LIB_PATH}
    )

add_shared_libtarget(dstore dstore_src_list dstore_include_directories "${dstore_macro_options}" "${dstore_compile_options}" "${dstore_link_options}" "${dstore_link_libs}")
target_link_directories(dstore 
    BEFORE PUBLIC 
    ${dstore_link_directories}
)

add_static_libtarget(dstore dstore_src_list dstore_include_directories "${dstore_macro_options}" "${dstore_compile_options}")

add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/dfx)
