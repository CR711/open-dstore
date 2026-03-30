FILE(GLOB_RECURSE dfx_src_list RELATIVE ${PROJECT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)

add_library(dfx INTERFACE)
target_include_directories(dfx INTERFACE
    ${PROJECT_SOURCE_DIR}/interface
    ${PROJECT_SOURCE_DIR}/include
    ${SECURE_INCLUDE_PATH}
    ${UTILS_INCLUDE_PATH}
    ${CJSON_INCLUDE_PATH}
)
target_link_libraries(dfx INTERFACE dstore)
