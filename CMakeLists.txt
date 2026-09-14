cmake_minimum_required(VERSION 3.20)

project(fluxkv
        VERSION 0.1.0
        DESCRIPTION "Minimal mcbp key-value server on magma"
        LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "" FORCE)
endif()

if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
    message(FATAL_ERROR
            "fluxkv builds on Linux only - it links a Couchbase Server build.")
endif()

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
include(FindCouchbaseMagma)

add_executable(fluxkv_server
               server/main.cc
               server/server.cc
               server/connection.cc
               server/protocol.cc
               server/engine.cc)

target_include_directories(fluxkv_server PRIVATE
                           "${CMAKE_CURRENT_SOURCE_DIR}/server"
                           ${CBMAGMA_INCLUDE_DIRS})

target_compile_definitions(fluxkv_server PRIVATE ${CBMAGMA_DEFINITIONS})

# These archives reference each other (magma -> cbcrypto, kv_engine -> platform),
# so a single pass in list order leaves symbols unresolved. Wrap them in a link
# group and let the linker rescan until it reaches a fixed point.
target_link_libraries(fluxkv_server PRIVATE
                      -Wl,--start-group
                      ${CBMAGMA_LIBRARIES}
                      -Wl,--end-group)

set_target_properties(fluxkv_server PROPERTIES
                      BUILD_RPATH "${CBMAGMA_RPATH}")

enable_testing()
add_subdirectory(tests)
