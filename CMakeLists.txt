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
    message(FATAL_ERROR "fluxkv builds on Linux only.")
endif()

# The server links a Couchbase build; the client does not. Turn the server off
# to build just the client on a load-generator machine.
option(FLUXKV_BUILD_SERVER "Build fluxkv_server (needs a Couchbase build)" ON)

if(FLUXKV_BUILD_SERVER)
    list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
    include(FindCouchbaseMagma)

    # Couchbase compiles folly with -march=x86-64-v3, which picks folly's
    # SIMD+CRC F14 hash-table code path. Any TU that includes F14 headers has
    # to target the same ISA or the link fails on F14LinkCheck<mode>.
    set(FLUXKV_ARCH_FLAGS -march=x86-64-v3)

    # The document cache is its own library so the unit test can link it
    # without the rest of the server.
    add_library(fluxkv_cache STATIC
                server/cache/doccache.cc
                server/cache/s3fifo_cache.cc
                server/statslot.cc)
    target_include_directories(fluxkv_cache PUBLIC
                               "${CMAKE_CURRENT_SOURCE_DIR}/server"
                               ${CBMAGMA_INCLUDE_DIRS})
    target_compile_definitions(fluxkv_cache PUBLIC ${CBMAGMA_DEFINITIONS})
    target_compile_options(fluxkv_cache PUBLIC ${FLUXKV_ARCH_FLAGS})

    add_executable(fluxkv_server
                   server/main.cc
                   server/server.cc
                   server/connection.cc
                   server/protocol.cc
                   server/engine.cc
                   server/tuner.cc
                   server/alloc.cc)

    target_include_directories(fluxkv_server PRIVATE
                               "${CMAKE_CURRENT_SOURCE_DIR}/server"
                               ${CBMAGMA_INCLUDE_DIRS})

    target_compile_definitions(fluxkv_server PRIVATE ${CBMAGMA_DEFINITIONS})
    target_compile_options(fluxkv_server PRIVATE ${FLUXKV_ARCH_FLAGS})

    # These archives reference each other (magma -> cbcrypto, kv_engine ->
    # platform), so a single pass in list order leaves symbols unresolved.
    # Wrap them in a link group and let the linker rescan to a fixed point.
    target_link_libraries(fluxkv_server PRIVATE
                          fluxkv_cache
                          -Wl,--start-group
                          ${CBMAGMA_LIBRARIES}
                          -Wl,--end-group)

    set_target_properties(fluxkv_server PROPERTIES
                          BUILD_RPATH "${CBMAGMA_RPATH}")

    # Unit test for the document cache. Only folly is needed, but it comes
    # from the same Couchbase build, so it is gated with the server.
    add_executable(cache_test tests/cache_test.cc)
    target_link_libraries(cache_test PRIVATE
                          fluxkv_cache
                          -Wl,--start-group
                          ${CBMAGMA_LIBRARIES}
                          -Wl,--end-group)
    set_target_properties(cache_test PROPERTIES
                          BUILD_RPATH "${CBMAGMA_RPATH}")
endif()

add_executable(fluxbench client/fluxbench.cc)
target_link_libraries(fluxbench PRIVATE pthread)

enable_testing()
add_subdirectory(tests)
