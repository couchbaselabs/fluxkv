# FindCouchbaseMagma.cmake
#
# Locates magma and the Couchbase libraries it needs, from a Couchbase Server
# tree that was built separately.
#
# Inputs (cache variables):
#   COUCHBASE_SOURCE_DIR   Couchbase Server source root (contains magma/, platform/)
#   COUCHBASE_BUILD_DIR    Couchbase Server build root  (contains magma/libmagma.a)
#   COUCHBASE_INSTALL_DIR  Couchbase install prefix     (default: <source>/install)
#
# Outputs:
#   CBMAGMA_INCLUDE_DIRS   Include directories
#   CBMAGMA_LIBRARIES      Libraries, in link order (static link order matters)
#   CBMAGMA_DEFINITIONS    Required compile definitions
#
# The lists below mirror the compile and link commands the Couchbase build uses
# for its own magma tools. Keep them in sync if the Couchbase build changes.

if(NOT COUCHBASE_SOURCE_DIR)
    message(FATAL_ERROR
            "COUCHBASE_SOURCE_DIR is not set. Point it at a Couchbase Server "
            "source root, e.g. -DCOUCHBASE_SOURCE_DIR=/data/couchbase/source")
endif()

if(NOT COUCHBASE_BUILD_DIR)
    message(FATAL_ERROR
            "COUCHBASE_BUILD_DIR is not set. Point it at the matching build "
            "root, e.g. -DCOUCHBASE_BUILD_DIR=/data/couchbase/source/build")
endif()

if(NOT COUCHBASE_INSTALL_DIR)
    set(COUCHBASE_INSTALL_DIR "${COUCHBASE_SOURCE_DIR}/install")
endif()

set(_src "${COUCHBASE_SOURCE_DIR}")
set(_bld "${COUCHBASE_BUILD_DIR}")
set(_inst "${COUCHBASE_INSTALL_DIR}")
set(_deps "${_bld}/tlm/deps")

# Fail early with a clear message if this is not a built Couchbase tree.
if(NOT EXISTS "${_src}/magma/include/libmagma/magma.h")
    message(FATAL_ERROR
            "magma headers not found at ${_src}/magma/include/libmagma/magma.h "
            "- is COUCHBASE_SOURCE_DIR correct?")
endif()

find_library(CBMAGMA_MAGMA_LIB
             NAMES magma
             PATHS "${_bld}/magma"
             NO_DEFAULT_PATH)
if(NOT CBMAGMA_MAGMA_LIB)
    message(FATAL_ERROR
            "libmagma not found in ${_bld}/magma - has the Couchbase tree been "
            "built? Build the 'magma' target first.")
endif()

set(CBMAGMA_INCLUDE_DIRS
    # Couchbase source tree
    "${_src}"
    "${_src}/magma"
    "${_src}/magma/include"
    # The shared write-ahead log is a vendored library inside magma; its
    # headers are not on magma's public include path. Harmless when the
    # directory is absent.
    "${_src}/magma/shwal_lib/include"
    "${_src}/kv_engine/include"
    "${_src}/kv_engine/engines/ep/src/kvstore/storage_common"
    "${_src}/kv_engine/engines/ep/src/kvstore/magma-kvstore/kv_magma_common"
    "${_src}/platform/include"
    "${_src}/phosphor/include"
    "${_src}/third_party/gsl-lite/include"
    "${_src}/third_party/HdrHistogram_c/src"
    # Generated headers from the build tree
    "${_bld}"
    "${_bld}/magma"
    "${_bld}/platform/include"
    # Third-party dependencies (cbdeps)
    "${_deps}/boost.exploded/include"
    "${_deps}/breakpad.exploded/include/breakpad"
    "${_deps}/double-conversion.exploded/include"
    "${_deps}/flatbuffers.exploded/include"
    "${_deps}/fmt.exploded/include"
    "${_deps}/folly.exploded/include"
    "${_deps}/gflags.exploded/include"
    "${_deps}/glog.exploded/include"
    "${_deps}/json.exploded/include"
    "${_deps}/libevent.exploded/include"
    "${_deps}/liburing.exploded/include"
    "${_deps}/lz4.exploded/include"
    "${_deps}/openssl.exploded/include"
    "${_deps}/snappy.exploded/include"
    "${_deps}/spdlog.exploded/include"
    "${_deps}/zstd-cpp.exploded/include")

set(CBMAGMA_DEFINITIONS
    COUCHBASE_ENTERPRISE_EDITION=1
    FOLLY_CFG_NO_COROUTINES=1
    GFLAGS_IS_A_DLL=0
    GLOG_NO_ABBREVIATED_SEVERITIES
    HAVE_JEMALLOC
    HAVE_JEMALLOC_SDALLOCX
    JSON_DISABLE_ENUM_SERIALIZATION=1
    SPDLOG_COMPILED_LIB
    SPDLOG_FMT_EXTERNAL
    _GNU_SOURCE=1
    __STDC_FORMAT_MACROS
    gsl_CONFIG_CONTRACT_VIOLATION_THROWS)

# lz4 ships a versioned soname; resolve whatever this tree has.
file(GLOB _lz4_lib "${_deps}/lz4.exploded/lib/liblz4.so*")
list(GET _lz4_lib 0 CBMAGMA_LZ4_LIB)

# Link order matters: these are mostly static archives.
set(CBMAGMA_LIBRARIES
    "${_bld}/platform/cbcrypto/libcbcrypto.a"
    "${_bld}/platform/cbcompress/libcbcompress.a"
    "${_bld}/phosphor/libphosphor.a"
    "${_bld}/platform/libplatform.a"
    "${_bld}/platform/hdrhistogram/libhdrhistogram.a"
    "${_deps}/spdlog.exploded/lib64/libspdlog.a"
    "${_deps}/flatbuffers.exploded/lib64/libflatbuffers.a"
    "${_deps}/boost.exploded/lib/libboost_container.a"
    "${_deps}/boost.exploded/lib/libboost_context.a"
    "${_deps}/boost.exploded/lib/libboost_filesystem.a"
    "${_deps}/boost.exploded/lib/libboost_program_options.a"
    "${_deps}/boost.exploded/lib/libboost_regex.a"
    "${_deps}/boost.exploded/lib/libboost_system.a"
    "${_deps}/boost.exploded/lib/libboost_thread.a"
    "${_deps}/boost.exploded/lib/libboost_chrono.a"
    "${_deps}/boost.exploded/lib/libboost_atomic.a"
    "${_deps}/zstd-cpp.exploded/lib/libzstd.so"
    "${_inst}/lib/liburing.so"
    "${_bld}/kv_engine/libep-engine_storage_common.a"
    "${_inst}/lib/libevent_core.so"
    "${_inst}/lib/libevent_extra.so"
    "${_inst}/lib/libevent_pthreads.so"
    "${_inst}/lib/libevent_openssl.so"
    "${_bld}/kv_engine/libep-engine_magma_common.a"
    "${CBMAGMA_MAGMA_LIB}"
    "${CBMAGMA_LZ4_LIB}"
    "${_bld}/kv_engine/libep-engine_collections.a"
    "${_bld}/kv_engine/libmcd_util.a"
    "${_bld}/kv_engine/libmemcached_logger.a"
    "${_bld}/kv_engine/libmcd_dek.a"
    "${_bld}/third_party/HdrHistogram_c/src/libhdr_histogram_static.a"
    m
    "${_deps}/breakpad.exploded/lib/libbreakpad_client.a"
    "${_bld}/kv_engine/libengine_utilities.a"
    "${_deps}/snappy.exploded/lib/libsnappy.so"
    "${_inst}/lib/libsodium.so"
    "${_deps}/zlib.exploded/lib/libz.so"
    "${_deps}/jemalloc.exploded/lib/libjemalloc.so"
    "${_deps}/folly.exploded/lib/libfolly.a"
    "${_deps}/openssl.exploded/lib/libssl.so"
    "${_deps}/openssl.exploded/lib/libcrypto.so"
    "${_deps}/double-conversion.exploded/lib/libdouble-conversion.a"
    "${_deps}/gflags.exploded/lib/libgflags_nothreads.a"
    "${_deps}/glog.exploded/lib64/libglog.a"
    pthread
    rt
    "${_bld}/platform/libplatform_cbassert.a"
    stdc++exp
    dl
    "${_bld}/kv_engine/libjson_validator.a"
    "${_bld}/platform/libJSON_checker.a"
    "${_deps}/fmt.exploded/lib/libfmt.a"
    "${_deps}/simdutf.exploded/lib/libsimdutf.a")

# Shared libraries above are referenced by absolute path; record their
# directories so the built binary can find them at runtime.
set(CBMAGMA_RPATH
    "${_deps}/zstd-cpp.exploded/lib"
    "${_deps}/lz4.exploded/lib"
    "${_deps}/snappy.exploded/lib"
    "${_deps}/zlib.exploded/lib"
    "${_deps}/jemalloc.exploded/lib"
    "${_deps}/openssl.exploded/lib"
    "${_inst}/lib")

message(STATUS "Found magma: ${CBMAGMA_MAGMA_LIB}")
message(STATUS "  Couchbase source: ${_src}")
message(STATUS "  Couchbase build:  ${_bld}")
