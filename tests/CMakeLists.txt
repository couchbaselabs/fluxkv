# Tests for fluxkv.
#
# The smoke test needs both binaries, so it is only registered when the server
# is built. Run with: ctest --test-dir build --output-on-failure

if(FLUXKV_BUILD_SERVER)
    add_test(NAME cache_unit COMMAND cache_test)
    set_tests_properties(cache_unit PROPERTIES TIMEOUT 120)

    add_test(NAME smoke
             COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/smoke_test.sh
                     $<TARGET_FILE:fluxkv_server>
                     $<TARGET_FILE:fluxbench>)
    set_tests_properties(smoke PROPERTIES TIMEOUT 180)

    add_test(NAME durability
             COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/durability_test.sh
                     $<TARGET_FILE:fluxkv_server>
                     $<TARGET_FILE:fluxbench>)
    set_tests_properties(durability PROPERTIES TIMEOUT 600)

    add_test(NAME smoke_cache
             COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/smoke_test.sh
                     $<TARGET_FILE:fluxkv_server>
                     $<TARGET_FILE:fluxbench>)
    set_tests_properties(smoke_cache PROPERTIES
                         TIMEOUT 180
                         ENVIRONMENT "FLUXKV_TEST_CACHE_SIZE=67108864")

    # The server tests bind a fixed port, so they cannot share the machine.
    set_tests_properties(smoke_cache PROPERTIES DEPENDS smoke)
    set_tests_properties(durability PROPERTIES DEPENDS smoke_cache)
endif()
