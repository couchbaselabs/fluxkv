# Tests for fluxkv.
#
# The smoke test needs both binaries, so it is only registered when the server
# is built. Run with: ctest --test-dir build --output-on-failure

if(FLUXKV_BUILD_SERVER)
    add_test(NAME smoke
             COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/smoke_test.sh
                     $<TARGET_FILE:fluxkv_server>
                     $<TARGET_FILE:fluxbench>)
    set_tests_properties(smoke PROPERTIES TIMEOUT 180)
endif()
