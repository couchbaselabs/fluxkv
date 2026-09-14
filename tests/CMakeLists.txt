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

    add_test(NAME durability
             COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/durability_test.sh
                     $<TARGET_FILE:fluxkv_server>
                     $<TARGET_FILE:fluxbench>)
    set_tests_properties(durability PROPERTIES TIMEOUT 600)

    # Both tests bind a fixed port, so they cannot share the machine.
    set_tests_properties(durability PROPERTIES DEPENDS smoke)
endif()
