add_library(testUtil IdTableHelpers.cpp ValidatorHelpers.cpp RandomTestHelpers.cpp BenchmarkMeasurementContainerHelpers.cpp IndexTestHelpers.cpp)
# TODO<c++20> Once there is more support for it, we should be able to do this cheaper with modules.
qlever_target_link_libraries(testUtil engine global gmock_main)

# The precompiled headers of `testUtil` are reused by all the unit tests that
# link against `testUtil` (see `linkTest` in `test/CMakeLists.txt`).
if (USE_PRECOMPILED_HEADERS)
    if (REDUCED_FEATURE_SET_FOR_CPP17)
        set(PRECOMPILED_HEADER_FILES_TEST ${PRECOMPILED_HEADER_FILES_EXTERNAL})
    else ()
        set(PRECOMPILED_HEADER_FILES_TEST ${PRECOMPILED_HEADER_FILES_WITH_BEAST})
    endif ()
    target_precompile_headers(testUtil PRIVATE
            <gtest/gtest.h> <gmock/gmock.h> ${PRECOMPILED_HEADER_FILES_TEST})
    # GCC only uses the precompiled header of `testUtil` for a test if every
    # definition of `testUtil` is also defined for the test. We therefore omit
    # the (unused) `testUtil_EXPORTS`, which CMake defines for shared libraries.
    set_target_properties(testUtil PROPERTIES DEFINE_SYMBOL "")
endif ()
