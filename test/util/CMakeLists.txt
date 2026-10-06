add_library(testUtil IdTableHelpers.cpp ValidatorHelpers.cpp RandomTestHelpers.cpp BenchmarkMeasurementContainerHelpers.cpp IndexTestHelpers.cpp)
# TODO<c++20> Once there is more support for it, we should be able to do this cheaper with modules.
qlever_target_link_libraries(testUtil engine global GTest::gtest GTest::gmock)

# The `main` function for all test binaries. It runs the tests in a scratch
# working directory, so that tests that write files via relative paths do not
# litter the directory from which the test binary is run (see
# `QleverTestMain.cpp` for details).
add_library(qleverTestMain QleverTestMain.cpp)
qlever_target_link_libraries(qleverTestMain global GTest::gtest GTest::gmock)
