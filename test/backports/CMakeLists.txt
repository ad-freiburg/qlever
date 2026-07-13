
add_executable(AlgorithmBackportTests algorithmTest.cpp)
# `DebugJoinView` directly exercises `cppcoro::generator`-based coroutine
# machinery, which is entirely excluded under `REDUCED_FEATURE_SET_FOR_CPP17`.
if (NOT REDUCED_FEATURE_SET_FOR_CPP17)
    add_executable(DebugJoinView DebugJoinView.cpp)
    qlever_target_link_libraries(DebugJoinView)
endif ()
target_link_libraries(AlgorithmBackportTests GTest::gtest GTest::gmock_main)
gtest_discover_tests(AlgorithmBackportTests AlgorithmBackportTests)
addLinkAndDiscoverTestNoLibs(ConceptsTest)
addLinkAndDiscoverTestNoLibs(ShiftTest)
addLinkAndDiscoverTestNoLibs(ValueIdentityTest)
addLinkAndDiscoverTestNoLibs(BackportIteratorTest)
addLinkAndDiscoverTestNoLibs(StartsWithTest)
addLinkAndDiscoverTestNoLibs(EndsWithTest)
addLinkAndDiscoverTestNoLibs(FunctionalTest)
addLinkAndDiscoverTestNoLibs(ThreeWayComparisonTest)
addLinkAndDiscoverTestNoLibs(AtomicFlagTest)
