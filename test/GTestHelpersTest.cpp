// Copyright 2026, The QLever Authors, in particular:
//
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <string>

#include "util/DanglingViewTestHelpers.h"
#include "util/GTestHelpers.h"
#include "util/PmrStringSsoTestHelpers.h"

// _____________________________________________________________________________
TEST(GTestHelpersTest, CurrentTestSuiteAndTestName) {
  EXPECT_EQ(gtestCurrentTestSuiteName(), "GTestHelpersTest");
  EXPECT_EQ(gtestCurrentTestName(),
            "GTestHelpersTest_CurrentTestSuiteAndTestName");
  // Inside a running test the flag `assertInGtestEnvironment` makes no
  // difference, because the gtest environment is always present here.
  EXPECT_EQ(gtestCurrentTestSuiteName(false), "GTestHelpersTest");
  EXPECT_EQ(gtestCurrentTestName(false),
            "GTestHelpersTest_CurrentTestSuiteAndTestName");
}

// _____________________________________________________________________________
// A parameterized fixture whose gtest names contain a '/'. The parameter itself
// is irrelevant.
class GTestHelpersParameterizedTest : public ::testing::TestWithParam<int> {};

// _____________________________________________________________________________
TEST_P(GTestHelpersParameterizedTest, SlashesAreReplaced) {
  // The raw gtest names of this instantiation contain a '/' (from the
  // `CustomInstantiation/` prefix and the parameter index), so the checks below
  // actually exercise the sanitizing. The parameter value itself is irrelevant.
  const auto* testInfo =
      ::testing::UnitTest::GetInstance()->current_test_info();
  EXPECT_THAT(std::string{testInfo->test_suite_name()},
              ::testing::HasSubstr("/"));
  const std::string suiteName = gtestCurrentTestSuiteName();
  const std::string testName = gtestCurrentTestName();
  // Each '/' is replaced by '_', not deleted: the raw names are
  // `CustomInstantiation/GTestHelpersParameterizedTest` and
  // `SlashesAreReplaced/0`.
  EXPECT_EQ(suiteName, "CustomInstantiation_GTestHelpersParameterizedTest");
  EXPECT_EQ(testName, suiteName + "_SlashesAreReplaced_0");
}

INSTANTIATE_TEST_SUITE_P(CustomInstantiation, GTestHelpersParameterizedTest,
                         ::testing::Values(1));

// _____________________________________________________________________________
// Return true iff `pointer` points inside the object storage of `object`. A
// string keeps its characters inline (SSO) iff `data()` points into the string
// object itself. This address check observes the actual storage location and
// thereby independently confirms the capacity that `SsoCapacityOfPmrString`
// derives from counting allocations. `pointer` must come from `object`; the
// negative checks below rely on that.
template <typename T>
static bool pointsIntoObject(const void* pointer, const T& object) {
  const auto start = reinterpret_cast<std::uintptr_t>(&object);
  const auto address = reinterpret_cast<std::uintptr_t>(pointer);
  return address >= start && address - start < sizeof(object);
}

// _____________________________________________________________________________
TEST(GTestHelpersTest, PmrStringSsoCapacity) {
  // The probed capacity must be exact: a string of that size is stored inside
  // the object, and a string with one additional character is not. The second
  // check shows that the capacity is the maximum and not merely a lower bound.
  const size_t capacity = SsoCapacityOfPmrString();
  // A zero capacity means this platform offers no inline string storage at
  // all, so every SSO-dependent assertion below would be meaningless there.
  // All supported STLs provide SSO; this states the premise explicitly
  // instead of tripping the `minCapacity > 0` contract inside
  // `requireSsoCapacityOfPmrStringAtLeast` with a confusing message.
  ASSERT_GT(capacity, 0u) << "This platform offers no inline string storage, "
                             "so the SSO assertions below are meaningless";
  EXPECT_NO_THROW(requireSsoCapacityOfPmrStringAtLeast(capacity));
  const detail::PmrSsoProbeString atCapacity(capacity, 'x');
  EXPECT_TRUE(pointsIntoObject(atCapacity.data(), atCapacity));
  const detail::PmrSsoProbeString aboveCapacity(capacity + 1, 'y');
  EXPECT_FALSE(pointsIntoObject(aboveCapacity.data(), aboveCapacity));
}

// _____________________________________________________________________________
TEST(GTestHelpersTest, RequireSsoCapacityRejectsInvalidArguments) {
  // `minCapacity == 0` violates the precondition, and a size above the probed
  // capacity violates the platform premise.
  AD_EXPECT_THROW_WITH_MESSAGE(requireSsoCapacityOfPmrStringAtLeast(0),
                               ::testing::HasSubstr("minCapacity > 0"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      requireSsoCapacityOfPmrStringAtLeast(SsoCapacityOfPmrString() + 1),
      ::testing::HasSubstr("Platform premise violated"));
}

// _____________________________________________________________________________
TEST(GTestHelpersTest, PmrStringUsesSsoUpToCapacity) {
  // Verify that empty and small strings use inline storage: the data must lie
  // inside the string object itself, not in allocator-provided memory.
  for (const size_t size : {size_t{0}, size_t{7}, size_t{15}}) {
    // `minCapacity == 0` is a rejected precondition, so probe from 1 on.
    if (size > 0) {
      requireSsoCapacityOfPmrStringAtLeast(size);
    }
    const detail::PmrSsoProbeString shortString(size, 'x');
    EXPECT_TRUE(pointsIntoObject(shortString.data(), shortString));
    EXPECT_EQ(shortString.size(), size);
  }
  // The inline storage is part of the string object, so a string with
  // `sizeof(PmrSsoProbeString)` characters cannot fit into it and must live in
  // allocator-provided memory.
  const detail::PmrSsoProbeString longString(sizeof(detail::PmrSsoProbeString),
                                             'y');
  EXPECT_FALSE(pointsIntoObject(longString.data(), longString));
}

// _____________________________________________________________________________
TEST(GTestHelpersTest, ClobberStack) {
  // Only the value that `clobberStack` reads back through its own `volatile`
  // buffer before returning can be checked: there is no legal way to read the
  // clobbered stack memory after the function has returned (that would be
  // undefined behavior), so whether the stack really stays overwritten is not
  // tested here.
  EXPECT_EQ(clobberStack<512>('X'), 'X');
  EXPECT_EQ(clobberStack<4096>('#'), '#');
}
