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

#include <cstdint>
#include <string>

#include "util/DanglingViewTestHelpers.h"
#include "util/GTestHelpers.h"
#include "util/PmrStringSsoTestHelpers.h"

TEST(GTestHelpersTest, CurrentTestSuiteAndTestName) {
  EXPECT_EQ(gtestCurrentTestSuiteName(), "GTestHelpersTest");
  EXPECT_EQ(gtestCurrentTestName(),
            "GTestHelpersTest_CurrentTestSuiteAndTestName");
  EXPECT_EQ(gtestCurrentTestSuiteName(false), "GTestHelpersTest");
  EXPECT_EQ(gtestCurrentTestName(false),
            "GTestHelpersTest_CurrentTestSuiteAndTestName");
}

// _____________________________________________________________________________
class GTestHelpersParameterizedTest
    : public ::testing::TestWithParam<const char*> {};

TEST_P(GTestHelpersParameterizedTest, SlashesAreReplaced) {
  const std::string suiteName = gtestCurrentTestSuiteName();
  const std::string testName = gtestCurrentTestName();
  EXPECT_THAT(suiteName, ::testing::Not(::testing::HasSubstr("/")));
  EXPECT_THAT(testName, ::testing::Not(::testing::HasSubstr("/")));
  EXPECT_THAT(testName,
              ::testing::StartsWith(gtestCurrentTestSuiteName() + "_"));
}

INSTANTIATE_TEST_SUITE_P(CustomInstantiation, GTestHelpersParameterizedTest,
                         ::testing::Values("param/1"));

// _____________________________________________________________________________
// Return true iff `pointer` points inside the object storage of `object`.
template <typename T>
static bool pointsIntoObject(const void* pointer, const T& object) {
  const auto start = reinterpret_cast<std::uintptr_t>(&object);
  const auto address = reinterpret_cast<std::uintptr_t>(pointer);
  return address >= start && address - start < sizeof(object);
}

// _____________________________________________________________________________
TEST(GTestHelpersTest, PmrStringSsoCapacity) {
  // Ensure that the discovered capacity is usable: strings up to that size are
  // stored inside the object, and a string with one additional character is
  // not.
  size_t capacity = pmrStringSsoCapacity();
  // A zero capacity means this platform offers no inline string storage at
  // all, so every SSO-dependent assertion below would be meaningless there.
  // All supported STLs provide SSO; this states the premise explicitly
  // instead of tripping the `maxSize > 0` contract inside
  // `requirePmrStringInlineStorage` with a confusing message.
  ASSERT_GT(capacity, 0u) << "This platform offers no inline string storage, "
                             "so the SSO assertions below are meaningless";
  requirePmrStringInlineStorage(capacity);
  PmrSsoProbeString atCapacity(capacity, 'x');
  EXPECT_TRUE(pointsIntoObject(atCapacity.data(), atCapacity));
  PmrSsoProbeString aboveCapacity(capacity + 1, 'y');
  EXPECT_FALSE(pointsIntoObject(aboveCapacity.data(), aboveCapacity));
}

// _____________________________________________________________________________
TEST(GTestHelpersTest, RequirePmrStringInlineStorageRejectsInvalidSizes) {
  // `maxSize == 0` violates the precondition, and a size above the probed
  // capacity violates the platform premise; both must throw.
  EXPECT_ANY_THROW(requirePmrStringInlineStorage(0));
  AD_EXPECT_THROW_WITH_MESSAGE(
      requirePmrStringInlineStorage(pmrStringSsoCapacity() + 1),
      ::testing::HasSubstr("Platform premise violated"));
}

// _____________________________________________________________________________
TEST(GTestHelpersTest, AssertPmrStringUsesSso) {
  // Verify that empty and small strings use inline storage: the data must lie
  // inside the string object itself, not in allocator-provided memory.
  for (size_t size : {size_t{0}, size_t{7}, size_t{15}}) {
    // `maxSize == 0` is a rejected precondition, so probe from 1 on.
    if (size > 0) {
      requirePmrStringInlineStorage(size);
    }
    PmrSsoProbeString shortString(size, 'x');
    EXPECT_TRUE(pointsIntoObject(shortString.data(), shortString));
    EXPECT_EQ(shortString.size(), size);
  }
  // Verify that a string above the SSO threshold is not stored inside the
  // object.
  PmrSsoProbeString longString(64, 'y');
  EXPECT_FALSE(pointsIntoObject(longString.data(), longString));
}

// _____________________________________________________________________________
TEST(GTestHelpersTest, ClobberStack) {
  // Verify that the helper writes the given sentinel to the stack and reads it
  // back through a volatile access.
  EXPECT_EQ(clobberStack<512>('X'), 'X');
  EXPECT_EQ(clobberStack<4096>('#'), '#');
}
