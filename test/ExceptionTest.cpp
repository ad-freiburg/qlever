// Copyright 2023 - 2025, University of Freiburg
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "./util/GTestHelpers.h"
#include "util/Algorithm.h"
#include "util/Exception.h"

using namespace ad_utility;

namespace {
auto makeMatcher = [](const std::string& condition,
                      ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
  using namespace ::testing;
  auto line = l.line();
  auto e = [](size_t l) { return EndsWith(std::to_string(l)); };
  auto approximateLineMatcher =
      AnyOf(e(line - 5), e(line - 4), e(line - 3), e(line - 2), e(line - 1),
            e(line), e(line + 1), e(line + 2));
  return AllOf(HasSubstr(condition), HasSubstr(l.file_name()),
               approximateLineMatcher);
};

void checkContains(const std::exception& e, std::string_view substring) {
  ASSERT_TRUE(ad_utility::contains(std::string_view{e.what()}, substring));
}
}  // namespace

TEST(Exception, AbortException) {
  AbortException a{std::runtime_error{"errorA"}};
  ASSERT_STREQ(a.what(), "errorA");
  AbortException b{"errorB"};
  ASSERT_STREQ(b.what(), "errorB");
}

TEST(Exception, Exception) {
  ad_utility::source_location l = AD_CURRENT_SOURCE_LOC();
  Exception e{"exceptionE"};
  ASSERT_THAT(e.what(), ::testing::StartsWith("exceptionE"));
  ASSERT_THAT(e.what(), ::testing::EndsWith(std::to_string(l.line() + 1)));
  checkContains(e, l.file_name());
}

TEST(Exception, AD_THROW) {
  ad_utility::source_location l;
  try {
    l = AD_CURRENT_SOURCE_LOC();
    AD_THROW("anError");
    FAIL() << "No exception was thrown, but one was expected";
  } catch (const Exception& e) {
    ASSERT_THAT(e.what(), ::testing::StartsWith("anError"));
    ASSERT_THAT(e.what(), ::testing::EndsWith(std::to_string(l.line() + 1)));
    checkContains(e, l.file_name());
  }
}

TEST(Exception, AD_CONTRACT_CHECK) {
  ASSERT_NO_THROW(AD_CONTRACT_CHECK(3 < 5));
  std::vector<int> v;
  ASSERT_NO_THROW(AD_CONTRACT_CHECK(v.empty()));
  auto failCheck = [] { AD_CONTRACT_CHECK(3 > 5); };
  AD_EXPECT_THROW_WITH_MESSAGE(failCheck(), makeMatcher("3 > 5"));
}

TEST(Exception, contractCheckWithMessage) {
  ASSERT_NO_THROW(AD_CONTRACT_CHECK(3 < 5, "some message"));
  ASSERT_NO_THROW(
      AD_CONTRACT_CHECK(3 < 5, [x = "someMessage"]() { return x; }));
  std::vector<int> v;

  v.push_back(27);
  auto failCheck = [&v] { AD_CONTRACT_CHECK(v.empty(), "`v` must be empty"); };
  AD_EXPECT_THROW_WITH_MESSAGE(
      (failCheck()),
      makeMatcher("Assertion `v.empty()` failed. `v` must be empty. Please"));

  auto failCheck2 = [&v] {
    AD_CONTRACT_CHECK(v.empty(),
                      [&v] { return absl::StrCat("`v` has size ", v.size()); });
  };
  AD_EXPECT_THROW_WITH_MESSAGE(
      (failCheck2()),
      makeMatcher("Assertion `v.empty()` failed. `v` has size 1. Please"));

  auto failCheck3 = [&v] {
    AD_CONTRACT_CHECK(v.empty(), "`v` has size ", v.size(), " and not 0");
  };
  AD_EXPECT_THROW_WITH_MESSAGE(
      (failCheck3()),
      makeMatcher(
          "Assertion `v.empty()` failed. `v` has size 1 and not 0. Please"));

  // No additional message, check that no additional full stops or spaces are
  // inserted.
  auto failCheck4 = [&v] { AD_CONTRACT_CHECK(v.empty()); };
  AD_EXPECT_THROW_WITH_MESSAGE(
      (failCheck4()),
      makeMatcher("Assertion `v.empty()` failed. Please report"));
}

TEST(Exception, AD_CORRECTNESS_CHECK) {
  ASSERT_NO_THROW(AD_CORRECTNESS_CHECK(3 < 5));
  std::vector<int> v;
  ASSERT_NO_THROW(AD_CORRECTNESS_CHECK(v.empty()));
  v.push_back(27);
  auto failCheck = [&v] { AD_CONTRACT_CHECK(v.empty()); };
  AD_EXPECT_THROW_WITH_MESSAGE(failCheck(), makeMatcher("v.empty()"));
}

TEST(Exception, correctnessCheckWithMessage) {
  ASSERT_NO_THROW(AD_CORRECTNESS_CHECK(3 < 5, "some message"));
  ASSERT_NO_THROW(
      AD_CORRECTNESS_CHECK(3 < 5, [x = "someMessage"]() { return x; }));
  std::vector<int> v;

  v.push_back(27);
  auto failCheck = [&v] {
    AD_CORRECTNESS_CHECK(v.empty(), "`v` must be empty");
  };
  AD_EXPECT_THROW_WITH_MESSAGE(
      (failCheck()),
      makeMatcher("Assertion `v.empty()` failed. `v` must be empty. Please"));

  auto failCheck2 = [&v] {
    AD_CORRECTNESS_CHECK(
        v.empty(), [&v] { return absl::StrCat("`v` has size ", v.size()); });
  };
  AD_EXPECT_THROW_WITH_MESSAGE(
      (failCheck2()),
      makeMatcher("Assertion `v.empty()` failed. `v` has size 1. Please"));

  auto failCheck3 = [&v] {
    AD_CORRECTNESS_CHECK(v.empty(), "`v` has size ", v.size(), " and not 0");
  };
  AD_EXPECT_THROW_WITH_MESSAGE(
      (failCheck3()),
      makeMatcher(
          "Assertion `v.empty()` failed. `v` has size 1 and not 0. Please"));

  // No additional message, check that no additional full stops or spaces are
  // inserted.
  auto failCheck4 = [&v] { AD_CORRECTNESS_CHECK(v.empty()); };
  AD_EXPECT_THROW_WITH_MESSAGE(
      (failCheck4()),
      makeMatcher("Assertion `v.empty()` failed. Please report"));
}

TEST(Exception, AD_FAIL) {
  try {
    AD_FAIL();
    FAIL() << "No exception was thrown, but one was expected";
  } catch (const Exception& e) {
    ASSERT_THAT(e.what(),
                ::testing::StartsWith("This code should be unreachable."));
  }
}

// The following functions demonstrate and test that all the check macros can be
// used inside `constexpr` functions, see the note on `constexpr` in
// `util/Exception.h`. The `static_assert`s below actually evaluate them at
// compile time, which is what pins down the property.
namespace constexprChecks {
constexpr int contractCheck(int x) {
  AD_CONTRACT_CHECK(x > 0);
  return x + 1;
}

constexpr int contractCheckWithMessage(int x) {
  AD_CONTRACT_CHECK(x > 0, "x must be positive, but was ", x);
  return x + 1;
}

constexpr int correctnessCheck(int x) {
  AD_CORRECTNESS_CHECK(x > 0);
  return x + 1;
}

constexpr int correctnessCheckWithMessage(int x) {
  AD_CORRECTNESS_CHECK(x > 0, "x must be positive, but was ", x);
  return x + 1;
}

constexpr int expensiveCheck(int x) {
  AD_EXPENSIVE_CHECK(x > 0);
  return x + 1;
}

// `AD_FAIL` is only valid on a branch that is not taken, else the function
// could never be evaluated at compile time.
constexpr int fail(int x) {
  if (x > 0) {
    return x + 1;
  }
  AD_FAIL();
}

static_assert(contractCheck(1) == 2);
static_assert(contractCheckWithMessage(1) == 2);
static_assert(correctnessCheck(1) == 2);
static_assert(correctnessCheckWithMessage(1) == 2);
static_assert(expensiveCheck(1) == 2);
static_assert(fail(1) == 2);
}  // namespace constexprChecks

// _____________________________________________________________________________
TEST(Exception, constexprChecks) {
  using namespace constexprChecks;
  using ::testing::HasSubstr;
  // The same functions still throw at runtime if the condition is violated.
  EXPECT_EQ(contractCheck(1), 2);
  AD_EXPECT_THROW_WITH_MESSAGE(contractCheck(-1),
                               HasSubstr("Assertion `x > 0` failed"));
  AD_EXPECT_THROW_WITH_MESSAGE(contractCheckWithMessage(-1),
                               HasSubstr("x must be positive, but was -1"));
  AD_EXPECT_THROW_WITH_MESSAGE(correctnessCheck(-1),
                               HasSubstr("Assertion `x > 0` failed"));
  AD_EXPECT_THROW_WITH_MESSAGE(correctnessCheckWithMessage(-1),
                               HasSubstr("x must be positive, but was -1"));
  AD_EXPECT_THROW_WITH_MESSAGE(fail(-1),
                               HasSubstr("This code should be unreachable"));
  if (ad_utility::areExpensiveChecksEnabled) {
    AD_EXPECT_THROW_WITH_MESSAGE(expensiveCheck(-1),
                                 HasSubstr("Assertion `x > 0` failed"));
  } else {
    EXPECT_EQ(expensiveCheck(-1), 0);
  }
}

TEST(Exception, AD_EXPENSIVE_CHECK) {
#if (!defined(NDEBUG) || defined(AD_ENABLE_EXPENSIVE_CHECKS))
  ASSERT_ANY_THROW(AD_EXPENSIVE_CHECK(3 > 5));
#else
  ASSERT_NO_THROW(AD_EXPENSIVE_CHECK(3 > 5));
#endif
  if (ad_utility::areExpensiveChecksEnabled) {
    ASSERT_ANY_THROW(AD_EXPENSIVE_CHECK(3 > 5));
  } else {
    ASSERT_NO_THROW(AD_EXPENSIVE_CHECK(3 > 5));
  }
}
