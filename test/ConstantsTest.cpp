//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gmock/gmock.h>

#include "global/Constants.h"
#include "util/GTestHelpers.h"

using namespace ad_utility;
using namespace std::chrono_literals;

using ::testing::AllOf;
using ::testing::HasSubstr;

TEST(Constants, testDefaultQueryTimeoutIsStriclyPositive) {
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      RuntimeParameters().set<"default-query-timeout">(0s),
      AllOf(HasSubstr("default-query-timeout"), HasSubstr("0s")),
      std::runtime_error);
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      RuntimeParameters().set<"default-query-timeout">(-1s),
      AllOf(HasSubstr("default-query-timeout"), HasSubstr("-1s")),
      std::runtime_error);
  EXPECT_NO_THROW(RuntimeParameters().set<"default-query-timeout">(1s));
}
