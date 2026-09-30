// Copyright 2023 - 2026 The QLever Authors, in particular:
//
// 2023 - 2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
// 2026        Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include "global/Constants.h"
#include "global/RuntimeParameters.h"
#include "util/GTestHelpers.h"
#include "util/RuntimeParametersTestHelpers.h"

using namespace ad_utility;
using namespace std::chrono_literals;

using ::testing::AllOf;
using ::testing::HasSubstr;

// _____________________________________________________________________________
TEST(Constants, testDefaultQueryTimeoutIsStriclyPositive) {
  auto reset =
      setRuntimeParameterForTest<&RuntimeParameters::defaultQueryTimeout_>(
          1337s);
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      setRuntimeParameter<&RuntimeParameters::defaultQueryTimeout_>(0s),
      AllOf(HasSubstr("default-query-timeout"), HasSubstr("0s")),
      std::runtime_error);
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      setRuntimeParameter<&RuntimeParameters::defaultQueryTimeout_>(-1s),
      AllOf(HasSubstr("default-query-timeout"), HasSubstr("-1s")),
      std::runtime_error);
  EXPECT_NO_THROW(
      setRuntimeParameter<&RuntimeParameters::defaultQueryTimeout_>(1s));
}

namespace {
constexpr std::string_view hi = "hi";
constexpr std::string_view bye = "-bye";
}  // namespace

// _____________________________________________________________________________
TEST(Constants, makeQleverInternalIri) {
  EXPECT_EQ(makeQleverInternalIri("hi", "-bye"),
            (makeQleverInternalIriConst<hi, bye>()));
  EXPECT_EQ(makeQleverInternalIri(hi, bye),
            "<http://qlever.cs.uni-freiburg.de/builtin-functions/hi-bye>");
}

// _____________________________________________________________________________
TEST(Constants, isFullTextPseudoPredicate) {
  EXPECT_TRUE(isFullTextPseudoPredicate(CONTAINS_ENTITY_PREDICATE));
  EXPECT_TRUE(isFullTextPseudoPredicate(CONTAINS_WORD_PREDICATE));
  EXPECT_FALSE(
      isFullTextPseudoPredicate("<http://example.com/normalPredicate>"));
}
