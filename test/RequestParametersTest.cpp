// Copyright 2026, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Tomas Damek <tomas.damek@email.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "engine/RequestParameters.h"
#include "gmock/gmock.h"
#include "util/GTestHelpers.h"

namespace {
using namespace qlever::http_api_helpers;
}  // namespace

// _____________________________________________________________________________
TEST(RequestParametersTest, parsePinGeoIndexSimplification) {
  // No value given - no simplification.
  EXPECT_EQ(parsePinGeoIndexSimplification(std::nullopt), std::nullopt);

  // A valid positive number is parsed correctly.
  EXPECT_THAT(parsePinGeoIndexSimplification("10.5"),
              ::testing::Optional(10.5));

  // A non-numeric value throws.
  AD_EXPECT_THROW_WITH_MESSAGE(
      parsePinGeoIndexSimplification("not-a-number"),
      testing::HasSubstr(
          "Invalid value for `pin-geo-index-simplification`: must be a "
          "floating-point number of meters."));

  // Negative and zero values are not rejected by the parser itself (that is
  // left to the downstream consumer, see `GeoConverters::simplifyPolyline`).
  EXPECT_THAT(parsePinGeoIndexSimplification("-5"), ::testing::Optional(-5.0));
  EXPECT_THAT(parsePinGeoIndexSimplification("0"), ::testing::Optional(0.0));
}

// _____________________________________________________________________________
TEST(RequestParametersTest, determineResultPinning) {
  EXPECT_THAT(determineResultPinning(
                  {{"pin-subresults", {"true"}}, {"pin-result", {"true"}}}),
              ResultPinning(true, true));
  EXPECT_THAT(determineResultPinning({{"pin-result", {"true"}}}),
              ResultPinning(false, true));
  EXPECT_THAT(determineResultPinning({{"pin-subresults", {"otherValue"}}}),
              ResultPinning(false, false));
}

// _____________________________________________________________________________
TEST(RequestParametersTest, parseSendLimit) {
  // No value given - no specific limit given.
  EXPECT_EQ(parseSendLimit({}), std::nullopt);

  // A valid positive number is parsed correctly.
  EXPECT_THAT(parseSendLimit({{"send", {"12"}}}), testing::Optional(12ul));

  // A non-numeric value throws.
  AD_EXPECT_THROW_WITH_MESSAGE(
      qlever::http_api_helpers::parseSendLimit({{"send", {"not-a-number"}}}),
      testing::HasSubstr("Invalid value for `send`: must be a "
                         "positive integer specifying the number of bindings "
                         "to be exported."));

  // A negative value throws.
  AD_EXPECT_THROW_WITH_MESSAGE(
      qlever::http_api_helpers::parseSendLimit({{"send", {"-1"}}}),
      testing::HasSubstr("Invalid value for `send`: must be a "
                         "positive integer specifying the number of bindings "
                         "to be exported."));
}
