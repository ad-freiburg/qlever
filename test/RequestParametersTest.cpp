#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "engine/RequestParameters.h"
#include "util/GTestHelpers.h"

namespace {
using namespace ad_utility::request_parameters;
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
              testing::Pair(true, true));
  EXPECT_THAT(determineResultPinning({{"pin-result", {"true"}}}),
              testing::Pair(false, true));
  EXPECT_THAT(determineResultPinning({{"pin-subresults", {"otherValue"}}}),
              testing::Pair(false, false));
}
