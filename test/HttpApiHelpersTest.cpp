// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Tomas Damek <tomas.damek@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "engine/HttpApiHelpers.h"
#include "util/GTestHelpers.h"
#include "util/RuntimeParametersTestHelpers.h"

namespace {
using namespace qlever::http_api_helpers;
}  // namespace

// _____________________________________________________________________________
TEST(HttpApiHelpersTest, parsePinGeoIndexSimplification) {
  // No value given - no simplification.
  EXPECT_EQ(parsePinGeoIndexSimplification(std::nullopt), std::nullopt);

  // A valid positive number is parsed correctly.
  EXPECT_THAT(parsePinGeoIndexSimplification("10.5"),
              ::testing::Optional(10.5));

  // A non-numeric value throws.
  AD_EXPECT_THROW_WITH_MESSAGE(
      parsePinGeoIndexSimplification("not-a-number"),
      ::testing::HasSubstr(
          "Invalid value for `pin-geo-index-simplification`: must be a "
          "floating-point number of meters."));

  // Negative and zero values are not rejected by the parser itself (that is
  // left to the downstream consumer, see `GeoConverters::simplifyPolyline`).
  EXPECT_THAT(parsePinGeoIndexSimplification("-5"), ::testing::Optional(-5.0));
  EXPECT_THAT(parsePinGeoIndexSimplification("0"), ::testing::Optional(0.0));
}

// _____________________________________________________________________________
TEST(HttpApiHelpersTest, determineMediaType) {
  auto checkActionMediatype = [&](const std::string& actionName,
                                  ad_utility::MediaType expectedMediaType) {
    EXPECT_THAT(determineMediaTypes({{"action", {actionName}}}, ""),
                ::testing::ElementsAre(expectedMediaType));
  };
  // The media type associated with the action overrides the `Accept` header.
  EXPECT_THAT(determineMediaTypes({{"action", {"csv_export"}}},
                                  "application/sparql-results+json"),
              ::testing::ElementsAre(ad_utility::MediaType::csv));
  checkActionMediatype("csv_export", ad_utility::MediaType::csv);
  checkActionMediatype("tsv_export", ad_utility::MediaType::tsv);
  checkActionMediatype("qlever_json_export", ad_utility::MediaType::qleverJson);
  checkActionMediatype("sparql_json_export", ad_utility::MediaType::sparqlJson);
  checkActionMediatype("turtle_export", ad_utility::MediaType::turtle);
  checkActionMediatype("binary_export", ad_utility::MediaType::octetStream);
  EXPECT_THAT(determineMediaTypes({}, "application/sparql-results+json"),
              ::testing::ElementsAre(ad_utility::MediaType::sparqlJson));
  // No supported media type in the `Accept` header. (Contrary to its docstring
  // and interface) `ad_utility::getMediaTypeFromAcceptHeader` throws an
  // exception if no supported media type is found.
  AD_EXPECT_THROW_WITH_MESSAGE(
      determineMediaTypes({}, "text/css"),
      ::testing::HasSubstr("Not a single media type known to this parser was "
                           "detected in \"text/css\"."));
  // No or empty `Accept` header means that any content type is allowed.
  EXPECT_THAT(determineMediaTypes({}, ""), ::testing::ElementsAre());
}

// _____________________________________________________________________________
TEST(HttpApiHelpersTest, describeForLog) {
  // Nothing pinned.
  EXPECT_EQ(ResultPinning{}.describeForLog(), "");

  // Pin result only.
  EXPECT_EQ(ResultPinning{.pinResult_ = true}.describeForLog(),
            " [pin result]");

  // Pin subresults only.
  EXPECT_EQ(ResultPinning{.pinSubtrees_ = true}.describeForLog(),
            " [pin subresults]");

  // Pin result and subresults.
  EXPECT_EQ((ResultPinning{.pinSubtrees_ = true, .pinResult_ = true}
                 .describeForLog()),
            " [pin result] [pin subresults]");

  // Pinned name only.
  EXPECT_EQ(ResultPinning{.pinResultWithName_ = "myPin"}.describeForLog(),
            " [pin result with name \"myPin\"]");

  // Pinned name and geo index, but no simplification.
  EXPECT_EQ(
      (ResultPinning{.pinResultWithName_ = "myPin", .pinNamedGeoIndex_ = "geom"}
           .describeForLog()),
      " [pin result with name \"myPin\" with geo index on ?geom]");

  // Pinned name, geo index, and simplification.
  EXPECT_EQ((ResultPinning{.pinResultWithName_ = "myPin",
                           .pinNamedGeoIndex_ = "geom",
                           .geoIndexSimplificationInMeters_ = 5.0}
                 .describeForLog()),
            " [pin result with name \"myPin\" with geo index on ?geom, "
            "simplification=5m]");

  // Everything combined.
  EXPECT_EQ((ResultPinning{.pinSubtrees_ = true,
                           .pinResult_ = true,
                           .pinResultWithName_ = "myPin",
                           .pinNamedGeoIndex_ = "geom",
                           .geoIndexSimplificationInMeters_ = 5.0}
                 .describeForLog()),
            " [pin result] [pin subresults] [pin result with name \"myPin\" "
            "with geo index on ?geom, simplification=5m]");
}

// _____________________________________________________________________________
TEST(HttpApiHelpersTest, determineResultPinning) {
  EXPECT_EQ(determineResultPinning(
                {{"pin-subresults", {"true"}}, {"pin-result", {"true"}}}),
            ResultPinning(true, true));
  EXPECT_EQ(determineResultPinning({{"pin-result", {"true"}}}),
            ResultPinning(false, true));
  EXPECT_EQ(determineResultPinning({{"pin-subresults", {"otherValue"}}}),
            ResultPinning(false, false));

  // `pin-result-with-name` is parsed into `pinResultWithName_`.
  EXPECT_EQ(determineResultPinning({{"pin-result-with-name", {"myPin"}}}),
            (ResultPinning{.pinResultWithName_ = "myPin"}));

  // `pin-geo-index-on-var` is parsed into `pinNamedGeoIndex_`.
  EXPECT_EQ(determineResultPinning({{"pin-result-with-name", {"myPin"}},
                                    {"pin-geo-index-on-var", {"geom"}}}),
            (ResultPinning{.pinResultWithName_ = "myPin",
                           .pinNamedGeoIndex_ = "geom"}));

  // `pin-geo-index-simplification` is parsed into
  // `geoIndexSimplificationInMeters_`.
  EXPECT_EQ(determineResultPinning({{"pin-result-with-name", {"myPin"}},
                                    {"pin-geo-index-on-var", {"geom"}},
                                    {"pin-geo-index-simplification", {"5.0"}}}),
            (ResultPinning{.pinResultWithName_ = "myPin",
                           .pinNamedGeoIndex_ = "geom",
                           .geoIndexSimplificationInMeters_ = 5.0}));

  // An invalid `pin-geo-index-simplification` value throws.
  AD_EXPECT_THROW_WITH_MESSAGE(
      determineResultPinning(
          {{"pin-geo-index-simplification", {"not-a-number"}}}),
      ::testing::HasSubstr(
          "Invalid value for `pin-geo-index-simplification`: must be a "
          "floating-point number of meters."));
}

// _____________________________________________________________________________
TEST(HttpApiHelpersTest, parseSendLimit) {
  auto expectInvalidSendLimit = [](std::string value,
                                   ad_utility::source_location l =
                                       AD_CURRENT_SOURCE_LOC()) {
    auto trace = generateLocationTrace(l);
    AD_EXPECT_THROW_WITH_MESSAGE(
        parseSendLimit({{"send", {std::move(value)}}}),
        ::testing::HasSubstr(
            "Invalid value for `send`: must be a "
            "positive integer specifying the number of bindings "
            "to be exported."));
  };
  // No value given - no specific limit given.
  EXPECT_EQ(parseSendLimit({}), std::nullopt);
  // A valid positive number is parsed correctly.
  EXPECT_THAT(parseSendLimit({{"send", {"12"}}}), ::testing::Optional(12ul));
  // A non-numeric value throws.
  expectInvalidSendLimit("not-a-number");
  // A negative value throws.
  expectInvalidSendLimit("-1");
}

// _____________________________________________________________________________
TEST(HttpApiHelpersTest, considerSendParameter) {
  using enum ad_utility::MediaType;
  // Always considered for `qlever-results+json`.
  EXPECT_TRUE(considerSendParameter(qleverJson));

  // Considered for `sparql-results+json` if and only if the runtime
  // parameter `sparql-results-json-with-time` is set.
  {
    auto cleanup = setRuntimeParameterForTest<
        &RuntimeParameters::sparqlResultsJsonWithTime_>(true);
    EXPECT_TRUE(considerSendParameter(sparqlJson));
  }
  {
    auto cleanup = setRuntimeParameterForTest<
        &RuntimeParameters::sparqlResultsJsonWithTime_>(false);
    EXPECT_FALSE(considerSendParameter(sparqlJson));
  }

  // Not considered for other media types.
  EXPECT_FALSE(considerSendParameter(csv));
  EXPECT_FALSE(considerSendParameter(tsv));
}
