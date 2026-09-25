//  Copyright 2024, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Christoph Ullinger <ullingec@informatik.uni-freiburg.de>

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <gtest/gtest.h>

#include <vector>

#include "global/Constants.h"
#include "rdfTypes/GeoPoint.h"
#include "rdfTypes/GeoPointEncoding.h"
#include "rdfTypes/GeoSparqlHelpers.h"
#include "util/GTestHelpers.h"
#include "util/HashSet.h"

// _____________________________________________________________________________
TEST(GeoPoint, GeoPoint) {
  GeoPoint g = GeoPoint(70.5, 130.2);

  ASSERT_DOUBLE_EQ(g.getLat(), 70.5);
  ASSERT_DOUBLE_EQ(g.getLng(), 130.2);

  ASSERT_THROW(GeoPoint(-99.5, 1.0), CoordinateOutOfRangeException);
  ASSERT_THROW(GeoPoint(99.5, 1.0), CoordinateOutOfRangeException);
  ASSERT_THROW(GeoPoint(9.5, -185.0), CoordinateOutOfRangeException);
  ASSERT_THROW(GeoPoint(9.5, 185.0), CoordinateOutOfRangeException);
  ASSERT_THROW(GeoPoint(0, 185.0), CoordinateOutOfRangeException);
  ASSERT_THROW(GeoPoint(90.1, 180.0), CoordinateOutOfRangeException);
  ASSERT_THROW(GeoPoint(90.1, 180.0), CoordinateOutOfRangeException);
  ASSERT_THROW(GeoPoint(ad_utility::detail::invalidCoordinate, 20.0),
               CoordinateOutOfRangeException);
  ASSERT_THROW(GeoPoint(20.0, ad_utility::detail::invalidCoordinate),
               CoordinateOutOfRangeException);
  ASSERT_THROW(GeoPoint(ad_utility::detail::invalidCoordinate,
                        ad_utility::detail::invalidCoordinate),
               CoordinateOutOfRangeException);
  AD_EXPECT_THROW_WITH_MESSAGE(GeoPoint(100, 200),
                               ::testing::ContainsRegex("out of range"));

  ASSERT_NO_THROW(GeoPoint(0, 180));
  ASSERT_NO_THROW(GeoPoint(0, -180));
  ASSERT_NO_THROW(GeoPoint(90, 0));
  ASSERT_NO_THROW(GeoPoint(90, 180));
  ASSERT_NO_THROW(GeoPoint(90, -180));
  ASSERT_NO_THROW(GeoPoint(-90, 0));
  ASSERT_NO_THROW(GeoPoint(-90, 180));
  ASSERT_NO_THROW(GeoPoint(-90, -180));
  ASSERT_NO_THROW(GeoPoint(0, 1.0));
  ASSERT_NO_THROW(GeoPoint(1.0, -180.0));
  ASSERT_NO_THROW(GeoPoint(0, 0));

  g = GeoPoint(0, 0);
  ASSERT_DOUBLE_EQ(g.getLat(), 0);
  ASSERT_DOUBLE_EQ(g.getLng(), 0);
  g = GeoPoint(90, 180);
  ASSERT_DOUBLE_EQ(g.getLat(), 90);
  ASSERT_DOUBLE_EQ(g.getLng(), 180);
  g = GeoPoint(-90, -180);
  ASSERT_DOUBLE_EQ(g.getLat(), -90);
  ASSERT_DOUBLE_EQ(g.getLng(), -180);
  g = GeoPoint(-90, 180);
  ASSERT_DOUBLE_EQ(g.getLat(), -90);
  ASSERT_DOUBLE_EQ(g.getLng(), 180);
  g = GeoPoint(90, -180);
  ASSERT_DOUBLE_EQ(g.getLat(), 90);
  ASSERT_DOUBLE_EQ(g.getLng(), -180);
  g = GeoPoint(0, 180);
  ASSERT_DOUBLE_EQ(g.getLat(), 0);
  ASSERT_DOUBLE_EQ(g.getLng(), 180);
  g = GeoPoint(90, 0);
  ASSERT_DOUBLE_EQ(g.getLat(), 90);
  ASSERT_DOUBLE_EQ(g.getLng(), 0);
}

// _____________________________________________________________________________
TEST(GeoPoint, string) {
  GeoPoint g = GeoPoint(-70.5, -130.2);
  ASSERT_EQ(g.toStringRepresentation(), "POINT(-130.200000 -70.500000)");

  g = GeoPoint(90, 180);
  ASSERT_EQ(g.toStringRepresentation(), "POINT(180.000000 90.000000)");

  g = GeoPoint(0, 0);
  ASSERT_EQ(g.toStringRepresentation(), "POINT(0.000000 0.000000)");

  g = GeoPoint(-70.5, -130.2);
  auto strpair = g.toStringAndType();
  ASSERT_EQ(strpair.first, "POINT(-130.200000 -70.500000)");
  ASSERT_EQ(strpair.second, GEO_WKT_LITERAL);
}

// Test the bit representation of a point in the `ZOrder` encoding (for the
// `LatMajor` encoding, see the test `latMajorEncoding` below).
TEST(GeoPoint, bitRepresentation) {
  using T = GeoPoint::T;
  absl::Cleanup restoreEncoding{
      [encoding = GeoPoint::encoding()] { GeoPoint::setEncoding(encoding); }};
  GeoPoint::setEncoding(GeoPointEncodingEnum::ZOrder);

  // A point and its two quantized coordinates.
  GeoPoint g{-70.5, -130.2};
  const auto lat = static_cast<T>(
      std::round(((-70.5 + 90) / (2 * 90)) * GeoPoint::maxCoordinateEncoded));
  const auto lng = static_cast<T>(std::round(((-130.2 + 180) / (2 * 180)) *
                                             GeoPoint::maxCoordinateEncoded));

  // The two quantized coordinates are bit-interleaved, the latitude in the odd
  // bits.
  EXPECT_EQ(GeoPoint::deinterleaveCoordinates(g.toBitRepresentation()),
            std::pair(lat, lng));
  EXPECT_EQ(GeoPoint::interleaveCoordinates(lat, lng), g.toBitRepresentation());
  EXPECT_EQ(GeoPoint::interleaveCoordinates(1, 0), 0b10u);
  EXPECT_EQ(GeoPoint::interleaveCoordinates(0, 1), 0b01u);
  EXPECT_EQ(GeoPoint::interleaveCoordinates(0b11, 0b01), 0b1011u);

  // The corners of the coordinate space have all 60 bits set, no bit set, only
  // the longitude bits (the even positions), or only the latitude bits (the
  // odd positions). The upper 4 bits are never set (they hold the datatype of
  // an `Id`).
  constexpr T allBits = (T{1} << 60) - 1;
  constexpr T lngBits = 0x0555555555555555ULL;
  EXPECT_EQ(GeoPoint(90, 180).toBitRepresentation(), allBits);
  EXPECT_EQ(GeoPoint(-90, -180).toBitRepresentation(), 0u);
  EXPECT_EQ(GeoPoint(-90, 180).toBitRepresentation(), lngBits);
  EXPECT_EQ(GeoPoint(90, -180).toBitRepresentation(), lngBits << 1);

  // Decoding gives the point back up to the precision of the quantization, and
  // exactly for a corner of the coordinate space.
  g = GeoPoint::fromBitRepresentation(
      GeoPoint::interleaveCoordinates(lat, lng));
  EXPECT_NEAR(g.getLat(), -70.5, 1e-5);
  EXPECT_NEAR(g.getLng(), -130.2, 1e-5);
  g = GeoPoint::fromBitRepresentation(0);
  EXPECT_DOUBLE_EQ(g.getLat(), -90);
  EXPECT_DOUBLE_EQ(g.getLng(), -180);

  // The quantization is idempotent, so the round trip through the bits is
  // exact for every representable point.
  for (T bits : {T{0}, T{1}, T{12345678901ULL}, lngBits, allBits}) {
    EXPECT_EQ(GeoPoint::fromBitRepresentation(bits).toBitRepresentation(),
              bits);
  }
}

// _____________________________________________________________________________
TEST(GeoPoint, parseFromLiteral) {
  constexpr auto testParseFromLiteral = [](const char* input, bool hasVal,
                                           double lng = 0.0, double lat = 0.0) {
    std::string content = absl::StrCat(
        "\"", input, "\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>");
    auto value =
        ad_utility::triple_component::Literal::fromStringRepresentation(
            content);
    auto g = GeoPoint::parseFromLiteral(value);
    ASSERT_EQ(g.has_value(), hasVal);
    if (g.has_value()) {
      ASSERT_DOUBLE_EQ(g.value().getLat(), lat);
      ASSERT_DOUBLE_EQ(g.value().getLng(), lng);
    }
  };

  // Tests for Literals with wkt type
  testParseFromLiteral("POINT(24.3 26.8)", true, 24.3, 26.8);
  testParseFromLiteral("POINT (24.3   26.8)", true, 24.3, 26.8);
  testParseFromLiteral("point(24.3 26.8)", true, 24.3, 26.8);
  testParseFromLiteral(" pOiNt (24.3 26.8 )", true, 24.3, 26.8);
  testParseFromLiteral("POINT(0.3 -90.0)", true, 0.3, -90.0);
  testParseFromLiteral("POINT(-180.0 90.0)", true, -180, 90);
  testParseFromLiteral("POINT(0.0 0.0)", true);
  testParseFromLiteral(" POiNT ( 0.0  0.0 ) ", true);
  testParseFromLiteral("POLYGON(0.0 0.0, 1.1 1.1, 2.2 2.2)", false);
  testParseFromLiteral(
      "MULTIPOLYGON((0.0 0.0, 1.1 1.1, 2.2 2.2),(3.3 3.3, 4.4 4.4, 5.5 5.5))",
      false);
  testParseFromLiteral("POINT(24.326.8)", false);
  testParseFromLiteral("POINT(24.326.8)", false);
  testParseFromLiteral("POINT()", false);
  testParseFromLiteral("()", false);
  testParseFromLiteral("(2.3 5.6)", false);
  testParseFromLiteral("notapoint", false);
  testParseFromLiteral("", false);

  // Literals of different type
  ASSERT_FALSE(
      GeoPoint::parseFromLiteral(
          ad_utility::triple_component::Literal::fromStringRepresentation(
              "\"123\"^^xsd:integer"))
          .has_value());
  ASSERT_FALSE(
      GeoPoint::parseFromLiteral(
          ad_utility::triple_component::Literal::fromStringRepresentation(
              "\"hi\"@en"))
          .has_value());
}

// _____________________________________________________________________________
TEST(GeoPoint, equal) {
  auto g1 = GeoPoint(-70.5, -130.2);
  auto g2 = GeoPoint(90, 180);
  auto g3 = GeoPoint(-90, -180);
  auto g4 = GeoPoint(-90, 180);
  auto g5 = GeoPoint::fromBitRepresentation(0);
  auto g6 = GeoPoint(-70.5, -130.2);
  auto g7 = GeoPoint(0, 0);
  auto g8 = GeoPoint(0, 0);
  auto g9 = GeoPoint(-90, -180);

  std::vector<GeoPoint> f = {g1, g2, g3, g4, g7};
  for (size_t i = 0; i < f.size(); i++) {
    for (size_t j = 0; j < f.size(); j++) {
      if (i != j) {
        ASSERT_FALSE(f[i] == f[j]);
      }
    }
  }

  ASSERT_TRUE(g1 == g6);
  ASSERT_TRUE(g7 == g8);
  ASSERT_TRUE(g5 == g3);
  ASSERT_TRUE(g3 == g9);
}

// _____________________________________________________________________________
TEST(GeoPoint, Hashing) {
  GeoPoint g1{50.0, 50.0};
  GeoPoint g2{10.5, 80.5};
  ad_utility::HashSet<GeoPoint> set{g1, g2};
  EXPECT_THAT(set, ::testing::UnorderedElementsAre(g1, g2));
}

// _____________________________________________________________________________
TEST(GeoPoint, quantizeCoordinate) {
  using T = GeoPoint::T;
  constexpr T max = GeoPoint::maxCoordinateEncoded;
  EXPECT_EQ(GeoPoint::quantizeCoordinate(-90, 90), 0u);
  EXPECT_EQ(GeoPoint::quantizeCoordinate(90, 90), max);
  EXPECT_DOUBLE_EQ(GeoPoint::dequantizeCoordinate(0, 180), -180);
  EXPECT_DOUBLE_EQ(GeoPoint::dequantizeCoordinate(max, 180), 180);
  // The quantization is idempotent.
  for (T q : {T{0}, T{1}, T{123456789}, max}) {
    EXPECT_EQ(
        GeoPoint::quantizeCoordinate(GeoPoint::dequantizeCoordinate(q, 90), 90),
        q);
  }
  // Values out of range violate the preconditions.
  EXPECT_ANY_THROW(GeoPoint::quantizeCoordinate(-90.5, 90));
  EXPECT_ANY_THROW(GeoPoint::quantizeCoordinate(90.5, 90));
  EXPECT_ANY_THROW(GeoPoint::dequantizeCoordinate(max + 1, 90));
}

// Test the `lat-major` encoding of a point, and that `toBitRepresentation` and
// `fromBitRepresentation` use the encoding of the process.
TEST(GeoPoint, latMajorEncoding) {
  using T = GeoPoint::T;
  using E = GeoPointEncodingEnum;
  absl::Cleanup restoreEncoding{
      [encoding = GeoPoint::encoding()] { GeoPoint::setEncoding(encoding); }};

  // In `LatMajor`, the latitude is in the upper and the longitude in the lower
  // 30 bits, in `ZOrder`, the bits are interleaved. Splitting is the inverse
  // of combining.
  constexpr T lat = 0b101;
  constexpr T lng = 0b11;
  static_assert(GeoPoint::combineCoordinates(lat, lng, E::LatMajor) ==
                ((lat << 30) | lng));
  static_assert(GeoPoint::combineCoordinates(lat, lng, E::ZOrder) ==
                GeoPoint::interleaveCoordinates(lat, lng));
  for (auto encoding : {E::ZOrder, E::LatMajor}) {
    EXPECT_EQ(GeoPoint::splitCoordinates(
                  GeoPoint::combineCoordinates(lat, lng, encoding), encoding),
              std::pair(lat, lng));
  }

  // A point is encoded and decoded with the encoding of the process.
  GeoPoint point{48.0, 7.8};
  T latQuantized = GeoPoint::quantizeCoordinate(48.0, 90);
  T lngQuantized = GeoPoint::quantizeCoordinate(7.8, 180);
  for (auto encoding : {E::ZOrder, E::LatMajor}) {
    GeoPoint::setEncoding(encoding);
    T bits = point.toBitRepresentation();
    EXPECT_EQ(bits, GeoPoint::combineCoordinates(latQuantized, lngQuantized,
                                                 encoding));
    EXPECT_NEAR(GeoPoint::fromBitRepresentation(bits).getLat(), 48.0, 1e-6);
    EXPECT_NEAR(GeoPoint::fromBitRepresentation(bits).getLng(), 7.8, 1e-6);
  }
}

// Test the names of the two encodings in the configuration of an index and in
// the option of `qlever-index`.
TEST(GeoPoint, encodingNames) {
  using ad_utility::GeoPointEncoding;
  EXPECT_EQ(GeoPointEncoding::fromString("z-order"), GeoPointEncoding::ZOrder);
  EXPECT_EQ(GeoPointEncoding::fromString("lat-major"),
            GeoPointEncoding::LatMajor);
  EXPECT_ANY_THROW(GeoPointEncoding::fromString("lng-major"));
}
