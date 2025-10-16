//  Copyright 2025, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include <gmock/gmock.h>
#include <util/geo/Geo.h>

#include "GeometryInfoTestHelpers.h"
#include "rdfTypes/GeometryInfo.h"
#include "util/GTestHelpers.h"

namespace {

using namespace ad_utility;
using namespace geoInfoTestHelpers;

// Example WKT literals for all supported geometry types
constexpr std::string_view litPoint =
    "\"POINT(3 4)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>";
constexpr std::string_view litLineString =
    "\"LINESTRING(2 2, 4 4)\""
    "^^<http://www.opengis.net/ont/geosparql#wktLiteral>";
constexpr std::string_view litPolygon =
    "\"POLYGON((2 4, 4 4, 4 2, 2 2))\""
    "^^<http://www.opengis.net/ont/geosparql#wktLiteral>";
constexpr std::string_view litMultiPoint =
    "\"MULTIPOINT((2 2), (4 4))\""
    "^^<http://www.opengis.net/ont/geosparql#wktLiteral>";
constexpr std::string_view litMultiLineString =
    "\"MULTILINESTRING((2 2, 4 4), (2 2, 6 8))\""
    "^^<http://www.opengis.net/ont/geosparql#wktLiteral>";
constexpr std::string_view litMultiPolygon =
    "\"MULTIPOLYGON(((2 4,8 4,8 6,2 6,2 4)), ((2 4, 4 4, 4 2, 2 2)))\""
    "^^<http://www.opengis.net/ont/geosparql#wktLiteral>";
constexpr std::string_view litCollection =
    "\"GEOMETRYCOLLECTION(POLYGON((2 4,8 4,8 6,2 6,2 4)), LINESTRING(2 2, 4 4),"
    "POINT(3 4))\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>";

constexpr std::string_view litInvalidType =
    "\"BLABLIBLU(xyz)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>";
constexpr std::string_view litInvalidBrackets =
    "\"POLYGON)2 4, 4 4, 4 2, 2 2(\""
    "^^<http://www.opengis.net/ont/geosparql#wktLiteral>";
constexpr std::string_view litInvalidNumCoords =
    "\"POINT(1)\""
    "^^<http://www.opengis.net/ont/geosparql#wktLiteral>";
constexpr std::string_view litCoordOutOfRange =
    "\"LINESTRING(2 -500, 4 4)\""
    "^^<http://www.opengis.net/ont/geosparql#wktLiteral>";

const auto getAllTestLiterals = []() {
  return std::vector<std::string_view>{
      litPoint,           litLineString,   litPolygon,   litMultiPoint,
      litMultiLineString, litMultiPolygon, litCollection};
};

constexpr std::array<uint32_t, 7> allTestLiteralNumGeometries{1, 1, 1, 2,
                                                              2, 2, 3};

// ____________________________________________________________________________
TEST(GeometryInfoTest, BasicTests) {
  // Constructor and getters
  GeometryInfo g{5, {{1, 1}, {2, 2}}, {1.5, 1.5}, {2}};
  ASSERT_EQ(g.getWktType().type(), 5);
  ASSERT_NEAR(g.getCentroid().centroid().getLat(), 1.5, 0.0001);
  ASSERT_NEAR(g.getCentroid().centroid().getLng(), 1.5, 0.0001);
  auto [lowerLeft, upperRight] = g.getBoundingBox().pair();
  ASSERT_NEAR(lowerLeft.getLat(), 1, 0.0001);
  ASSERT_NEAR(lowerLeft.getLng(), 1, 0.0001);
  ASSERT_NEAR(upperRight.getLat(), 2, 0.0001);
  ASSERT_NEAR(upperRight.getLng(), 2, 0.0001);
  ASSERT_EQ(g.getNumGeometries().numGeometries(), 2);

  // Too large wkt type value
  AD_EXPECT_THROW_WITH_MESSAGE(
      GeometryInfo(120, {{1, 1}, {2, 2}}, {1.5, 1.5}, {1}),
      ::testing::HasSubstr("WKT Type out of range"));

  // Wrong bounding box point ordering
  AD_EXPECT_THROW_WITH_MESSAGE(
      GeometryInfo(1, {{2, 2}, {1, 1}}, {1.5, 1.5}, {1}),
      ::testing::HasSubstr("Bounding box coordinates invalid"));

  // Zero geometries
  AD_EXPECT_THROW_WITH_MESSAGE(
      GeometryInfo(1, {{2, 2}, {3, 3}}, {1.5, 1.5}, {0}),
      ::testing::HasSubstr("Number of geometries must be strictly positive"));
}

// ____________________________________________________________________________
TEST(GeometryInfoTest, FromWktLiteral) {
  auto g = GeometryInfo::fromWktLiteral(litPoint);
  GeometryInfo exp{1, {{4, 3}, {4, 3}}, {4, 3}, {1}};
  checkGeoInfo(g, exp);

  auto g2 = GeometryInfo::fromWktLiteral(litLineString);
  GeometryInfo exp2{2, {{2, 2}, {4, 4}}, {3, 3}, {1}};
  checkGeoInfo(g2, exp2);

  auto g3 = GeometryInfo::fromWktLiteral(litPolygon);
  GeometryInfo exp3{3, {{2, 2}, {4, 4}}, {3, 3}, {1}};
  checkGeoInfo(g3, exp3);

  auto g4 = GeometryInfo::fromWktLiteral(litMultiPoint);
  GeometryInfo exp4{4, {{2, 2}, {4, 4}}, {3, 3}, {2}};
  checkGeoInfo(g4, exp4);

  auto g5 = GeometryInfo::fromWktLiteral(litMultiLineString);
  GeometryInfo exp5{5, {{2, 2}, {8, 6}}, {4.436542, 3.718271}, {2}};
  checkGeoInfo(g5, exp5);

  auto g6 = GeometryInfo::fromWktLiteral(litMultiPolygon);
  GeometryInfo exp6{6, {{2, 2}, {6, 8}}, {4.5, 4.5}, {2}};
  checkGeoInfo(g6, exp6);

  auto g7 = GeometryInfo::fromWktLiteral(litCollection);
  GeometryInfo exp7{7, {{2, 2}, {6, 8}}, {5, 5}, {3}};
  checkGeoInfo(g7, exp7);

  auto g8 = GeometryInfo::fromWktLiteral(litInvalidType);
  checkGeoInfo(g8, std::nullopt);
}

// ____________________________________________________________________________
TEST(GeometryInfoTest, FromGeoPoint) {
  GeoPoint p{1.234, 5.678};
  auto g = GeometryInfo::fromGeoPoint(p);
  GeometryInfo exp{1, {p, p}, Centroid{p}, {1}};
  checkGeoInfo(g, exp);

  GeoPoint p2{0, 0};
  auto g2 = GeometryInfo::fromGeoPoint(p2);
  GeometryInfo exp2{1, {p2, p2}, Centroid{p2}, {1}};
  checkGeoInfo(g2, exp2);
}

// ____________________________________________________________________________
TEST(GeometryInfoTest, RequestedInfoInstance) {
  for (auto lit : getAllTestLiterals()) {
    checkRequestedInfoForInstance(GeometryInfo::fromWktLiteral(lit));
  }
}

// ____________________________________________________________________________
TEST(GeometryInfoTest, RequestedInfoLiteral) {
  for (auto lit : getAllTestLiterals()) {
    checkRequestedInfoForWktLiteral(lit);
  }
}

// ____________________________________________________________________________
TEST(GeometryInfoTest, BoundingBoxAsWKT) {
  BoundingBox bb1{{0, 0}, {1, 1}};
  ASSERT_EQ(bb1.asWkt(), "POLYGON((0 0,1 0,1 1,0 1,0 0))");
  BoundingBox bb2{{0, 0}, {0, 0}};
  ASSERT_EQ(bb2.asWkt(), "POLYGON((0 0,0 0,0 0,0 0,0 0))");
  auto bb3 = GeometryInfo::getBoundingBox(
      "\"LINESTRING(2 4,8 8)\""
      "^^<http://www.opengis.net/ont/geosparql#wktLiteral>");
  ASSERT_TRUE(bb3.has_value());
  ASSERT_EQ(bb3.value().asWkt(), "POLYGON((2 4,8 4,8 8,2 8,2 4))");
}

// ____________________________________________________________________________
TEST(GeometryInfoTest, BoundingBoxGetBoundingCoordinate) {
  using enum ad_utility::BoundingCoordinate;

  BoundingBox bb1{{2, 1}, {4, 3}};
  EXPECT_NEAR(bb1.getBoundingCoordinate<MIN_X>(), 1, 0.0001);
  EXPECT_NEAR(bb1.getBoundingCoordinate<MIN_Y>(), 2, 0.0001);
  EXPECT_NEAR(bb1.getBoundingCoordinate<MAX_X>(), 3, 0.0001);
  EXPECT_NEAR(bb1.getBoundingCoordinate<MAX_Y>(), 4, 0.0001);

  BoundingBox bb2{{-20, -5}, {-4, -3}};
  EXPECT_NEAR(bb2.getBoundingCoordinate<MIN_X>(), -5, 0.0001);
  EXPECT_NEAR(bb2.getBoundingCoordinate<MIN_Y>(), -20, 0.0001);
  EXPECT_NEAR(bb2.getBoundingCoordinate<MAX_X>(), -3, 0.0001);
  EXPECT_NEAR(bb2.getBoundingCoordinate<MAX_Y>(), -4, 0.0001);
}

// ____________________________________________________________________________
TEST(GeometryInfoTest, GeometryTypeAsIri) {
  ASSERT_EQ(GeometryType{1}.asIri().value(),
            "http://www.opengis.net/ont/sf#Point");
  ASSERT_EQ(GeometryType{2}.asIri().value(),
            "http://www.opengis.net/ont/sf#LineString");
  ASSERT_EQ(GeometryType{3}.asIri().value(),
            "http://www.opengis.net/ont/sf#Polygon");
  ASSERT_EQ(GeometryType{4}.asIri().value(),
            "http://www.opengis.net/ont/sf#MultiPoint");
  ASSERT_EQ(GeometryType{5}.asIri().value(),
            "http://www.opengis.net/ont/sf#MultiLineString");
  ASSERT_EQ(GeometryType{6}.asIri().value(),
            "http://www.opengis.net/ont/sf#MultiPolygon");
  ASSERT_EQ(GeometryType{7}.asIri().value(),
            "http://www.opengis.net/ont/sf#GeometryCollection");
  ASSERT_FALSE(GeometryType{8}.asIri().has_value());
}

namespace {
constexpr std::string_view example = "Example";
}

// ____________________________________________________________________________
TEST(GeometryInfoTest, GeometryInfoHelpers) {
  using namespace ad_utility::detail;
  Point<double> p{50, 60};
  auto g = utilPointToGeoPoint(p);
  EXPECT_NEAR(g.getLng(), p.getX(), 0.0001);
  EXPECT_NEAR(g.getLat(), p.getY(), 0.0001);

  auto p2 = geoPointToUtilPoint(g);
  EXPECT_NEAR(g.getLng(), p2.getX(), 0.0001);
  EXPECT_NEAR(g.getLat(), p2.getY(), 0.0001);

  EXPECT_EQ(removeDatatype(litPoint), "POINT(3 4)");

  auto parseRes1 = parseWkt(litPoint);
  EXPECT_EQ(parseRes1.first, util::geo::WKTType::POINT);
  ASSERT_TRUE(parseRes1.second.has_value());
  auto parsed1 = parseRes1.second.value();

  auto centroid1 = centroidAsGeoPoint(parsed1);
  Centroid centroidExp1{{4, 3}};
  checkCentroid(centroid1, centroidExp1);

  auto bb1 = boundingBoxAsGeoPoints(parsed1);
  BoundingBox bbExp1{{4, 3}, {4, 3}};
  checkBoundingBox(bb1, bbExp1);

  auto bb1Wkt =
      boundingBoxAsWkt(bb1.value().lowerLeft(), bb1.value().upperRight());
  EXPECT_EQ(bb1Wkt, "POLYGON((3 4,3 4,3 4,3 4,3 4))");

  EXPECT_EQ(addSfPrefix<example>(), "http://www.opengis.net/ont/sf#Example");
  EXPECT_FALSE(wktTypeToIri(0).has_value());
  EXPECT_FALSE(wktTypeToIri(8).has_value());
  EXPECT_TRUE(wktTypeToIri(1).has_value());
  EXPECT_EQ(wktTypeToIri(1).value(), "http://www.opengis.net/ont/sf#Point");

  EXPECT_EQ(countChildGeometries(parsed1), 1);
}

// ____________________________________________________________________________
TEST(GeometryInfoTest, InvalidLiteralAdHocCompuation) {
  checkInvalidLiteral(litInvalidType);
  checkInvalidLiteral(litInvalidBrackets, true);
  checkInvalidLiteral(litInvalidNumCoords, true);
}

// ____________________________________________________________________________
TEST(GeometryInfoTest, CoordinateOutOfRangeDoesNotThrow) {
  checkInvalidLiteral(litCoordOutOfRange, true, true);
  EXPECT_EQ(GeometryInfo::getWktType(litCoordOutOfRange).value(),
            std::optional<GeometryType>{GeometryType{2}});
  EXPECT_EQ(GeometryInfo::getRequestedInfo<GeometryType>(litCoordOutOfRange),
            std::optional<GeometryType>{GeometryType{2}});
  EXPECT_EQ(GeometryInfo::getRequestedInfo<NumGeometries>(litCoordOutOfRange),
            NumGeometries{1});
}

// _____________________________________________________________________________
TEST(GeometryInfoTest, WebMercProjection) {
  util::geo::DBox b1{{1, 2}, {3, 4}};
  auto b1WebMerc = boxToWebMerc(b1);
  auto result1 =
      ad_utility::detail::projectInt32WebMercToDoubleLatLng(b1WebMerc);
  checkUtilBoundingBox(result1, b1);
}

// _____________________________________________________________________________
TEST(GeometryInfoTest, NumGeometries) {
  const auto testLiterals = getAllTestLiterals();
  ASSERT_EQ(testLiterals.size(), allTestLiteralNumGeometries.size());

  for (size_t i = 0; i < testLiterals.size(); ++i) {
    const auto& lit = testLiterals[i];
    NumGeometries expected{allTestLiteralNumGeometries[i]};

    EXPECT_EQ(GeometryInfo::getNumGeometries(lit), expected);

    auto gi = GeometryInfo::fromWktLiteral(lit);
    ASSERT_TRUE(gi.has_value());
    EXPECT_EQ(gi.value().getNumGeometries(), expected);
  }
}

}  // namespace
