//  Copyright 2025, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include <gmock/gmock.h>

#include "GeometryInfoTestHelpers.h"
#include "rdfTypes/GeometryInfo.h"
#include "rdfTypes/GeometryInfoHelpersImpl.h"
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

// TODO<#1951>
// constexpr std::string_view litInvalid =
//     "\"BLABLIBLU(xyz)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>";

const auto getAllTestLiterals = []() {
  return std::vector<std::string_view>{
      litPoint,           litLineString,   litPolygon,   litMultiPoint,
      litMultiLineString, litMultiPolygon, litCollection};
};

// ____________________________________________________________________________
TEST(GeometryInfoTest, BasicTests) {
  // Constructor and getters
  GeometryInfo g{5, {{1, 1}, {2, 2}}, {1.5, 1.5}};
  ASSERT_EQ(g.getWktType().type_, 5);
  ASSERT_NEAR(g.getCentroid().centroid_.getLat(), 1.5, 0.0001);
  ASSERT_NEAR(g.getCentroid().centroid_.getLng(), 1.5, 0.0001);
  auto [lowerLeft, upperRight] = g.getBoundingBox();
  ASSERT_NEAR(lowerLeft.getLat(), 1, 0.0001);
  ASSERT_NEAR(lowerLeft.getLng(), 1, 0.0001);
  ASSERT_NEAR(upperRight.getLat(), 2, 0.0001);
  ASSERT_NEAR(upperRight.getLng(), 2, 0.0001);

  // Too large wkt type value
  AD_EXPECT_THROW_WITH_MESSAGE(GeometryInfo(120, {{1, 1}, {2, 2}}, {1.5, 1.5}),
                               ::testing::HasSubstr("WKT Type out of range"));

  // Wrong bounding box point ordering
  AD_EXPECT_THROW_WITH_MESSAGE(
      GeometryInfo(1, {{2, 2}, {1, 1}}, {1.5, 1.5}),
      ::testing::HasSubstr("Bounding box coordinates invalid"));
}

// ____________________________________________________________________________
TEST(GeometryInfoTest, FromWktLiteral) {
  auto g = GeometryInfo::fromWktLiteral(litPoint);
  GeometryInfo exp{1, {{4, 3}, {4, 3}}, {4, 3}};
  checkGeoInfo(g, exp);

  auto g2 = GeometryInfo::fromWktLiteral(litLineString);
  GeometryInfo exp2{2, {{2, 2}, {4, 4}}, {3, 3}};
  checkGeoInfo(g2, exp2);

  auto g3 = GeometryInfo::fromWktLiteral(litPolygon);
  GeometryInfo exp3{3, {{2, 2}, {4, 4}}, {3, 3}};
  checkGeoInfo(g3, exp3);

  auto g4 = GeometryInfo::fromWktLiteral(litMultiPoint);
  GeometryInfo exp4{4, {{2, 2}, {4, 4}}, {3, 3}};
  checkGeoInfo(g4, exp4);

  auto g5 = GeometryInfo::fromWktLiteral(litMultiLineString);
  GeometryInfo exp5{5, {{2, 2}, {8, 6}}, {4.436542, 3.718271}};
  checkGeoInfo(g5, exp5);

  auto g6 = GeometryInfo::fromWktLiteral(litMultiPolygon);
  GeometryInfo exp6{6, {{2, 2}, {6, 8}}, {4.5, 4.5}};
  checkGeoInfo(g6, exp6);

  auto g7 = GeometryInfo::fromWktLiteral(litCollection);
  GeometryInfo exp7{7, {{2, 2}, {6, 8}}, {5, 5}};
  checkGeoInfo(g7, exp7);

  // TODO<#1951>
  // auto g8 = GeometryInfo::fromWktLiteral(litInvalid);
  // checkGeoInfo(g8, std::nullopt);
}

// ____________________________________________________________________________
TEST(GeometryInfoTest, FromGeoPoint) {
  GeoPoint p{1.234, 5.678};
  auto g = GeometryInfo::fromGeoPoint(p);
  GeometryInfo exp{1, {p, p}, p};
  checkGeoInfo(g, exp);

  GeoPoint p2{0, 0};
  auto g2 = GeometryInfo::fromGeoPoint(p2);
  GeometryInfo exp2{1, {p2, p2}, p2};
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
  ASSERT_EQ(bb3.asWkt(), "POLYGON((2 4,8 4,8 8,2 8,2 4))");
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
  ASSERT_TRUE(parseRes1.second.has_value());
  auto parsed1 = parseRes1.second.value();

  auto centroid1 = centroidAsGeoPoint(parsed1);
  checkCentroid(centroid1, {{4, 3}});

  auto bb1 = boundingBoxAsGeoPoints(parsed1);
  checkBoundingBox(bb1, {{4, 3}, {4, 3}});

  auto bb1Wkt = boundingBoxAsWkt(bb1.lowerLeft_, bb1.upperRight_);
  EXPECT_EQ(bb1Wkt, "POLYGON((3 4,3 4,3 4,3 4,3 4))");

  EXPECT_EQ(addSfPrefix<"Example">(), "http://www.opengis.net/ont/sf#Example");
  EXPECT_FALSE(wktTypeToIri(0).has_value());
  EXPECT_FALSE(wktTypeToIri(8).has_value());
  EXPECT_TRUE(wktTypeToIri(1).has_value());
  EXPECT_EQ(wktTypeToIri(1).value(), "http://www.opengis.net/ont/sf#Point");
}

}  // namespace
