//  Copyright 2025, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include <gmock/gmock.h>

#include <stdexcept>

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

// University building 101 in Freiburg: ca. 1611 square-meters (osmway:33903391)
constexpr std::string_view litSmallRealWorldPolygon1 =
    "\"POLYGON((7.8346338 48.0126612,7.8348921 48.0123905,7.8349457 "
    "48.0124216,7.8349855 48.0124448,7.8353244 48.0126418,7.8354091 "
    "48.0126911,7.8352246 48.0129047,7.8351623 48.012879,7.8350687 "
    "48.0128404,7.8347244 48.0126985,7.8346338 48.0126612))\""
    "^^<http://www.opengis.net/ont/geosparql#wktLiteral>";
const double areaSmallRealWorldPolygon1 = 1611.0;

// University building 106 in Freiburg: ca. 491 square-meters (osmway:33903567)
constexpr std::string_view litSmallRealWorldPolygon2 =
    "\"POLYGON((7.8333378 48.0146547,7.8334932 48.0144793,7.833657 "
    "48.0145439,7.8336726 48.01455,7.8336875 48.0145564,7.8337433 "
    "48.0145785,7.8335879 48.0147539,7.8335143 48.0147242,7.8333378 "
    "48.0146547))\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>";
const double areaSmallRealWorldPolygon2 = 491.0;
constexpr std::string_view litSmallRealWorldPolygon2AsMulti =
    "\"MULTIPOLYGON(((7.8333378 48.0146547,7.8334932 48.0144793,7.833657 "
    "48.0145439,7.8336726 48.01455,7.8336875 48.0145564,7.8337433 "
    "48.0145785,7.8335879 48.0147539,7.8335143 48.0147242,7.8333378 "
    "48.0146547)))\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>";

// The outer boundary of this polygon is a triangle between Freiburg Central
// Railway Station, Freiburg University Library and Freiburg Cathedral, ca.
// 117122 square-meters. It has a hole consisting of a smaller triangle ca.
// 15103 square-meters. Therefore the area is 117122 - 15103 = 102019 sq.-m.
constexpr std::string_view litSmallRealWorldPolygonWithHole =
    "\"POLYGON((7.8412948 47.9977308, 7.8450491 47.9946, 7.852918  47.995562, "
    "7.8412948 47.9977308),(7.847796 47.995486, 7.844982 47.995615, 7.8447057 "
    "47.9969221))\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>";
const double areaSmallRealWorldPolygonWithHole = 102019.0;

// This example multipolygon uses the same coordinates as the one with a hole
// above. Therefore the union of them is just the first polygon with size ca.
// 117122 square meters.
constexpr std::string_view litRealWorldMultiPolygonFullyContained =
    "\"MULTIPOLYGON(((7.8412948 47.9977308, 7.8450491 47.9946, 7.852918 "
    "47.995562, 7.8412948 47.9977308)),((7.847796 47.995486, 7.844982 "
    "47.995615, 7.8447057 47.9969221)))\""
    "^^<http://www.opengis.net/ont/geosparql#wktLiteral>";
const double areaRealWorldMultiPolygonFullyContained = 117122.0;

// This multipolygon contains two non-intersecting polygons (university
// buildings 101 and 106 in Freiburg), thus its size is the sum of the member
// polygons, that is ca. 1611 + 491 = 2102 square meters.
constexpr std::string_view litRealWorldMultiPolygonNonIntersecting =
    "\"MULTIPOLYGON(((7.8346338 48.0126612,7.8348921 48.0123905,7.8349457 "
    "48.0124216,7.8349855 48.0124448,7.8353244 48.0126418,7.8354091 "
    "48.0126911,7.8352246 48.0129047,7.8351623 48.012879,7.8350687 "
    "48.0128404,7.8347244 48.0126985,7.8346338 48.0126612)),((7.8333378 "
    "48.0146547,7.8334932 48.0144793,7.833657 48.0145439,7.8336726 "
    "48.01455,7.8336875 48.0145564,7.8337433 48.0145785,7.8335879 "
    "48.0147539,7.8335143 48.0147242,7.8333378 48.0146547)))\""
    "^^<http://www.opengis.net/ont/geosparql#wktLiteral>";
const double areaRealWorldMultiPolygonNonIntersecting = 2102.0;

// Two polygons which intersect each other. Their sizes are ca. 117122 and 18962
// square meters, so their union must be smaller than the sum of these (which
// would be 136084 square meters): it is ca. 119319 square meters.
constexpr std::string_view litRealWorldMultiPolygonIntersecting =
    "\"MULTIPOLYGON(((7.847796 47.995486, 7.844982 47.995615,  7.844529 "
    "47.995205, 7.844933 47.994211)),((7.8412948 47.9977308, 7.8450491 "
    "47.9946, 7.852918 47.995562, 7.8412948 47.9977308)))\""
    "^^<http://www.opengis.net/ont/geosparql#wktLiteral>";
const double areaRealWorldMultiPolygonIntersecting = 119319.0;
const size_t numRealWorldMultiPolygonIntersecting = 2;

// Two polygons which intersect each other. This is equivalent to the one above
// but contains an additional ca. 15103 square meter hole which has to be
// considered correctly during the computation of the polygons' union for
// determining the area.
constexpr std::string_view litRealWorldMultiPolygonHoleIntersection =
    "\"MULTIPOLYGON(((7.8412948 47.9977308, 7.8450491 47.9946, 7.852918 "
    "47.995562, 7.8412948 47.9977308),(7.847796 47.995486, 7.844982 47.995615, "
    "7.8447057 47.9969221)),((7.847796 47.995486, 7.844529 47.995205, 7.844933 "
    "47.994211)))\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>";
const double areaRealWorldMultiPolygonHoleIntersection =
    areaRealWorldMultiPolygonIntersecting - 15103.0;

const auto getAllTestLiterals = []() {
  return std::vector<std::string_view>{
      litPoint,           litLineString,   litPolygon,   litMultiPoint,
      litMultiLineString, litMultiPolygon, litCollection};
};

// ____________________________________________________________________________
TEST(GeometryInfoTest, BasicTests) {
  // Constructor and getters
  GeometryInfo g{5, {{1, 1}, {2, 2}}, {1.5, 1.5}, {5}};
  ASSERT_EQ(g.getWktType().type(), 5);
  ASSERT_NEAR(g.getCentroid().centroid().getLat(), 1.5, 0.0001);
  ASSERT_NEAR(g.getCentroid().centroid().getLng(), 1.5, 0.0001);
  auto [lowerLeft, upperRight] = g.getBoundingBox().pair();
  ASSERT_NEAR(lowerLeft.getLat(), 1, 0.0001);
  ASSERT_NEAR(lowerLeft.getLng(), 1, 0.0001);
  ASSERT_NEAR(upperRight.getLat(), 2, 0.0001);
  ASSERT_NEAR(upperRight.getLng(), 2, 0.0001);
  ASSERT_NEAR(g.getMetricArea().area(), 5, 0.0001);

  // Too large wkt type value
  AD_EXPECT_THROW_WITH_MESSAGE(
      GeometryInfo(120, {{1, 1}, {2, 2}}, {1.5, 1.5}, {5}),
      ::testing::HasSubstr("WKT Type out of range"));

  // Wrong bounding box point ordering
  AD_EXPECT_THROW_WITH_MESSAGE(
      GeometryInfo(1, {{2, 2}, {1, 1}}, {1.5, 1.5}, {0}),
      ::testing::HasSubstr("Bounding box coordinates invalid"));

  // Negative area
  AD_EXPECT_THROW_WITH_MESSAGE(
      GeometryInfo(5, {{1, 1}, {2, 2}}, {1.5, 1.5}, {-900}),
      ::testing::HasSubstr("Metric area must be positive"));
}

// ____________________________________________________________________________
TEST(GeometryInfoTest, FromWktLiteral) {
  auto area = getAreaForTesting;

  auto g = GeometryInfo::fromWktLiteral(litPoint);
  GeometryInfo exp{1, {{4, 3}, {4, 3}}, {4, 3}, {0}};
  checkGeoInfo(g, exp);

  auto g2 = GeometryInfo::fromWktLiteral(litLineString);
  GeometryInfo exp2{2, {{2, 2}, {4, 4}}, {3, 3}, {0}};
  checkGeoInfo(g2, exp2);

  auto g3 = GeometryInfo::fromWktLiteral(litPolygon);
  GeometryInfo exp3{3, {{2, 2}, {4, 4}}, {3, 3}, area(litPolygon)};
  checkGeoInfo(g3, exp3);

  auto g4 = GeometryInfo::fromWktLiteral(litMultiPoint);
  GeometryInfo exp4{4, {{2, 2}, {4, 4}}, {3, 3}, {0}};
  checkGeoInfo(g4, exp4);

  auto g5 = GeometryInfo::fromWktLiteral(litMultiLineString);
  GeometryInfo exp5{5, {{2, 2}, {8, 6}}, {4.436542, 3.718271}, {0}};
  checkGeoInfo(g5, exp5);

  auto g6 = GeometryInfo::fromWktLiteral(litMultiPolygon);
  GeometryInfo exp6{6, {{2, 2}, {6, 8}}, {4.5, 4.5}, area(litMultiPolygon)};
  checkGeoInfo(g6, exp6);

  auto g7 = GeometryInfo::fromWktLiteral(litCollection);
  GeometryInfo exp7{7, {{2, 2}, {6, 8}}, {5, 5}, area(litCollection)};
  checkGeoInfo(g7, exp7);

  auto g8 = GeometryInfo::fromWktLiteral(litInvalidType);
  checkGeoInfo(g8, std::nullopt);
}

// ____________________________________________________________________________
TEST(GeometryInfoTest, FromGeoPoint) {
  GeoPoint p{1.234, 5.678};
  auto g = GeometryInfo::fromGeoPoint(p);
  GeometryInfo exp{1, {p, p}, Centroid{p}, {0}};
  checkGeoInfo(g, exp);

  GeoPoint p2{0, 0};
  auto g2 = GeometryInfo::fromGeoPoint(p2);
  GeometryInfo exp2{1, {p2, p2}, Centroid{p2}, {0}};
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

  EXPECT_EQ(computeMetricArea(ParsedWkt{DPoint{4, 5}}), 0);
  EXPECT_EQ(computeMetricArea(ParsedWkt{DLine{DPoint{1, 2}, DPoint{3, 4}}}), 0);
}

// ____________________________________________________________________________
TEST(GeometryInfoTest, ComputeMetricAreaPolygon) {
  testPolygonArea(litSmallRealWorldPolygon1, areaSmallRealWorldPolygon1);
  testPolygonArea(litSmallRealWorldPolygon2, areaSmallRealWorldPolygon2);
  testPolygonArea(litSmallRealWorldPolygonWithHole,
                  areaSmallRealWorldPolygonWithHole);
}

// ____________________________________________________________________________
TEST(GeometryInfoTest, ComputeMetricAreaMultipolygon) {
  using namespace ad_utility::detail;
  testMultiPolygonArea(litRealWorldMultiPolygonFullyContained,
                       areaRealWorldMultiPolygonFullyContained);
  testMultiPolygonArea(litRealWorldMultiPolygonNonIntersecting,
                       areaRealWorldMultiPolygonNonIntersecting);
  testMultiPolygonArea(litRealWorldMultiPolygonIntersecting,
                       areaRealWorldMultiPolygonIntersecting);
  testMultiPolygonArea(litRealWorldMultiPolygonHoleIntersection,
                       areaRealWorldMultiPolygonHoleIntersection);

  // Edge case: empty multipolygon
  EXPECT_EQ(computeMetricAreaMultiPolygon(MultiPolygon<CoordType>{}), 0);

  // Edge case: multipolygon with only one member
  testMultiPolygonArea(litSmallRealWorldPolygon2AsMulti,
                       areaSmallRealWorldPolygon2);
}

// ____________________________________________________________________________
TEST(GeometryInfoTest, ComputeMetricAreaCollection) {
  using namespace ad_utility::detail;

  // Join two polygons and a line (no area) to a geometry collection literal
  auto collection1 = absl::StrCat(
      "GEOMETRYCOLLECTION(", removeDatatype(litSmallRealWorldPolygon1), ", ",
      removeDatatype(litLineString), ", ",
      removeDatatype(litSmallRealWorldPolygon2), ")");
  const double expectedCollection1 =
      areaSmallRealWorldPolygon1 + areaSmallRealWorldPolygon2;
  testCollectionArea(addDatatype(collection1), 2, expectedCollection1);

  // Collection with only one member (polygon)
  auto collection2 = absl::StrCat(
      "GEOMETRYCOLLECTION(", removeDatatype(litSmallRealWorldPolygon1), ")");
  testCollectionArea(addDatatype(collection2), 1, areaSmallRealWorldPolygon1);

  // Collection with only one member (non-polygon)
  auto collection3 =
      absl::StrCat("GEOMETRYCOLLECTION(", removeDatatype(litLineString), ")");
  testCollectionArea(addDatatype(collection3), 0, 0);

  // Collection containing a multipolygon
  auto collection4 =
      absl::StrCat("GEOMETRYCOLLECTION(",
                   removeDatatype(litRealWorldMultiPolygonIntersecting), ")");
  testCollectionArea(addDatatype(collection4),
                     numRealWorldMultiPolygonIntersecting,
                     areaRealWorldMultiPolygonIntersecting);

  // Collection containing a nested collection
  auto collection5 = absl::StrCat("GEOMETRYCOLLECTION(", collection4, ")");
  testCollectionArea(addDatatype(collection5),
                     numRealWorldMultiPolygonIntersecting,
                     areaRealWorldMultiPolygonIntersecting);
}

// ____________________________________________________________________________
TEST(GeometryInfoTest, MetricArea) {
  MetricArea a1{500};
  EXPECT_EQ(a1.area(), 500);

  AD_EXPECT_THROW_WITH_MESSAGE(
      MetricArea{-1}, ::testing::HasSubstr("Metric area must be positive"));
}

// ____________________________________________________________________________
TEST(GeometryInfoTest, InvalidLiteralAdHocCompuation) {
  checkInvalidLiteral(litInvalidType);
  checkInvalidLiteral(litInvalidBrackets, true);
  checkInvalidLiteral(litInvalidNumCoords, true);
}

// ____________________________________________________________________________
TEST(GeometryInfoTest, CoordinateOutOfRangeDoesNotThrow) {
  checkInvalidLiteral(litCoordOutOfRange, true);
  EXPECT_EQ(GeometryInfo::getWktType(litCoordOutOfRange).value(),
            std::optional<GeometryType>{GeometryType{2}});
  EXPECT_EQ(GeometryInfo::getRequestedInfo<GeometryType>(litCoordOutOfRange),
            std::optional<GeometryType>{GeometryType{2}});
}

// _____________________________________________________________________________
TEST(GeometryInfoTest, WebMercProjection) {
  util::geo::DBox b1{{1, 2}, {3, 4}};
  auto b1WebMerc = boxToWebMerc(b1);
  auto result1 =
      ad_utility::detail::projectInt32WebMercToDoubleLatLng(b1WebMerc);
  checkUtilBoundingBox(result1, b1);
}

}  // namespace
