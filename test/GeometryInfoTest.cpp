//  Copyright 2025, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include <absl/strings/str_join.h>
#include <gmock/gmock.h>
#include <util/geo/Geo.h>

#include <range/v3/numeric/accumulate.hpp>
#include <stdexcept>

#include "GeometryInfoTestHelpers.h"
#include "rdfTypes/GeometryInfo.h"
#include "rdfTypes/GeometryInfoHelpersImpl.h"
#include "util/GTestHelpers.h"
#include "util/GeoConverters.h"
#include "util/geo/Collection.h"
#include "util/geo/Geo.h"

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

constexpr std::string_view litShortRealWorldLine =
    "\"LINESTRING(7.8412948 47.9977308, 7.8450491 47.9946000)\""
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

constexpr std::array<uint32_t, 7> allTestLiteralNumGeometries{1, 1, 1, 2,
                                                              2, 2, 3};

// ____________________________________________________________________________
TEST(GeometryInfoTest, BasicTests) {
  // Constructor and getters
  GeometryInfo g{5,   {{1, 1}, {2, 2}},  {1.5, 1.5},
                 {2}, MetricLength{900}, MetricArea{5}};
  ASSERT_EQ(g.getWktType().type(), 5);
  ASSERT_NEAR(g.getCentroid().centroid().getLat(), 1.5, 0.0001);
  ASSERT_NEAR(g.getCentroid().centroid().getLng(), 1.5, 0.0001);
  auto [lowerLeft, upperRight] = g.getBoundingBox().pair();
  ASSERT_NEAR(lowerLeft.getLat(), 1, 0.0001);
  ASSERT_NEAR(lowerLeft.getLng(), 1, 0.0001);
  ASSERT_NEAR(upperRight.getLat(), 2, 0.0001);
  ASSERT_NEAR(upperRight.getLng(), 2, 0.0001);
  ASSERT_EQ(g.getNumGeometries().numGeometries(), 2);
  ASSERT_NEAR(g.getMetricLength().length(), 900, 0.0001);
  ASSERT_NEAR(g.getMetricArea().area(), 5, 0.0001);

  // Too large wkt type value
  AD_EXPECT_THROW_WITH_MESSAGE(
      GeometryInfo(120, {{1, 1}, {2, 2}}, {1.5, 1.5}, {1}, MetricLength{1},
                   MetricArea{5}),
      ::testing::HasSubstr("WKT Type out of range"));

  // Wrong bounding box point ordering
  AD_EXPECT_THROW_WITH_MESSAGE(
      GeometryInfo(1, {{2, 2}, {1, 1}}, {1.5, 1.5}, {1}, MetricLength{1},
                   MetricArea{0}),
      ::testing::HasSubstr("Bounding box coordinates invalid"));

  // Zero geometries
  AD_EXPECT_THROW_WITH_MESSAGE(
      GeometryInfo(1, {{2, 2}, {3, 3}}, {1.5, 1.5}, {0}, MetricLength{1},
                   MetricArea{5}),
      ::testing::HasSubstr("Number of geometries must be strictly positive"));

  // Negative length
  AD_EXPECT_THROW_WITH_MESSAGE(
      GeometryInfo(5, {{1, 1}, {2, 2}}, {1.5, 1.5}, {1}, MetricLength{-900},
                   MetricArea{5}),
      ::testing::HasSubstr("Metric length must be positive"));

  // Negative area
  AD_EXPECT_THROW_WITH_MESSAGE(
      GeometryInfo(5, {{1, 1}, {2, 2}}, {1.5, 1.5}, {1}, MetricLength{0},
                   MetricArea{-900}),
      ::testing::HasSubstr("Metric area must be positive"));
}

// ____________________________________________________________________________
TEST(GeometryInfoTest, FromWktLiteral) {
  // To avoid hard-coding lengths for unit tests unrelated to actual length
  // computation, we compute the expected values.
  auto len = getLengthForTesting;
  auto area = getAreaForTesting;

  auto g = GeometryInfo::fromWktLiteral(litPoint);
  GeometryInfo exp{1,   {{4, 3}, {4, 3}}, {4, 3},
                   {1}, MetricLength{0},  MetricArea{0}};
  EXPECT_GEOMETRYINFO(g, exp);

  auto g2 = GeometryInfo::fromWktLiteral(litLineString);
  GeometryInfo exp2{2,   {{2, 2}, {4, 4}},   {3, 3},
                    {1}, len(litLineString), MetricArea{0}};
  EXPECT_GEOMETRYINFO(g2, exp2);

  auto g3 = GeometryInfo::fromWktLiteral(litPolygon);
  GeometryInfo exp3{3,   {{2, 2}, {4, 4}}, {3, 3},
                    {1}, len(litPolygon),  area(litPolygon)};
  EXPECT_GEOMETRYINFO(g3, exp3);

  auto g4 = GeometryInfo::fromWktLiteral(litMultiPoint);
  GeometryInfo exp4{4,   {{2, 2}, {4, 4}}, {3, 3},
                    {2}, MetricLength{0},  MetricArea{0}};
  EXPECT_GEOMETRYINFO(g4, exp4);

  auto g5 = GeometryInfo::fromWktLiteral(litMultiLineString);
  GeometryInfo exp5{5,   {{2, 2}, {8, 6}},        {4.436542, 3.718271},
                    {2}, len(litMultiLineString), MetricArea{0}};
  EXPECT_GEOMETRYINFO(g5, exp5);

  auto g6 = GeometryInfo::fromWktLiteral(litMultiPolygon);
  GeometryInfo exp6{6,   {{2, 2}, {6, 8}},     {4.5, 4.5},
                    {2}, len(litMultiPolygon), area(litMultiPolygon)};
  EXPECT_GEOMETRYINFO(g6, exp6);

  auto g7 = GeometryInfo::fromWktLiteral(litCollection);
  GeometryInfo exp7{7,   {{2, 2}, {6, 8}},   {5, 5},
                    {3}, len(litCollection), area(litCollection)};
  EXPECT_GEOMETRYINFO(g7, exp7);

  auto g8 = GeometryInfo::fromWktLiteral(litInvalidType);
  EXPECT_GEOMETRYINFO(g8, std::nullopt);
}

// ____________________________________________________________________________
TEST(GeometryInfoTest, FromGeoPoint) {
  GeoPoint p{1.234, 5.678};
  auto g = GeometryInfo::fromGeoPoint(p);
  GeometryInfo exp{1, {p, p}, Centroid{p}, {1}, MetricLength{0}, MetricArea{0}};
  EXPECT_GEOMETRYINFO(g, exp);

  GeoPoint p2{0, 0};
  auto g2 = GeometryInfo::fromGeoPoint(p2);
  GeometryInfo exp2{1,   {p2, p2},        Centroid{p2},
                    {1}, MetricLength{0}, MetricArea{0}};
  EXPECT_GEOMETRYINFO(g2, exp2);
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
  EXPECT_CENTROID_NEAR(centroid1, centroidExp1);

  auto bb1 = boundingBoxAsGeoPoints(parsed1);
  BoundingBox bbExp1{{4, 3}, {4, 3}};
  EXPECT_BOUNDINGBOX_NEAR(bb1, bbExp1);

  auto bb1Wkt =
      boundingBoxAsWkt(bb1.value().lowerLeft(), bb1.value().upperRight());
  EXPECT_EQ(bb1Wkt, "POLYGON((3 4,3 4,3 4,3 4,3 4))");

  EXPECT_EQ(addSfPrefix<example>(), "http://www.opengis.net/ont/sf#Example");
  EXPECT_FALSE(wktTypeToIri(0).has_value());
  EXPECT_FALSE(wktTypeToIri(8).has_value());
  EXPECT_TRUE(wktTypeToIri(1).has_value());
  EXPECT_EQ(wktTypeToIri(1).value(), "http://www.opengis.net/ont/sf#Point");

  EXPECT_EQ(countChildGeometries(parsed1), 1);

  EXPECT_EQ(computeMetricLength(parsed1).length(), 0);
  auto parseRes2 = parseWkt(litShortRealWorldLine);
  EXPECT_EQ(parseRes2.first, 2);
  ASSERT_TRUE(parseRes2.second.has_value());
  auto parsed2 = parseRes2.second.value();
  EXPECT_NEAR(computeMetricLength(parsed2).length(), 446.363, 1);
  EXPECT_EQ(GeometryInfo::getMetricLength(litInvalidType), std::nullopt);

  EXPECT_EQ(computeMetricArea(ParsedWkt{DPoint{4, 5}}), 0);
  EXPECT_EQ(computeMetricArea(ParsedWkt{DLine{DPoint{1, 2}, DPoint{3, 4}}}), 0);
}

// ____________________________________________________________________________
TEST(GeometryInfoTest, MetricLength) {
  MetricLength m1{5};
  EXPECT_EQ(m1.length(), 5);

  AD_EXPECT_THROW_WITH_MESSAGE(
      MetricLength{-500},
      ::testing::HasSubstr("Metric length must be positive"));
}

// ____________________________________________________________________________
TEST(GeometryInfoTest, ComputeMetricAreaPolygon) {
  using namespace ad_utility::detail;
  using Poly = Polygon<CoordType>;

  testMetricArea<Poly>(litSmallRealWorldPolygon1, areaSmallRealWorldPolygon1);
  testMetricArea<Poly>(litSmallRealWorldPolygon2, areaSmallRealWorldPolygon2);
  testMetricArea<Poly>(litSmallRealWorldPolygonWithHole,
                       areaSmallRealWorldPolygonWithHole);
}

// ____________________________________________________________________________
TEST(GeometryInfoTest, ComputeMetricAreaMultipolygon) {
  using namespace ad_utility::detail;
  using MultiP = MultiPolygon<CoordType>;

  testMetricArea<MultiP>(litRealWorldMultiPolygonFullyContained,
                         areaRealWorldMultiPolygonFullyContained);
  testMetricArea<MultiP>(litRealWorldMultiPolygonNonIntersecting,
                         areaRealWorldMultiPolygonNonIntersecting);
  testMetricArea<MultiP>(litRealWorldMultiPolygonIntersecting,
                         areaRealWorldMultiPolygonIntersecting);
  testMetricArea<MultiP>(litRealWorldMultiPolygonHoleIntersection,
                         areaRealWorldMultiPolygonHoleIntersection);

  // Edge case: empty multipolygon
  EXPECT_EQ(computeMetricArea(MultiP{}), 0);

  // Edge case: multipolygon with only one member
  testMetricArea<MultiP>(litSmallRealWorldPolygon2AsMulti,
                         areaSmallRealWorldPolygon2);
}

// ____________________________________________________________________________
TEST(GeometryInfoTest, ComputeMetricAreaCollection) {
  using namespace ad_utility::detail;
  using Coll = Collection<CoordType>;

  // Join two polygons and a line (no area) to a geometry collection literal
  auto collection1 = absl::StrCat(
      "GEOMETRYCOLLECTION(", removeDatatype(litSmallRealWorldPolygon1), ", ",
      removeDatatype(litLineString), ", ",
      removeDatatype(litSmallRealWorldPolygon2), ")");
  const double expectedCollection1 =
      areaSmallRealWorldPolygon1 + areaSmallRealWorldPolygon2;
  testMetricArea<Coll>(addDatatype(collection1), expectedCollection1, 2);

  // Collection with only one member (polygon)
  auto collection2 = absl::StrCat(
      "GEOMETRYCOLLECTION(", removeDatatype(litSmallRealWorldPolygon1), ")");
  testMetricArea<Coll>(addDatatype(collection2), areaSmallRealWorldPolygon1, 1);

  // Collection with only one member (non-polygon)
  auto collection3 =
      absl::StrCat("GEOMETRYCOLLECTION(", removeDatatype(litLineString), ")");
  testMetricArea<Coll>(addDatatype(collection3), 0, 0);

  // Collection containing a multipolygon and a point to be ignored
  auto collection4 =
      absl::StrCat("GEOMETRYCOLLECTION(",
                   removeDatatype(litRealWorldMultiPolygonIntersecting),
                   ", POINT(1.0 2.0))");
  testMetricArea<Coll>(addDatatype(collection4),
                       areaRealWorldMultiPolygonIntersecting,
                       numRealWorldMultiPolygonIntersecting);

  // Collection containing a nested collection and a further point
  auto collection5 =
      absl::StrCat("GEOMETRYCOLLECTION(POINT(3 4),", collection4, ")");
  testMetricArea<Coll>(addDatatype(collection5),
                       areaRealWorldMultiPolygonIntersecting,
                       numRealWorldMultiPolygonIntersecting);

  // The same case of a nested collection but the collection is not flattened
  // during parsing
  {
    Coll inner = getGeometryOfTypeOrThrow<Coll>(addDatatype(collection4));
    Coll outer{DAnyGeometry{DPoint{3, 4}}, DAnyGeometry{inner}};
    auto expected = areaRealWorldMultiPolygonIntersecting;
    EXPECT_NEAR(computeMetricArea(outer), expected, 0.01 * expected);
  }
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

// _____________________________________________________________________________
TEST(GeometryInfoTest, AnyGeometryMember) {
  // Test that the enum we define corresponds to the geometry type identifiers
  // used by `libspatialjoin`.
  using namespace util::geo;
  using enum AnyGeometryMember;

  checkAnyGeometryMemberEnum({DPoint{}}, POINT);
  checkAnyGeometryMemberEnum({DLine{}}, LINE);
  checkAnyGeometryMemberEnum({DPolygon{}}, POLYGON);
  checkAnyGeometryMemberEnum({DMultiLine{}}, MULTILINE);
  checkAnyGeometryMemberEnum({DMultiPolygon{}}, MULTIPOLYGON);
  checkAnyGeometryMemberEnum({DMultiPoint{}}, MULTIPOINT);
  checkAnyGeometryMemberEnum({DCollection{}}, COLLECTION);
}

// _____________________________________________________________________________
TEST(GeometryInfoTest, ComputeMetricLengthCollectionAnyGeom) {
  // This test builds a big geometry collection containing one geometry of every
  // supported geometry type and feeds it to `computeMetricLength`.
  using namespace ad_utility::detail;

  double expected = 0.0;
  DCollection collection;

  for (const auto& lit : getAllTestLiterals()) {
    expected += getLengthForTesting(lit).length();

    auto parsed = parseWkt(lit);
    ASSERT_TRUE(parsed.second.has_value());
    std::visit(
        [&](const auto& value) -> void {
          collection.push_back(AnyGeometry<CoordType>{value});
        },
        parsed.second.value());
  }

  MetricLength result{computeMetricLength(collection)};
  EXPECT_METRICLENGTH_NEAR(MetricLength{expected}, result);
}

// _____________________________________________________________________________
TEST(GeometryInfoTest, SizeOfAndAlignmentBytes) {
  // These assertions check that we are not wasting space with alignment bytes
  // when serializing `GeometryInfo` objects.
  static_assert(sizeof(EncodedBoundingBox) == 16);
  static_assert(sizeof(GeometryType) == 1);
  static_assert(sizeof(MetricLength) == sizeof(double));
  static_assert(sizeof(NumGeometries) == 4);
  static_assert(sizeof(MetricArea) == sizeof(double));

  using EncodedGeometryTypeAndCentroid = uint64_t;
  static_assert(sizeof(GeometryInfo) ==
                4 +  // Currently we need 4 B alignment
                    sizeof(EncodedGeometryTypeAndCentroid) +
                    sizeof(EncodedBoundingBox) + sizeof(NumGeometries) +
                    sizeof(MetricLength) + sizeof(MetricArea));
}

}  // namespace
