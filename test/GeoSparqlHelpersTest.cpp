// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures
// Authors: Hannah Bast <bast@cs.uni-freiburg.de,
//          Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include <gmock/gmock.h>
#include <util/geo/Geo.h>

#include <cmath>
#include <string>

#include "engine/SpatialJoinConfig.h"
#include "global/Constants.h"
#include "rdfTypes/GeoPoint.h"
#include "rdfTypes/GeoSparqlHelpers.h"
#include "rdfTypes/Iri.h"
#include "util/GTestHelpers.h"

namespace {

using ad_utility::source_location;
using ad_utility::WktDist;
using ad_utility::WktGeometricRelation;
using ad_utility::WktLatitude;
using ad_utility::WktLongitude;
using ad_utility::detail::parseWktPoint;

// _____________________________________________________________________________
TEST(GeoSparqlHelpers, ParseWktPoint) {
  // Test that the given WKT point parses correctly (with all three of
  // parseWktPoint, wktLatitude, and wktLongitude).
  auto testParseWktPointCorrect = [](const std::string& point,
                                     double expected_lng, double expected_lat) {
    auto [lng, lat] = parseWktPoint(point);
    ASSERT_DOUBLE_EQ(expected_lng, lng);
    ASSERT_DOUBLE_EQ(expected_lat, lat);
    ASSERT_DOUBLE_EQ(expected_lng, WktLongitude()(GeoPoint(lat, lng)));
    ASSERT_DOUBLE_EQ(expected_lat, WktLatitude()(GeoPoint(lat, lng)));
  };

  // Test that the given WKT point is invalid (with all three of
  // parseWktPoint, wktLongitude, and wktLatitude).
  auto testWktPointInvalid = [](const std::string& point) {
    auto [lat, lng] = parseWktPoint(point);
    ASSERT_TRUE(std::isnan(lng));
    ASSERT_TRUE(std::isnan(lat));
  };

  // Some valid WKT points, including those from the test for `wktDist` below.
  testParseWktPointCorrect("POINT(2.0 1.5)", 2.0, 1.5);
  testParseWktPointCorrect("POINT(2.0 -1.5)", 2.0, -1.5);
  testParseWktPointCorrect("PoInT(3   0.0)", 3.0, 0.0);
  testParseWktPointCorrect("pOiNt(7 -0.0)", 7.0, 0.0);
  testParseWktPointCorrect(" pOiNt\t(  7 \r -0.0 \n ) ", 7.0, 0.0);
  testParseWktPointCorrect("POINT(2.2945 48.8585)", 2.2945, 48.8585);
  testParseWktPointCorrect("POINT(2 48.8585)", 2.0, 48.8585);
  testParseWktPointCorrect("POINT(20 48.8585)", 20.0, 48.8585);
  testParseWktPointCorrect("POINT(7.8529 47.9957)", 7.8529, 47.9957);
  testParseWktPointCorrect("POINT(7.8529 47)", 7.8529, 47.0);
  testParseWktPointCorrect("POINT(17 47)", 17.0, 47.0);
  testParseWktPointCorrect("POINT(7 47)", 7.0, 47.0);

  // Invalid WKT points because of issues unrelated to the number format (one of
  // the quotes missing, one of the parentheses missing, it must be exactly two
  // coordinates).
  testWktPointInvalid("POINT42.0 7.8)");
  testWktPointInvalid("POINT(42.0 7.8");
  testWktPointInvalid("POINT(42.0)");
  testWktPointInvalid("POINT(42.0 7.8 3.14)");

  // Invalid WKT points because of issues related to the number format (dot must
  // have preceding integer part and succeeding decimal part, explicit plus sign
  // not allowed, scientific notation not allowed,
  testWktPointInvalid("POINT(42. 7.)");
  testWktPointInvalid("POINT(.42 .8)");
  testWktPointInvalid("POINT(+42.0 7.8)");
  testWktPointInvalid("POINT(42.0 +7.8)");
  testWktPointInvalid("POINT(42e3 7.8)");
}

// _____________________________________________________________________________
TEST(GeoSparqlHelpers, WktDist) {
  using enum UnitOfMeasurement;
  GeoPoint eiffeltower{48.8585, 2.2945};
  GeoPoint frCathedral{47.9957, 7.8529};

  // Equal coordinates: distance 0.
  EXPECT_NEAR(WktDist()(frCathedral, frCathedral, KILOMETERS), 0, 0.01);
  EXPECT_NEAR(WktDist()(eiffeltower, eiffeltower, METERS), 0, 0.01);
  EXPECT_NEAR(WktDist()(eiffeltower, eiffeltower, MILES), 0, 0.01);

  // Distance between points: the Eiffel tower and the Freiburg Cathedral (421km
  // according to the distance measurement of Google Maps).
  EXPECT_NEAR(WktDist()(eiffeltower, frCathedral), 421.57, 0.02);
  EXPECT_NEAR(WktDist()(eiffeltower, frCathedral, KILOMETERS), 421.57, 0.02);
  EXPECT_NEAR(WktDist()(eiffeltower, frCathedral, METERS), 421569, 15);
  EXPECT_NEAR(WktDist()(eiffeltower, frCathedral, MILES), 261.95, 0.02);
  EXPECT_NEAR(ad_utility::WktMetricDist()(eiffeltower, frCathedral), 421569,
              15);

  // Distance between WKT non-point literals.
  EXPECT_NEAR(
      WktDist()(
          // Line between Freiburg Central Station and Freiburg University
          // Library.
          "\"LINESTRING(7.8412948 47.9977308, 7.8450491 47.9946000)\""
          "^^<http://www.opengis.net/ont/geosparql#wktLiteral>",
          // University building 101.
          "\"POLYGON((7.8346338 48.0126612,7.8348921 48.0123905,7.8349457 "
          "48.0124216,7.8349855 48.0124448,7.8353244 48.0126418,7.8354091 "
          "48.0126911,7.8352246 48.0129047,7.8351623 48.012879,7.8350687 "
          "48.0128404,7.8347244 48.0126985,7.8346338 48.0126612))\""
          "^^<http://www.opengis.net/ont/geosparql#wktLiteral>",
          KILOMETERS),
      1.7, 0.01);

  // Invalid WKT literal.
  EXPECT_TRUE(std::isnan(
      WktDist()(eiffeltower,
                // University building 101.
                "\"POLYGON(bla bli blu)\""
                "^^<http://www.opengis.net/ont/geosparql#wktLiteral>")));
}

// _____________________________________________________________________________
template <SpatialJoinType::Enum SJType>
void checkGeoRelationDummyImpl(
    source_location sourceLocation = AD_CURRENT_SOURCE_LOC()) {
  auto l = generateLocationTrace(sourceLocation);
  const auto geoRelationFunction = WktGeometricRelation<SJType>();
  AD_EXPECT_THROW_WITH_MESSAGE(
      geoRelationFunction(GeoPoint{1, 1}, GeoPoint{2, 2}),
      ::testing::HasSubstr(
          "currently only implemented for a subset of all possible queries"));
}

// _____________________________________________________________________________
TEST(GeoSparqlHelpers, WktGeometricRelation) {
  // Currently the geometric relation functions are only a dummy implementation
  using enum SpatialJoinType::Enum;
  checkGeoRelationDummyImpl<INTERSECTS>();
  checkGeoRelationDummyImpl<CONTAINS>();
  checkGeoRelationDummyImpl<COVERS>();
  checkGeoRelationDummyImpl<CROSSES>();
  checkGeoRelationDummyImpl<TOUCHES>();
  checkGeoRelationDummyImpl<EQUALS>();
  checkGeoRelationDummyImpl<OVERLAPS>();
  checkGeoRelationDummyImpl<WITHIN>();
}

// _____________________________________________________________________________
TEST(GeoSparqlHelpers, WktDe9imRelation) {
  // The `geof:relate` function is currently only a dummy implementation.
  AD_EXPECT_THROW_WITH_MESSAGE(
      ad_utility::WktDe9imRelation()(GeoPoint{1, 1}, GeoPoint{2, 2},
                                     std::string{"T*T***T**"}),
      ::testing::HasSubstr(
          "currently only implemented for a subset of all possible queries"));
}

// Test that the geometry type and the CRS of a WKT literal are returned as
// `xsd:anyURI` literals, and `UNDEF` for a missing type or a type without IRI.
TEST(GeoSparqlHelpers, WktTypeIri) {
  using namespace ad_utility::triple_component;
  using ad_utility::GeometryType;
  using ad_utility::SourceCrsType;
  using ad_utility::WktCrsType;
  using ad_utility::WktGeometryType;
  auto anyUri = [](std::string_view iri) {
    auto lit = Literal::literalWithoutQuotes(iri);
    lit.addDatatype(Iri::fromIrirefWithoutBrackets(XSD_ANYURI_TYPE));
    return sparqlExpression::IdOrLiteralOrIri{LiteralOrIri{std::move(lit)}};
  };
  sparqlExpression::IdOrLiteralOrIri undef = ValueId::makeUndefined();

  // Types with an IRI.
  EXPECT_EQ(WktGeometryType{}(GeometryType{util::geo::WKTType::POINT}),
            anyUri("http://www.opengis.net/ont/sf#Point"));
  EXPECT_EQ(WktCrsType{}(SourceCrsType{util::geo::CRSType::WGS84}),
            anyUri("http://www.opengis.net/def/crs/EPSG/0/4326"));

  // No type, or a type without IRI (only 0 to 7 and 0 to 3 are valid).
  EXPECT_EQ(WktGeometryType{}(std::nullopt), undef);
  EXPECT_EQ(WktCrsType{}(std::nullopt), undef);
  EXPECT_EQ(WktGeometryType{}(GeometryType{8}), undef);
  EXPECT_EQ(WktCrsType{}(SourceCrsType{4}), undef);
}

// Test that a `GeoPoint` or a WKT literal is projected to the CRS specified by
// an IRI and returned as a new WKT literal with the transformed coordinates.
// Also test that an unsupported CRS IRI yields `UNDEF`.
TEST(GeoSparqlHelpers, WktTransform) {
  using namespace ad_utility::triple_component;
  using ad_utility::GeoPointOrWkt;
  using ad_utility::WktTransform;
  auto anyWkt = [](std::string_view wkt) {
    auto lit = Literal::literalWithoutQuotes(wkt);
    lit.addDatatype(ad_utility::detail::wktLiteralIri);
    return sparqlExpression::IdOrLiteralOrIri{LiteralOrIri{std::move(lit)}};
  };
  // From `GeoPoint` to other CRS.
  GeoPoint examplePoint{2, 1};
  EXPECT_EQ(
      WktTransform{}(GeoPointOrWkt{examplePoint}, util::geo::CRSType::CRS84),
      anyWkt("POINT(1 2)"));
  EXPECT_EQ(
      WktTransform{}(GeoPointOrWkt{examplePoint}, util::geo::CRSType::WGS84),
      anyWkt("<http://www.opengis.net/def/crs/EPSG/0/4326> POINT(2 1)"));
  EXPECT_EQ(WktTransform{}(GeoPointOrWkt{examplePoint},
                           util::geo::CRSType::WEB_MERCATOR),
            anyWkt("<http://www.opengis.net/def/crs/EPSG/0/3857> "
                   "POINT(111319.490793 222684.208506)"));

  // From `std::string` to other CRS.
  // CRS84:
  std::string exampleWkt{
      "\"POINT(1 2)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>"};
  EXPECT_EQ(
      WktTransform{}(GeoPointOrWkt{exampleWkt}, util::geo::CRSType::CRS84),
      anyWkt("POINT(1 2)"));
  EXPECT_EQ(
      WktTransform{}(GeoPointOrWkt{exampleWkt}, util::geo::CRSType::WGS84),
      anyWkt("<http://www.opengis.net/def/crs/EPSG/0/4326> POINT(2 1)"));
  EXPECT_EQ(WktTransform{}(GeoPointOrWkt{exampleWkt},
                           util::geo::CRSType::WEB_MERCATOR),
            anyWkt("<http://www.opengis.net/def/crs/EPSG/0/3857> "
                   "POINT(111319.490793 222684.208506)"));
  // WGS84:
  std::string exampleWktWgs84{
      "\"<http://www.opengis.net/def/crs/EPSG/0/4326> POINT(2 "
      "1)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>"};
  EXPECT_EQ(
      WktTransform{}(GeoPointOrWkt{exampleWktWgs84}, util::geo::CRSType::CRS84),
      anyWkt("POINT(1 2)"));
  EXPECT_EQ(
      WktTransform{}(GeoPointOrWkt{exampleWktWgs84}, util::geo::CRSType::WGS84),
      anyWkt("<http://www.opengis.net/def/crs/EPSG/0/4326> POINT(2 1)"));
  EXPECT_EQ(WktTransform{}(GeoPointOrWkt{exampleWktWgs84},
                           util::geo::CRSType::WEB_MERCATOR),
            anyWkt("<http://www.opengis.net/def/crs/EPSG/0/3857> "
                   "POINT(111319.490793 222684.208506)"));
  // WEB_MERCATOR:
  std::string exampleWktWebMerc{
      "\"<http://www.opengis.net/def/crs/EPSG/0/3857> POINT(111319.490793 "
      "222684.208506)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>"};
  EXPECT_EQ(WktTransform{}(GeoPointOrWkt{exampleWktWebMerc},
                           util::geo::CRSType::CRS84),
            anyWkt("POINT(1 2)"));
  EXPECT_EQ(WktTransform{}(GeoPointOrWkt{exampleWktWebMerc},
                           util::geo::CRSType::WGS84),
            anyWkt("<http://www.opengis.net/def/crs/EPSG/0/4326> POINT(2 1)"));
  EXPECT_EQ(WktTransform{}(GeoPointOrWkt{exampleWktWebMerc},
                           util::geo::CRSType::WEB_MERCATOR),
            anyWkt("<http://www.opengis.net/def/crs/EPSG/0/3857> "
                   "POINT(111319.490793 222684.208506)"));

  sparqlExpression::IdOrLiteralOrIri undef = ValueId::makeUndefined();
  // Test invalid WKT.
  std::string invalidWkt{
      "\"BLABLIBLU(xyz)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>"};
  EXPECT_EQ(
      WktTransform{}(GeoPointOrWkt{invalidWkt}, util::geo::CRSType::WGS84),
      undef);

  // Test unsupported target CRS.
  EXPECT_EQ(WktTransform{}(GeoPointOrWkt{examplePoint},
                           util::geo::CRSType::UNSUPPORTED),
            undef);

  // Test unsupported source CRS (the projection fails during parsing).
  std::string unsupportedSourceCrs{
      "\"<http://www.opengis.net/def/crs/EPSG/0/25832> POINT(500000 "
      "5000000)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>"};
  EXPECT_EQ(WktTransform{}(GeoPointOrWkt{unsupportedSourceCrs},
                           util::geo::CRSType::WGS84),
            undef);

  // Test that a geometry beyond the latitude range of Web Mercator (about
  // 85.05 degrees) cannot be projected to Web Mercator, but to WGS84.
  GeoPoint northPole{90, 0};
  std::string polygonBeyondRange{
      "\"POLYGON((0 80, 10 80, 10 -86, 0 80))\""
      "^^<http://www.opengis.net/ont/geosparql#wktLiteral>"};
  EXPECT_EQ(WktTransform{}(GeoPointOrWkt{northPole},
                           util::geo::CRSType::WEB_MERCATOR),
            undef);
  EXPECT_EQ(WktTransform{}(GeoPointOrWkt{polygonBeyondRange},
                           util::geo::CRSType::WEB_MERCATOR),
            undef);
  EXPECT_EQ(WktTransform{}(GeoPointOrWkt{northPole}, util::geo::CRSType::WGS84),
            anyWkt("<http://www.opengis.net/def/crs/EPSG/0/4326> POINT(90 0)"));
  EXPECT_NE(WktTransform{}(GeoPointOrWkt{GeoPoint{85, 0}},
                           util::geo::CRSType::WEB_MERCATOR),
            undef);

  // Test UNDEF from `wkt` without value.
  EXPECT_EQ(WktTransform{}(std::nullopt, util::geo::CRSType::WGS84), undef);
}

// Test that the centroid and the envelope of a geometry are returned in the
// CRS of the geometry: for CRS84 as a `GeoPoint` (the centroid) or a plain WKT
// literal (the envelope), otherwise as a WKT literal with the CRS IRI.
TEST(GeoSparqlHelpers, WktCentroidAndEnvelopeInSourceCrs) {
  using namespace ad_utility::triple_component;
  using ad_utility::BoundingBox;
  using ad_utility::Centroid;
  using ad_utility::SourceCrsType;
  using ad_utility::WithSourceCrs;
  using ad_utility::WktCentroid;
  using ad_utility::WktEnvelope;
  using util::geo::CRSType;
  auto wktLit = [](std::string_view wkt) {
    auto lit = Literal::literalWithoutQuotes(wkt);
    lit.addDatatype(Iri::fromIrirefWithoutBrackets(GEO_WKT_LITERAL));
    return sparqlExpression::IdOrLiteralOrIri{LiteralOrIri{std::move(lit)}};
  };
  sparqlExpression::IdOrLiteralOrIri undef = ValueId::makeUndefined();
  Centroid centroid{50, 60};
  BoundingBox box{{1, 2}, {3, 4}};

  // The centroid (latitude 50, longitude 60) in each supported CRS.
  EXPECT_EQ(WktCentroid{}(WithSourceCrs<Centroid>{
                centroid, SourceCrsType{CRSType::CRS84}}),
            sparqlExpression::IdOrLiteralOrIri{
                ValueId::makeFromGeoPoint(GeoPoint{50, 60})});
  EXPECT_EQ(
      WktCentroid{}(
          WithSourceCrs<Centroid>{centroid, SourceCrsType{CRSType::WGS84}}),
      wktLit("<http://www.opengis.net/def/crs/EPSG/0/4326> POINT(50 60)"));
  EXPECT_EQ(WktCentroid{}(WithSourceCrs<Centroid>{
                centroid, SourceCrsType{CRSType::WEB_MERCATOR}}),
            wktLit("<http://www.opengis.net/def/crs/EPSG/0/3857> "
                   "POINT(6679169.447596 6446275.841017)"));
  EXPECT_EQ(WktCentroid{}(std::nullopt), undef);

  // The envelope (latitudes 1 to 3, longitudes 2 to 4) in CRS84 and WGS84.
  EXPECT_EQ(WktEnvelope{}(
                WithSourceCrs<BoundingBox>{box, SourceCrsType{CRSType::CRS84}}),
            wktLit("POLYGON((2 1,4 1,4 3,2 3,2 1))"));
  EXPECT_EQ(WktEnvelope{}(
                WithSourceCrs<BoundingBox>{box, SourceCrsType{CRSType::WGS84}}),
            wktLit("<http://www.opengis.net/def/crs/EPSG/0/4326> "
                   "POLYGON((1 2,1 4,3 4,3 2,1 2))"));
  EXPECT_EQ(WktEnvelope{}(std::nullopt), undef);
}

}  // namespace
