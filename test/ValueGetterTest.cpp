//  Copyright 2025, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Authors: @DuDaAG,
//           Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "../test/printers/UnitOfMeasurementPrinters.h"
#include "./ValueGetterTestHelpers.h"
#include "GeometryInfoTestHelpers.h"
#include "rdfTypes/GeometryInfo.h"

namespace {

using namespace valueGetterTestHelpers;
using namespace unitVGTestHelpers;
using namespace geoInfoVGTestHelpers;

// _____________________________________________________________________________
TEST(LiteralValueGetterWithStrFunction, OperatorWithId) {
  sparqlExpression::detail::LiteralValueGetterWithStrFunction
      literalValueGetter;
  checkLiteralContentAndDatatypeFromId("\"noType\"", "noType", std::nullopt,
                                       literalValueGetter);
  checkLiteralContentAndDatatypeFromId("\"someType\"^^<someType>", "someType",
                                       std::nullopt, literalValueGetter);
  checkLiteralContentAndDatatypeFromId(
      "\"anXsdString\"^^<http://www.w3.org/2001/XMLSchema#string>",
      "anXsdString", std::nullopt, literalValueGetter);
  checkLiteralContentAndDatatypeFromId("<x>", "x", std::nullopt,
                                       literalValueGetter);
}

// _____________________________________________________________________________
TEST(LiteralValueGetterWithStrFunction, OperatorWithLiteralOrIri) {
  using Iri = ad_utility::triple_component::Iri;
  sparqlExpression::detail::LiteralValueGetterWithStrFunction
      literalValueGetter;
  checkLiteralContentAndDatatypeFromLiteralOrIri("noType", std::nullopt, false,
                                                 "noType", std::nullopt,
                                                 literalValueGetter);
  checkLiteralContentAndDatatypeFromLiteralOrIri(
      "someType", Iri::fromIriref("<someType>"), false, "someType",
      std::nullopt, literalValueGetter);
  checkLiteralContentAndDatatypeFromLiteralOrIri(
      "anXsdString",
      Iri::fromIriref("<http://www.w3.org/2001/XMLSchema#string>"), false,
      "anXsdString", std::nullopt, literalValueGetter);
  checkLiteralContentAndDatatypeFromLiteralOrIri(
      "<x>", std::nullopt, true, "x", std::nullopt, literalValueGetter);
}

// _____________________________________________________________________________
TEST(LiteralValueGetterWithoutStrFunction, OperatorWithId) {
  sparqlExpression::detail::LiteralValueGetterWithoutStrFunction
      literalValueGetter;
  checkLiteralContentAndDatatypeFromId("\"noType\"", "noType", std::nullopt,
                                       literalValueGetter);
  checkLiteralContentAndDatatypeFromId("\"someType\"^^<someType>", std::nullopt,
                                       std::nullopt, literalValueGetter);
  checkLiteralContentAndDatatypeFromId(
      "\"anXsdString\"^^<http://www.w3.org/2001/XMLSchema#string>",
      "anXsdString", std::nullopt, literalValueGetter);
  checkLiteralContentAndDatatypeFromId("<x>", std::nullopt, std::nullopt,
                                       literalValueGetter);
}

// _____________________________________________________________________________
TEST(LiteralValueGetterWithoutStrFunction, OperatorWithLiteralOrIri) {
  using Iri = ad_utility::triple_component::Iri;
  sparqlExpression::detail::LiteralValueGetterWithoutStrFunction
      literalValueGetter;
  checkLiteralContentAndDatatypeFromLiteralOrIri("noType", std::nullopt, false,
                                                 "noType", std::nullopt,
                                                 literalValueGetter);
  checkLiteralContentAndDatatypeFromLiteralOrIri(
      "someType", Iri::fromIriref("<someType>"), false, std::nullopt,
      std::nullopt, literalValueGetter);
  checkLiteralContentAndDatatypeFromLiteralOrIri(
      "anXsdString",
      Iri::fromIriref("<http://www.w3.org/2001/XMLSchema#string>"), false,
      "anXsdString", std::nullopt, literalValueGetter);
  checkLiteralContentAndDatatypeFromLiteralOrIri("<x>", std::nullopt, true,
                                                 std::nullopt, std::nullopt,
                                                 literalValueGetter);
}

// _____________________________________________________________________________
TEST(UnitOfMeasurementValueGetter, OperatorWithId) {
  sparqlExpression::detail::UnitOfMeasurementValueGetter unitValueGetter;
  checkUnitValueGetterFromId("<http://qudt.org/vocab/unit/M>",
                             UnitOfMeasurement::METERS, unitValueGetter);
  checkUnitValueGetterFromId("<http://qudt.org/vocab/unit/KiloM>",
                             UnitOfMeasurement::KILOMETERS, unitValueGetter);
  checkUnitValueGetterFromId("<http://qudt.org/vocab/unit/MI>",
                             UnitOfMeasurement::MILES, unitValueGetter);
  checkUnitValueGetterFromId(
      "\"http://qudt.org/vocab/unit/M\"^^<http://www.w3.org/2001/"
      "XMLSchema#anyURI>",
      UnitOfMeasurement::METERS, unitValueGetter);
  checkUnitValueGetterFromId(
      "\"http://qudt.org/vocab/unit/KiloM\"^^<http://www.w3.org/2001/"
      "XMLSchema#anyURI>",
      UnitOfMeasurement::KILOMETERS, unitValueGetter);
  checkUnitValueGetterFromId(
      "\"http://qudt.org/vocab/unit/MI\"^^<http://www.w3.org/2001/"
      "XMLSchema#anyURI>",
      UnitOfMeasurement::MILES, unitValueGetter);

  checkUnitValueGetterFromId(
      "\"http://qudt.org/vocab/unit/example\"^^<http://www.w3.org/2001/"
      "XMLSchema#anyURI>",
      UnitOfMeasurement::UNKNOWN, unitValueGetter);

  checkUnitValueGetterFromId(
      "\"http://example.com\"^^<http://www.w3.org/2001/XMLSchema#anyURI>",
      UnitOfMeasurement::UNKNOWN, unitValueGetter);
  checkUnitValueGetterFromId("\"x\"", UnitOfMeasurement::UNKNOWN,
                             unitValueGetter);
  checkUnitValueGetterFromId("\"1.5\"^^<http://example.com>",
                             UnitOfMeasurement::UNKNOWN, unitValueGetter);
  checkUnitValueGetterFromId("\"http://qudt.org/vocab/unit/MI\"",
                             UnitOfMeasurement::UNKNOWN, unitValueGetter);
}

// _____________________________________________________________________________
TEST(UnitOfMeasurementValueGetter, OperatorWithIdSkipEncodedValue) {
  sparqlExpression::detail::UnitOfMeasurementValueGetter getter;
  checkUnitValueGetterFromIdEncodedValue(ValueId::makeFromBool(true), getter);
  checkUnitValueGetterFromIdEncodedValue(ValueId::makeFromBool(false), getter);
  checkUnitValueGetterFromIdEncodedValue(ValueId::makeFromInt(-50), getter);
  checkUnitValueGetterFromIdEncodedValue(ValueId::makeFromInt(1000), getter);
  checkUnitValueGetterFromIdEncodedValue(ValueId::makeFromDouble(1000.5),
                                         getter);
  checkUnitValueGetterFromIdEncodedValue(
      ValueId::makeFromGeoPoint(GeoPoint{20.0, 20.0}), getter);
}

// _____________________________________________________________________________
TEST(UnitOfMeasurementValueGetter, OperatorWithLiteralOrIri) {
  sparqlExpression::detail::UnitOfMeasurementValueGetter unitValueGetter;
  checkUnitValueGetterFromLiteralOrIri("http://qudt.org/vocab/unit/M",
                                       UnitOfMeasurement::METERS,
                                       unitValueGetter);
  checkUnitValueGetterFromLiteralOrIri("http://qudt.org/vocab/unit/MI",
                                       UnitOfMeasurement::MILES,
                                       unitValueGetter);
  checkUnitValueGetterFromLiteralOrIri("http://qudt.org/vocab/unit/KiloM",
                                       UnitOfMeasurement::KILOMETERS,
                                       unitValueGetter);

  checkUnitValueGetterFromLiteralOrIri("http://qudt.org/vocab/unit/m",
                                       UnitOfMeasurement::UNKNOWN,
                                       unitValueGetter);
  checkUnitValueGetterFromLiteralOrIri("http://qudt.org/vocab/unit/",
                                       UnitOfMeasurement::UNKNOWN,
                                       unitValueGetter);
  checkUnitValueGetterFromLiteralOrIri(
      "http://example.com/", UnitOfMeasurement::UNKNOWN, unitValueGetter);
  checkUnitValueGetterFromLiteralOrIri("", UnitOfMeasurement::UNKNOWN,
                                       unitValueGetter);
  checkUnitValueGetterFromLiteralOrIri("x", UnitOfMeasurement::UNKNOWN,
                                       unitValueGetter);
}

// _____________________________________________________________________________
TEST(GeometryInfoValueGetterTest, OperatorWithVocabIdOrLiteral) {
  GeoInfoTester t;
  auto noGeoInfo = geoInfoMatcher(std::nullopt);
  static constexpr std::string_view line =
      "\"LINESTRING(2 2, 4 4)\""
      "^^<http://www.opengis.net/ont/geosparql#wktLiteral>";
  t.checkFromLocalAndNormalVocabAndLiteral(
      std::string{line},
      geoInfoMatcher(ad_utility::GeometryInfo{2,
                                              {{2, 2}, {4, 4}},
                                              {3, 3},
                                              {1},
                                              getLengthForTesting(line),
                                              MetricArea{0}}));
  static constexpr std::string_view polygon =
      "\"POLYGON((2 4, 4 4, 4 2, 2 2))\""
      "^^<http://www.opengis.net/ont/geosparql#wktLiteral>";
  t.checkFromLocalAndNormalVocabAndLiteral(
      std::string{polygon},
      geoInfoMatcher(ad_utility::GeometryInfo{3,
                                              {{2, 2}, {4, 4}},
                                              {3, 3},
                                              {1},
                                              getLengthForTesting(polygon),
                                              getAreaForTesting(polygon)}));
  t.checkFromLocalAndNormalVocabAndLiteral("\"someType\"^^<someType>",
                                           noGeoInfo);
  t.checkFromLocalAndNormalVocabAndLiteral(
      "\"anXsdString\"^^<http://www.w3.org/2001/XMLSchema#string>", noGeoInfo);
  t.checkFromLocalAndNormalVocabAndLiteral("\"noType\"", noGeoInfo);
  t.checkFromLocalAndNormalVocabAndLiteral("<https://example.com/test>",
                                           noGeoInfo);
}

// _____________________________________________________________________________
TEST(GeometryInfoValueGetterTest, OperatorWithIdGeoPoint) {
  GeoInfoTester t;
  auto noGeoInfo = geoInfoMatcher(std::nullopt);
  t.checkFromValueId(ValueId::makeFromGeoPoint({3, 2}),
                     geoInfoMatcher(GeometryInfo{1,
                                                 {{3, 2}, {3, 2}},
                                                 {3, 2},
                                                 {1},
                                                 ad_utility::MetricLength{0},
                                                 MetricArea{0}}));
  t.checkFromValueId(ValueId::makeUndefined(), noGeoInfo);
  t.checkFromValueId(ValueId::makeFromBool(true), noGeoInfo);
  t.checkFromValueId(ValueId::makeFromInt(42), noGeoInfo);
  t.checkFromValueId(ValueId::makeFromDouble(42.01), noGeoInfo);
}

// _____________________________________________________________________________
TEST(GeometryInfoValueGetterTest, OperatorWithUnrelatedId) {
  GeoInfoTester t;
  auto noGeoInfo = geoInfoMatcher(std::nullopt);
  t.checkFromValueId(ValueId::makeUndefined(), noGeoInfo);
  t.checkFromValueId(ValueId::makeFromBool(true), noGeoInfo);
  t.checkFromValueId(ValueId::makeFromInt(42), noGeoInfo);
  t.checkFromValueId(ValueId::makeFromDouble(42.01), noGeoInfo);
}

// _____________________________________________________________________________
TEST(GeoPointOrWktValueGetterTest, OperatorWithIdGeoPoint) {
  GeoPointOrWktTester t;

  GeoPoint p1{20, 30};
  auto p1id = ValueId::makeFromGeoPoint(p1);
  t.checkFromValueId(p1id, geoPointOrWktMatcher(p1));

  GeoPoint p2{0, 0};
  auto p2id = ValueId::makeFromGeoPoint(p2);
  t.checkFromValueId(p2id, geoPointOrWktMatcher(p2));

  auto noGeoInfoOrWkt = geoPointOrWktMatcher(std::nullopt);
  t.checkFromValueId(ValueId::makeUndefined(), noGeoInfoOrWkt);
  t.checkFromValueId(ValueId::makeFromBool(true), noGeoInfoOrWkt);
  t.checkFromValueId(ValueId::makeFromInt(42), noGeoInfoOrWkt);
  t.checkFromValueId(ValueId::makeFromDouble(42.01), noGeoInfoOrWkt);
}

// _____________________________________________________________________________
TEST(GeoPointOrWktValueGetterTest, OperatorWithLit) {
  checkGeoPointOrWktFromLocalAndNormalVocabAndLiteralForValid(
      "\"LINESTRING(2 2, 4 4)\""
      "^^<http://www.opengis.net/ont/geosparql#wktLiteral>");
  checkGeoPointOrWktFromLocalAndNormalVocabAndLiteralForValid(
      "\"POLYGON((2 4, 4 4, 4 2, 2 2))\""
      "^^<http://www.opengis.net/ont/geosparql#wktLiteral>");

  GeoPointOrWktTester t;
  auto noGeoInfoOrWkt = geoPointOrWktMatcher(std::nullopt);
  t.checkFromLocalAndNormalVocabAndLiteral("\"someType\"^^<someType>",
                                           noGeoInfoOrWkt);
  t.checkFromLocalAndNormalVocabAndLiteral(
      "\"anXsdString\"^^<http://www.w3.org/2001/XMLSchema#string>",
      noGeoInfoOrWkt);
  t.checkFromLocalAndNormalVocabAndLiteral("\"noType\"", noGeoInfoOrWkt);
  t.checkFromLocalAndNormalVocabAndLiteral("<https://example.com/test>",
                                           noGeoInfoOrWkt);
}

// _____________________________________________________________________________
TEST(IntValueGetterTest, OperatorWithId) {
  IntValueGetterTester t;
  t.checkFromValueId(ValueId::makeFromInt(42), Eq(42));
  t.checkFromValueId(ValueId::makeFromInt(-500), Eq(-500));
  t.checkFromValueId(ValueId::makeFromBool(true), Eq(std::nullopt));
  t.checkFromValueId(ValueId::makeUndefined(), Eq(std::nullopt));
  t.checkFromValueId(ValueId::makeFromDouble(4.5), Eq(std::nullopt));
  t.checkFromValueId(ValueId::makeFromGeoPoint({3, 4}), Eq(std::nullopt));
}

// _____________________________________________________________________________
TEST(IntValueGetterTest, OperatorWithLit) {
  IntValueGetterTester t;
  auto noInt = Eq(std::nullopt);
  t.checkFromLocalAndNormalVocabAndLiteral("\"someType\"^^<someType>", noInt);
  t.checkFromLocalAndNormalVocabAndLiteral(
      "\"anXsdString\"^^<http://www.w3.org/2001/XMLSchema#string>", noInt);
  t.checkFromLocalAndNormalVocabAndLiteral("\"noType\"", noInt);
  t.checkFromLocalAndNormalVocabAndLiteral("<https://example.com/test>", noInt);
}

};  // namespace
