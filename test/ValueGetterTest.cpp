//  Copyright 2025, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Authors: @DuDaAG,
//           Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "../test/printers/UnitOfMeasurementPrinters.h"
#include "./ValueGetterTestHelpers.h"

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
  using ad_utility::GeometryInfo;
  GeoInfoTester t;
  t.checkFromLocalAndNormalVocabAndLiteral(
      "\"LINESTRING(2 2, 4 4)\""
      "^^<http://www.opengis.net/ont/geosparql#wktLiteral>",
      GeoInfoMatcher(GeometryInfo{2, {{2, 2}, {4, 4}}, {3, 3}}));
  t.checkFromLocalAndNormalVocabAndLiteral(
      "\"POLYGON((2 4, 4 4, 4 2, 2 2))\""
      "^^<http://www.opengis.net/ont/geosparql#wktLiteral>",
      GeoInfoMatcher(GeometryInfo{3, {{2, 2}, {4, 4}}, {3, 3}}));

  auto nullopt = GeoInfoMatcher(std::nullopt);
  t.checkFromLocalAndNormalVocabAndLiteral("\"someType\"^^<someType>", nullopt);
  t.checkFromLocalAndNormalVocabAndLiteral(
      "\"anXsdString\"^^<http://www.w3.org/2001/XMLSchema#string>", nullopt);
  t.checkFromLocalAndNormalVocabAndLiteral("\"noType\"", nullopt);
  t.checkFromLocalAndNormalVocabAndLiteral("<https://example.com/test>",
                                           nullopt);
}

// _____________________________________________________________________________
TEST(GeometryInfoValueGetterTest, OperatorWithIdGeoPoint) {
  using ad_utility::GeometryInfo;
  GeoInfoTester t;
  t.checkFromValueId(ValueId::makeFromGeoPoint({3, 2}),
                     GeoInfoMatcher(GeometryInfo{1, {{3, 2}, {3, 2}}, {3, 2}}));
}

// _____________________________________________________________________________
TEST(GeometryInfoValueGetterTest, OperatorWithUnrelatedId) {
  GeoInfoTester t;
  auto nullopt = GeoInfoMatcher(std::nullopt);
  t.checkFromValueId(ValueId::makeUndefined(), nullopt);
  t.checkFromValueId(ValueId::makeFromBool(true), nullopt);
  t.checkFromValueId(ValueId::makeFromInt(42), nullopt);
  t.checkFromValueId(ValueId::makeFromDouble(42.01), nullopt);
}

// _____________________________________________________________________________
TEST(GeoPointOrWktValueGetterTest, OperatorWithIdGeoPoint) {
  GeoPointOrWktTester t;

  GeoPoint p1{20, 30};
  auto p1id = ValueId::makeFromGeoPoint(p1);
  t.checkFromValueId(p1id, GeoPointOrWktMatcher(p1));

  GeoPoint p2{0, 0};
  auto p2id = ValueId::makeFromGeoPoint(p2);
  t.checkFromValueId(p2id, GeoPointOrWktMatcher(p2));

  auto nullopt = GeoPointOrWktMatcher(std::nullopt);
  t.checkFromValueId(ValueId::makeUndefined(), nullopt);
  t.checkFromValueId(ValueId::makeFromBool(true), nullopt);
  t.checkFromValueId(ValueId::makeFromInt(42), nullopt);
  t.checkFromValueId(ValueId::makeFromDouble(42.01), nullopt);
}

// _____________________________________________________________________________
TEST(GeoPointOrWktValueGetterTest, OperatorWithLit) {
  checkGeoPointOrWktFromLocalAndNormalVocabAndLiteralForValid(
      "\"LINESTRING(2 2, 4 4)\""
      "^^<http://www.opengis.net/ont/geosparql#wktLiteral>");
  checkGeoPointOrWktFromLocalAndNormalVocabAndLiteralForValid(
      "\"POLYGON(2 4, 4 4, 4 2, 2 2)\""
      "^^<http://www.opengis.net/ont/geosparql#wktLiteral>");

  GeoPointOrWktTester t;
  auto nullopt = GeoPointOrWktMatcher(std::nullopt);
  t.checkFromLocalAndNormalVocabAndLiteral("\"someType\"^^<someType>", nullopt);
  t.checkFromLocalAndNormalVocabAndLiteral(
      "\"anXsdString\"^^<http://www.w3.org/2001/XMLSchema#string>", nullopt);
  t.checkFromLocalAndNormalVocabAndLiteral("\"noType\"", nullopt);
  t.checkFromLocalAndNormalVocabAndLiteral("<https://example.com/test>",
                                           nullopt);
}

};  // namespace
