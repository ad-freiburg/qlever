// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "util/UnitOfMeasurement.h"

#include "util/Algorithm.h"

namespace ad_utility::detail {

// ____________________________________________________________________________
double kilometerToUnit(double kilometers,
                       std::optional<UnitOfMeasurement> unit) {
  using enum UnitOfMeasurement;
  auto multiplicator = [&unit]() {
    if (!unit.has_value()) {
      return 1.0;
    }
    switch (unit.value()) {
      case METERS:
        return 1000.0;
      case KILOMETERS:
        return 1.0;
      case MILES:
        return kilometerToMile;
      case FEET:
        return kilometerToFeet;
      case YARDS:
        return kilometerToYards;
      default:
        AD_CORRECTNESS_CHECK(!isLengthUnit(unit.value()));
        AD_THROW("Unsupported unit of measurement for distance.");
    }
  };
  return multiplicator() * kilometers;
}

// ____________________________________________________________________________
double valueInUnitToKilometer(double valueInUnit,
                              std::optional<UnitOfMeasurement> unit) {
  return valueInUnit / kilometerToUnit(1.0, unit);
}

// ____________________________________________________________________________
double squareMeterToUnit(double squareMeters,
                         std::optional<UnitOfMeasurement> unit) {
  using enum UnitOfMeasurement;
  auto multiplicator = [&unit]() {
    if (!unit.has_value()) {
      return 1.0;
    }
    switch (unit.value()) {
      case SQUARE_METERS:
        return 1.0;
      case SQUARE_KILOMETERS:
        return 1.0E-6;
      case SQUARE_MILES:
        return squareMeterToSquareMile;
      case SQUARE_FEET:
        return squareMeterToSquareFeet;
      case SQUARE_YARDS:
        return squareMeterToSquareYard;
      case ACRE:
        return squareMeterToAcre;
      case ARE:
        return 1.0E-2;
      case HECTARE:
        return 1.0E-4;
      default:
        AD_CORRECTNESS_CHECK(!isAreaUnit(unit.value()));
        AD_THROW("Unsupported unit of measurement for area.");
    }
  };
  return multiplicator() * squareMeters;
}

// ____________________________________________________________________________
UnitOfMeasurement iriToUnitOfMeasurement(const std::string_view& iri) {
  using enum UnitOfMeasurement;
  static const std::unordered_map<std::string_view, UnitOfMeasurement> iriMap{
      {UNIT_METER_IRI, METERS},
      {UNIT_KILOMETER_IRI, KILOMETERS},
      {UNIT_MILE_IRI, MILES},
      {UNIT_FEET_IRI, FEET},
      {UNIT_YARDS_IRI, YARDS},
      {UNIT_SQUARE_METER_IRI, SQUARE_METERS},
      {UNIT_SQUARE_KILOMETER_IRI, SQUARE_KILOMETERS},
      {UNIT_SQUARE_MILE_IRI, SQUARE_MILES},
      {UNIT_SQUARE_FEET_IRI, SQUARE_FEET},
      {UNIT_SQUARE_YARDS_IRI, SQUARE_YARDS},
      {UNIT_ACRE_IRI, ACRE},
      {UNIT_ARE_IRI, ARE},
      {UNIT_HECTARE_IRI, HECTARE},
  };
  if (ad_utility::contains(iriMap, iri)) {
    return iriMap.at(iri);
  }
  return UNKNOWN;
}

// ____________________________________________________________________________
bool isLengthUnit(UnitOfMeasurement unit) {
  using enum UnitOfMeasurement;
  return unit == METERS || unit == KILOMETERS || unit == MILES ||
         unit == FEET || unit == YARDS;
}

// ____________________________________________________________________________
bool isAreaUnit(UnitOfMeasurement unit) {
  using enum UnitOfMeasurement;
  return unit == SQUARE_METERS || unit == SQUARE_KILOMETERS ||
         unit == SQUARE_MILES || unit == SQUARE_FEET || unit == SQUARE_YARDS ||
         unit == ACRE || unit == ARE || unit == HECTARE;
}

}  // namespace ad_utility::detail
