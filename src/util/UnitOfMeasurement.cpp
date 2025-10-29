// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "util/UnitOfMeasurement.h"

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
  if (iri == UNIT_METER_IRI) {
    return METERS;
  } else if (iri == UNIT_KILOMETER_IRI) {
    return KILOMETERS;
  } else if (iri == UNIT_MILE_IRI) {
    return MILES;
  } else if (iri == UNIT_FEET_IRI) {
    return FEET;
  } else if (iri == UNIT_YARDS_IRI) {
    return YARDS;
  } else if (iri == UNIT_SQUARE_METER_IRI) {
    return SQUARE_METERS;
  } else if (iri == UNIT_SQUARE_KILOMETER_IRI) {
    return SQUARE_KILOMETERS;
  } else if (iri == UNIT_SQUARE_MILE_IRI) {
    return SQUARE_MILES;
  } else if (iri == UNIT_SQUARE_FEET_IRI) {
    return SQUARE_FEET;
  } else if (iri == UNIT_SQUARE_YARDS_IRI) {
    return SQUARE_YARDS;
  } else if (iri == UNIT_ACRE_IRI) {
    return ACRE;
  } else if (iri == UNIT_ARE_IRI) {
    return ARE;
  } else if (iri == UNIT_HECTARE_IRI) {
    return HECTARE;
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
