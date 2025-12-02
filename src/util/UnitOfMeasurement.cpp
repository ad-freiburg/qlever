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
  [[maybe_unused]] static constexpr auto METERS = UnitOfMeasurement::METERS;
  [[maybe_unused]] static constexpr auto KILOMETERS =
      UnitOfMeasurement::KILOMETERS;
  [[maybe_unused]] static constexpr auto MILES = UnitOfMeasurement::MILES;
  [[maybe_unused]] static constexpr auto FEET = UnitOfMeasurement::FEET;
  [[maybe_unused]] static constexpr auto YARDS = UnitOfMeasurement::YARDS;
  [[maybe_unused]] static constexpr auto SQUARE_METERS =
      UnitOfMeasurement::SQUARE_METERS;
  [[maybe_unused]] static constexpr auto SQUARE_KILOMETERS =
      UnitOfMeasurement::SQUARE_KILOMETERS;
  [[maybe_unused]] static constexpr auto SQUARE_MILES =
      UnitOfMeasurement::SQUARE_MILES;
  [[maybe_unused]] static constexpr auto SQUARE_FEET =
      UnitOfMeasurement::SQUARE_FEET;
  [[maybe_unused]] static constexpr auto SQUARE_YARDS =
      UnitOfMeasurement::SQUARE_YARDS;
  [[maybe_unused]] static constexpr auto ACRE = UnitOfMeasurement::ACRE;
  [[maybe_unused]] static constexpr auto ARE = UnitOfMeasurement::ARE;
  [[maybe_unused]] static constexpr auto HECTARE = UnitOfMeasurement::HECTARE;
  [[maybe_unused]] static constexpr auto UNKNOWN = UnitOfMeasurement::UNKNOWN;
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
  [[maybe_unused]] static constexpr auto METERS = UnitOfMeasurement::METERS;
  [[maybe_unused]] static constexpr auto KILOMETERS =
      UnitOfMeasurement::KILOMETERS;
  [[maybe_unused]] static constexpr auto MILES = UnitOfMeasurement::MILES;
  [[maybe_unused]] static constexpr auto FEET = UnitOfMeasurement::FEET;
  [[maybe_unused]] static constexpr auto YARDS = UnitOfMeasurement::YARDS;
  [[maybe_unused]] static constexpr auto SQUARE_METERS =
      UnitOfMeasurement::SQUARE_METERS;
  [[maybe_unused]] static constexpr auto SQUARE_KILOMETERS =
      UnitOfMeasurement::SQUARE_KILOMETERS;
  [[maybe_unused]] static constexpr auto SQUARE_MILES =
      UnitOfMeasurement::SQUARE_MILES;
  [[maybe_unused]] static constexpr auto SQUARE_FEET =
      UnitOfMeasurement::SQUARE_FEET;
  [[maybe_unused]] static constexpr auto SQUARE_YARDS =
      UnitOfMeasurement::SQUARE_YARDS;
  [[maybe_unused]] static constexpr auto ACRE = UnitOfMeasurement::ACRE;
  [[maybe_unused]] static constexpr auto ARE = UnitOfMeasurement::ARE;
  [[maybe_unused]] static constexpr auto HECTARE = UnitOfMeasurement::HECTARE;
  [[maybe_unused]] static constexpr auto UNKNOWN = UnitOfMeasurement::UNKNOWN;
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
  [[maybe_unused]] static constexpr auto METERS = UnitOfMeasurement::METERS;
  [[maybe_unused]] static constexpr auto KILOMETERS =
      UnitOfMeasurement::KILOMETERS;
  [[maybe_unused]] static constexpr auto MILES = UnitOfMeasurement::MILES;
  [[maybe_unused]] static constexpr auto FEET = UnitOfMeasurement::FEET;
  [[maybe_unused]] static constexpr auto YARDS = UnitOfMeasurement::YARDS;
  [[maybe_unused]] static constexpr auto SQUARE_METERS =
      UnitOfMeasurement::SQUARE_METERS;
  [[maybe_unused]] static constexpr auto SQUARE_KILOMETERS =
      UnitOfMeasurement::SQUARE_KILOMETERS;
  [[maybe_unused]] static constexpr auto SQUARE_MILES =
      UnitOfMeasurement::SQUARE_MILES;
  [[maybe_unused]] static constexpr auto SQUARE_FEET =
      UnitOfMeasurement::SQUARE_FEET;
  [[maybe_unused]] static constexpr auto SQUARE_YARDS =
      UnitOfMeasurement::SQUARE_YARDS;
  [[maybe_unused]] static constexpr auto ACRE = UnitOfMeasurement::ACRE;
  [[maybe_unused]] static constexpr auto ARE = UnitOfMeasurement::ARE;
  [[maybe_unused]] static constexpr auto HECTARE = UnitOfMeasurement::HECTARE;
  [[maybe_unused]] static constexpr auto UNKNOWN = UnitOfMeasurement::UNKNOWN;
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
  [[maybe_unused]] static constexpr auto METERS = UnitOfMeasurement::METERS;
  [[maybe_unused]] static constexpr auto KILOMETERS =
      UnitOfMeasurement::KILOMETERS;
  [[maybe_unused]] static constexpr auto MILES = UnitOfMeasurement::MILES;
  [[maybe_unused]] static constexpr auto FEET = UnitOfMeasurement::FEET;
  [[maybe_unused]] static constexpr auto YARDS = UnitOfMeasurement::YARDS;
  [[maybe_unused]] static constexpr auto SQUARE_METERS =
      UnitOfMeasurement::SQUARE_METERS;
  [[maybe_unused]] static constexpr auto SQUARE_KILOMETERS =
      UnitOfMeasurement::SQUARE_KILOMETERS;
  [[maybe_unused]] static constexpr auto SQUARE_MILES =
      UnitOfMeasurement::SQUARE_MILES;
  [[maybe_unused]] static constexpr auto SQUARE_FEET =
      UnitOfMeasurement::SQUARE_FEET;
  [[maybe_unused]] static constexpr auto SQUARE_YARDS =
      UnitOfMeasurement::SQUARE_YARDS;
  [[maybe_unused]] static constexpr auto ACRE = UnitOfMeasurement::ACRE;
  [[maybe_unused]] static constexpr auto ARE = UnitOfMeasurement::ARE;
  [[maybe_unused]] static constexpr auto HECTARE = UnitOfMeasurement::HECTARE;
  [[maybe_unused]] static constexpr auto UNKNOWN = UnitOfMeasurement::UNKNOWN;
  return unit == METERS || unit == KILOMETERS || unit == MILES ||
         unit == FEET || unit == YARDS;
}

// ____________________________________________________________________________
bool isAreaUnit(UnitOfMeasurement unit) {
  [[maybe_unused]] static constexpr auto METERS = UnitOfMeasurement::METERS;
  [[maybe_unused]] static constexpr auto KILOMETERS =
      UnitOfMeasurement::KILOMETERS;
  [[maybe_unused]] static constexpr auto MILES = UnitOfMeasurement::MILES;
  [[maybe_unused]] static constexpr auto FEET = UnitOfMeasurement::FEET;
  [[maybe_unused]] static constexpr auto YARDS = UnitOfMeasurement::YARDS;
  [[maybe_unused]] static constexpr auto SQUARE_METERS =
      UnitOfMeasurement::SQUARE_METERS;
  [[maybe_unused]] static constexpr auto SQUARE_KILOMETERS =
      UnitOfMeasurement::SQUARE_KILOMETERS;
  [[maybe_unused]] static constexpr auto SQUARE_MILES =
      UnitOfMeasurement::SQUARE_MILES;
  [[maybe_unused]] static constexpr auto SQUARE_FEET =
      UnitOfMeasurement::SQUARE_FEET;
  [[maybe_unused]] static constexpr auto SQUARE_YARDS =
      UnitOfMeasurement::SQUARE_YARDS;
  [[maybe_unused]] static constexpr auto ACRE = UnitOfMeasurement::ACRE;
  [[maybe_unused]] static constexpr auto ARE = UnitOfMeasurement::ARE;
  [[maybe_unused]] static constexpr auto HECTARE = UnitOfMeasurement::HECTARE;
  [[maybe_unused]] static constexpr auto UNKNOWN = UnitOfMeasurement::UNKNOWN;
  return unit == SQUARE_METERS || unit == SQUARE_KILOMETERS ||
         unit == SQUARE_MILES || unit == SQUARE_FEET || unit == SQUARE_YARDS ||
         unit == ACRE || unit == ARE || unit == HECTARE;
}

}  // namespace ad_utility::detail
