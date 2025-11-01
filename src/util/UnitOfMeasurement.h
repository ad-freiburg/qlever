// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_UTIL_UNITOFMEASUREMENT_H
#define QLEVER_SRC_UTIL_UNITOFMEASUREMENT_H

#include "global/Constants.h"

enum class UnitOfMeasurement {
  METERS,
  KILOMETERS,
  MILES,
  SQUARE_METERS,
  SQUARE_KILOMETERS,
  SQUARE_MILES,
  UNKNOWN
};

constexpr inline std::string_view UNIT_PREFIX = "http://qudt.org/vocab/unit/";

namespace string_constants::detail {
constexpr inline std::string_view unit_meter = "M";
constexpr inline std::string_view unit_kilometer = "KiloM";
constexpr inline std::string_view unit_mile = "MI";
constexpr inline std::string_view unit_square_meter = "M2";
constexpr inline std::string_view unit_square_kilometer = "KiloM2";
constexpr inline std::string_view unit_square_mile = "MI2";
}  // namespace string_constants::detail

constexpr inline std::string_view UNIT_METER_IRI =
    ad_utility::constexprStrCat<UNIT_PREFIX,
                                string_constants::detail::unit_meter>();
constexpr inline std::string_view UNIT_KILOMETER_IRI =
    ad_utility::constexprStrCat<UNIT_PREFIX,
                                string_constants::detail::unit_kilometer>();
constexpr inline std::string_view UNIT_MILE_IRI =
    ad_utility::constexprStrCat<UNIT_PREFIX,
                                string_constants::detail::unit_mile>();
constexpr inline std::string_view UNIT_SQUARE_METER_IRI =
    ad_utility::constexprStrCat<UNIT_PREFIX,
                                string_constants::detail::unit_square_meter>();
constexpr inline std::string_view UNIT_SQUARE_KILOMETER_IRI =
    ad_utility::constexprStrCat<
        UNIT_PREFIX, string_constants::detail::unit_square_kilometer>();
constexpr inline std::string_view UNIT_SQUARE_MILE_IRI =
    ad_utility::constexprStrCat<UNIT_PREFIX,
                                string_constants::detail::unit_square_mile>();

namespace ad_utility::detail {

static constexpr double kilometerToMile = 0.62137119;
static constexpr double squareMeterToSquareMile =
    (kilometerToMile / 1000) * (kilometerToMile / 1000);

// Convert kilometers to other supported units. If `unit` is `std::nullopt` it
// is treated as kilometers.
double kilometerToUnit(double kilometers,
                       std::optional<UnitOfMeasurement> unit);

// Convert value from any supported unit to kilometers. If `unit` is
// `std::nullopt` it is treated as kilometers.
double valueInUnitToKilometer(double valueInUnit,
                              std::optional<UnitOfMeasurement> unit);

// Convert square meters to another supported area unit. If `unit` is
// `std::nullopt` it is treated as square meters (value is returned unchanged).
double squareMeterToUnit(double squareMeters,
                         std::optional<UnitOfMeasurement> unit);

// Returns `true` iff `unit` is a unit for measuring length / distance.
bool isLengthUnit(UnitOfMeasurement unit);

// Returns `true` iff `unit` is a unit for measuring area.
bool isAreaUnit(UnitOfMeasurement unit);

// Convert a unit IRI string (without quotes or brackets) to unit.
UnitOfMeasurement iriToUnitOfMeasurement(const std::string_view& uri);

}  // namespace ad_utility::detail

#endif  // QLEVER_SRC_UTIL_UNITOFMEASUREMENT_H
