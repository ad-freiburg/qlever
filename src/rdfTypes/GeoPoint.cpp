//  Copyright 2024, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Christoph Ullinger <ullingec@informatik.uni-freiburg.de>

#include "rdfTypes/GeoPoint.h"

#include <cmath>
#include <optional>

#include "parser/NormalizedString.h"
#include "rdfTypes/Literal.h"
#include "util/Exception.h"
#include "util/GeoSparqlHelpers.h"

// _____________________________________________________________________________
GeoPoint::GeoPoint(double lat, double lng) : lat_{lat}, lng_{lng} {
  // Ensure valid lat and lng values
  if (lat < -COORDINATE_LAT_MAX || lat > COORDINATE_LAT_MAX || std::isnan(lat))
    throw CoordinateOutOfRangeException(lat, true);
  if (lng < -COORDINATE_LNG_MAX || lng > COORDINATE_LNG_MAX || std::isnan(lng))
    throw CoordinateOutOfRangeException(lng, false);
};

// _____________________________________________________________________________
GeoPoint::T GeoPoint::toBitRepresentation() const {
  // Transforms a normal-scaled geographic coordinate to an integer
  constexpr auto scaleCoordinate = [](double value, double maxValue) {
    // Only positive values between 0 and 1
    double downscaled = (value + maxValue) / (2 * maxValue);

    AD_CORRECTNESS_CHECK(0.0 <= downscaled && downscaled <= 1.0, [&]() {
      return absl::StrCat("downscaled coordinate value ", downscaled,
                          " does not satisfy [0,1] constraint");
    });

    // Stretch to allowed range of values between 0 and maxCoordinateEncoded,
    // rounded to integer
    auto newscaled =
        static_cast<size_t>(round(downscaled * maxCoordinateEncoded));

    AD_CORRECTNESS_CHECK(
        0.0 <= newscaled && newscaled <= maxCoordinateEncoded, [&]() {
          return absl::StrCat("scaled coordinate value ", newscaled,
                              " does not satisfy [0,", maxCoordinateEncoded,
                              "] constraint");
        });
    return newscaled;
  };

  T lat = scaleCoordinate(getLat(), COORDINATE_LAT_MAX);
  T lng = scaleCoordinate(getLng(), COORDINATE_LNG_MAX);

  // Use shift to obtain 30 bit lat followed by 30 bit lng in lower bits
  auto bits = (lat << numDataBitsCoordinate) | lng;

  // Ensure the highest 4 bits are 0
  AD_CORRECTNESS_CHECK((bits & coordinateMaskFreeBits) == 0);
  return bits;
};

// _____________________________________________________________________________
std::optional<GeoPoint> GeoPoint::parseFromLiteral(
    const ad_utility::triple_component::Literal& value, bool checkDatatype) {
  if (!checkDatatype ||
      (value.hasDatatype() &&
       value.getDatatype() == asNormalizedStringViewUnsafe(GEO_WKT_LITERAL))) {
    auto [lng, lat] = ad_utility::detail::parseWktPoint(
        asStringViewUnsafe(value.getContent()));
    if (!std::isnan(lng) && !std::isnan(lat)) {
      return GeoPoint{lat, lng};
    }
  }
  return std::nullopt;
};

// _____________________________________________________________________________
GeoPoint GeoPoint::fromBitRepresentation(T bits) {
  // Extracts one of the coordinates from a single bitstring
  constexpr auto extractCoordinate = [](T bits, T mask, T shift,
                                        double maxValue) {
    // Obtain raw value from bits
    auto value = static_cast<double>((bits & mask) >> shift);
    AD_CORRECTNESS_CHECK(0.0 <= value && value <= maxCoordinateEncoded);

    // Transform to usual scaling
    value = ((value / maxCoordinateEncoded) * 2 * maxValue) - maxValue;
    AD_CORRECTNESS_CHECK(-maxValue <= value && value <= maxValue);
    return value;
  };

  double lat = extractCoordinate(bits, coordinateMaskLat, numDataBitsCoordinate,
                                 COORDINATE_LAT_MAX);
  double lng =
      extractCoordinate(bits, coordinateMaskLng, 0, COORDINATE_LNG_MAX);

  return {lat, lng};
};

// _____________________________________________________________________________
std::string GeoPoint::toStringRepresentation() const {
  // Extra conversion using std::to_string to get more decimals
  return absl::StrCat("POINT(", std::to_string(getLng()), " ",
                      std::to_string(getLat()), ")");
};

// _____________________________________________________________________________
std::pair<std::string, const char*> GeoPoint::toStringAndType() const {
  return std::pair(toStringRepresentation(), GEO_WKT_LITERAL.data());
};
