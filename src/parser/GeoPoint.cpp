//  Copyright 2024, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Christoph Ullinger <ullingec@informatik.uni-freiburg.de>

#include "parser/GeoPoint.h"

#include <cmath>
#include <optional>

#include "parser/Literal.h"
#include "parser/NormalizedString.h"
#include "util/GeoSparqlHelpers.h"

// _____________________________________________________________________________
GeoPoint::T GeoPoint::toBitRepresentation() {
  // Transforms a normal-scaled geographic coordinate to an integer
  constexpr auto scaleCoordinate = [](double value, double maxValue) {
    // Only positive values between 0 and 1
    double downscaled = (value + maxValue) / (2 * maxValue);
    AD_CORRECTNESS_CHECK(0.0 <= downscaled && downscaled <= 1.0);

    // Stretch to allowed range of values between 0 and maxCoordinateEncoded,
    // rounded to integer
    auto newscaled =
        static_cast<size_t>(round(downscaled * maxCoordinateEncoded));
    AD_CORRECTNESS_CHECK(0.0 <= newscaled && newscaled <= maxCoordinateEncoded);
    return newscaled;
  };

  T lat = scaleCoordinate(getLat(), COORDINATE_LAT_MAX);
  T lng = scaleCoordinate(getLng(), COORDINATE_LNG_MAX);

  // Use shift to obtain 30 bit lat followed by 30 bit lng in one 60 bit int
  return (lat << numDataBitsCoordinate) | lng;
};

// _____________________________________________________________________________
std::optional<GeoPoint> GeoPoint::parseFromLiteral(
    const ad_utility::triple_component::Literal& value) {
  if (value.hasDatatype() &&
      value.getDatatype().compare(asNormalizedStringViewUnsafe(
          std::string_view(GEO_WKT_LITERAL))) == 0) {
    auto [lng, lat] = ad_utility::detail::parseWktPoint(
        asStringViewUnsafe(value.getContent()));
    if (!std::isnan(lng) && !std::isnan(lat)) {
      return GeoPoint(lat, lng);
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

  return GeoPoint(lat, lng);
};
