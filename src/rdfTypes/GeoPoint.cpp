//  Copyright 2024, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Christoph Ullinger <ullingec@informatik.uni-freiburg.de>

#include "rdfTypes/GeoPoint.h"

#include <algorithm>
#include <cmath>
#include <optional>

#include "global/Constants.h"
#include "parser/NormalizedString.h"
#include "rdfTypes/GeoSparqlHelpers.h"
#include "rdfTypes/Literal.h"
#include "util/Exception.h"

// _____________________________________________________________________________
GeoPoint::GeoPoint(double lat, double lng) : lat_{lat}, lng_{lng} {
  // Ensure valid lat and lng values
  if (lat < -COORDINATE_LAT_MAX || lat > COORDINATE_LAT_MAX || std::isnan(lat))
    throw CoordinateOutOfRangeException(lat, true);
  if (lng < -COORDINATE_LNG_MAX || lng > COORDINATE_LNG_MAX || std::isnan(lng))
    throw CoordinateOutOfRangeException(lng, false);
}

// _____________________________________________________________________________
GeoPoint::T GeoPoint::quantizeCoordinate(double value, double maxValue) {
  // Only positive values between 0 and 1
  double downscaled = (value + maxValue) / (2 * maxValue);
  AD_CORRECTNESS_CHECK(0.0 <= downscaled && downscaled <= 1.0, [&]() {
    return absl::StrCat("downscaled coordinate value ", downscaled,
                        " does not satisfy [0,1] constraint");
  });
  // Stretch to allowed range of values between 0 and maxCoordinateEncoded,
  // rounded to integer
  auto newscaled = static_cast<T>(round(downscaled * maxCoordinateEncoded));
  AD_CORRECTNESS_CHECK(newscaled <= maxCoordinateEncoded, [&]() {
    return absl::StrCat("scaled coordinate value ", newscaled,
                        " does not satisfy [0,", maxCoordinateEncoded,
                        "] constraint");
  });
  return newscaled;
}

// _____________________________________________________________________________
double GeoPoint::dequantizeCoordinate(T quantized, double maxValue) {
  AD_CORRECTNESS_CHECK(quantized <= maxCoordinateEncoded);
  double value =
      ((static_cast<double>(quantized) / maxCoordinateEncoded) * 2 * maxValue) -
      maxValue;
  AD_CORRECTNESS_CHECK(-maxValue <= value && value <= maxValue);
  return value;
}

// _____________________________________________________________________________
GeoPoint::T GeoPoint::toBitRepresentation() const {
  T lat = quantizeCoordinate(getLat(), COORDINATE_LAT_MAX);
  T lng = quantizeCoordinate(getLng(), COORDINATE_LNG_MAX);
  auto bits = interleaveCoordinates(lat, lng);
  // Ensure the highest 4 bits are 0
  AD_CORRECTNESS_CHECK((bits & coordinateMaskFreeBits) == 0);
  return bits;
}

// _____________________________________________________________________________
std::vector<std::pair<GeoPoint::T, GeoPoint::T>>
GeoPoint::bitRangesForRectangle(double minLat, double maxLat, double minLng,
                                double maxLng) {
  AD_CONTRACT_CHECK(minLat <= maxLat && minLng <= maxLng);
  // The quantized bounds, clamped to the valid coordinate ranges and widened
  // by one step on each side.
  auto bounds = [](double lower, double upper, double maxValue) {
    T lo = quantizeCoordinate(std::clamp(lower, -maxValue, maxValue), maxValue);
    T hi = quantizeCoordinate(std::clamp(upper, -maxValue, maxValue), maxValue);
    return std::pair{lo == 0 ? T{0} : lo - 1,
                     std::min(hi + 1, maxCoordinateEncoded)};
  };
  auto [latLo, latHi] = bounds(minLat, maxLat, COORDINATE_LAT_MAX);
  auto [lngLo, lngHi] = bounds(minLng, maxLng, COORDINATE_LNG_MAX);

  // Cells that are not fully inside the rectangle are subdivided until their
  // side is at most a sixteenth of the rectangle's smaller side, which bounds
  // the number of ranges by a small constant times the aspect ratio.
  T minSide = std::min(latHi - latLo, lngHi - lngLo) + 1;
  T stopSide = std::max<T>(T{1}, minSide / 16);

  std::vector<std::pair<T, T>> ranges;
  // Visit the four children in the order (lat 0, lng 0), (0, 1), (1, 0),
  // (1, 1), which is ascending Z-order because the latitude bit is the
  // higher one of each pair.
  auto recurse = [&](auto& self, T latStart, T lngStart, T side) -> void {
    T latEnd = latStart + side - 1;
    T lngEnd = lngStart + side - 1;
    if (latEnd < latLo || latStart > latHi || lngEnd < lngLo ||
        lngStart > lngHi) {
      return;
    }
    bool inside = latStart >= latLo && latEnd <= latHi && lngStart >= lngLo &&
                  lngEnd <= lngHi;
    if (inside || side <= stopSide) {
      T lower = interleaveCoordinates(latStart, lngStart);
      T upper = interleaveCoordinates(latEnd, lngEnd);
      if (!ranges.empty() && ranges.back().second + 1 == lower) {
        ranges.back().second = upper;
      } else {
        ranges.emplace_back(lower, upper);
      }
      return;
    }
    T half = side / 2;
    self(self, latStart, lngStart, half);
    self(self, latStart, lngStart + half, half);
    self(self, latStart + half, lngStart, half);
    self(self, latStart + half, lngStart + half, half);
  };
  recurse(recurse, T{0}, T{0}, T{1} << numDataBitsCoordinate);
  return ranges;
}

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
}

// _____________________________________________________________________________
GeoPoint GeoPoint::fromBitRepresentation(T bits) {
  auto [lat, lng] = deinterleaveCoordinates(bits & ~coordinateMaskFreeBits);
  return {dequantizeCoordinate(lat, COORDINATE_LAT_MAX),
          dequantizeCoordinate(lng, COORDINATE_LNG_MAX)};
}

// _____________________________________________________________________________
std::string GeoPoint::toStringRepresentation() const {
  // Extra conversion using std::to_string to get more decimals
  return absl::StrCat("POINT(", std::to_string(getLng()), " ",
                      std::to_string(getLat()), ")");
}

// _____________________________________________________________________________
std::pair<std::string, const char*> GeoPoint::toStringAndType() const {
  return std::pair(toStringRepresentation(), GEO_WKT_LITERAL.data());
}
