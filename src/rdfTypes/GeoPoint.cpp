//  Copyright 2024, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Christoph Ullinger <ullingec@informatik.uni-freiburg.de>

#include "rdfTypes/GeoPoint.h"

#include <algorithm>
#include <cmath>
#include <optional>
#include <type_traits>

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
  AD_CORRECTNESS_CHECK(0.0 <= downscaled && downscaled <= 1.0,
                       "downscaled coordinate value ", downscaled,
                       " does not satisfy [0,1] constraint");
  // Stretch to allowed range of values between 0 and maxCoordinateEncoded,
  // rounded to integer
  auto newscaled = static_cast<T>(round(downscaled * maxCoordinateEncoded));
  AD_CORRECTNESS_CHECK(
      newscaled <= maxCoordinateEncoded, "scaled coordinate value ", newscaled,
      " does not satisfy [0,", maxCoordinateEncoded, "] constraint");
  return newscaled;
}

// _____________________________________________________________________________
double GeoPoint::dequantizeCoordinate(T quantized, double maxValue) {
  // Together with the check below, this ensures `0 <= quantized`.
  static_assert(std::is_unsigned_v<T>);
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
  auto bits = combineCoordinates(lat, lng, encoding());
  // Ensure the highest 4 bits are 0
  AD_CORRECTNESS_CHECK((bits & coordinateMaskFreeBits) == 0);
  return bits;
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
  auto [lat, lng] = splitCoordinates(bits, encoding());
  return {dequantizeCoordinate(lat, COORDINATE_LAT_MAX),
          dequantizeCoordinate(lng, COORDINATE_LNG_MAX)};
}

// _____________________________________________________________________________
std::vector<std::pair<GeoPoint::T, GeoPoint::T>>
GeoPoint::intervalsForRectangle(const GeoPoint& lowerLeft,
                                const GeoPoint& upperRight,
                                GeoPointEncodingEnum encoding) {
  AD_CONTRACT_CHECK(lowerLeft.getLat() <= upperRight.getLat() &&
                    lowerLeft.getLng() <= upperRight.getLng());

  // The rectangle in quantized coordinates, both ends included. The
  // quantization is monotone, so every point of the rectangle lies in it.
  T latMin = quantizeCoordinate(lowerLeft.getLat(), COORDINATE_LAT_MAX);
  T latMax = quantizeCoordinate(upperRight.getLat(), COORDINATE_LAT_MAX);
  T lngMin = quantizeCoordinate(lowerLeft.getLng(), COORDINATE_LNG_MAX);
  T lngMax = quantizeCoordinate(upperRight.getLng(), COORDINATE_LNG_MAX);

  // For `LatMajor`, the points of the latitude band form one interval.
  if (encoding == GeoPointEncodingEnum::LatMajor) {
    return {{combineCoordinates(latMin, 0, encoding),
             combineCoordinates(latMax, maxCoordinateEncoded, encoding)}};
  }

  // For `ZOrder`, walk the quadtree of the coordinate space in Z-order (so
  // that the intervals come out ascending), starting with the whole space as
  // the root cell: a cell outside the rectangle is dropped, a cell inside it
  // is emitted as one interval, and a cell that overlaps the border of the
  // rectangle is split into its four children, unless its side is at most
  // `stopSide` (then it is emitted whole, which is the "few percent more").
  // Intervals of adjacent cells are merged.
  AD_CORRECTNESS_CHECK(encoding == GeoPointEncodingEnum::ZOrder);
  T stopSide =
      std::max<T>(1, std::min(latMax - latMin + 1, lngMax - lngMin + 1) / 32);
  std::vector<std::pair<T, T>> intervals;
  auto visit = [&](auto&& self, T lat, T lng, T side) -> void {
    T latEnd = lat + side - 1;
    T lngEnd = lng + side - 1;
    if (latEnd < latMin || lat > latMax || lngEnd < lngMin || lng > lngMax) {
      return;
    }
    bool inside =
        lat >= latMin && latEnd <= latMax && lng >= lngMin && lngEnd <= lngMax;
    if (!inside && side > stopSide) {
      T half = side / 2;
      self(self, lat, lng, half);
      self(self, lat, lng + half, half);
      self(self, lat + half, lng, half);
      self(self, lat + half, lng + half, half);
      return;
    }
    T first = interleaveCoordinates(lat, lng);
    T last = interleaveCoordinates(latEnd, lngEnd);
    if (!intervals.empty() && intervals.back().second + 1 == first) {
      intervals.back().second = last;
    } else {
      intervals.emplace_back(first, last);
    }
  };
  visit(visit, 0, 0, maxCoordinateEncoded + 1);
  return intervals;
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
