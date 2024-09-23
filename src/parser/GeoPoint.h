//  Copyright 2024, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Christoph Ullinger <ullingec@informatik.uni-freiburg.de>

#pragma once

#include <absl/strings/str_cat.h>

#include <string>

#include "global/Constants.h"
#include "parser/Literal.h"
#include "util/SourceLocation.h"

class Literal;

/// Exception type for construction of GeoPoints that have invalid values
struct CoordinateOutOfRangeException : public std::exception {
 private:
  std::string errorMessage_;

 public:
  explicit CoordinateOutOfRangeException(
      double value, bool isLat,
      ad_utility::source_location s = ad_utility::source_location::current()) {
    errorMessage_ =
        absl::StrCat(s.file_name(), ", line ", s.line(), ": The given value ",
                     value, " is out of range for ",
                     isLat ? "latitude" : "longitude", " coordinates.");
  }

  const char* what() const noexcept override { return errorMessage_.c_str(); }
};

/// A GeoPoint represents a pair of geographical coordinates on earth consisting
/// of latitude (lat) and longitude (lng).
class GeoPoint {
 private:
  double lat_;
  double lng_;

 public:
  using T = uint64_t;

  // A GeoPoint has to store two values (lat and lng).
  // For simplicity in the binary encoding each uses half of the available bits.
  static constexpr T numDataBits = 60;
  static constexpr T numDataBitsCoordinate = numDataBits / 2;
  static constexpr T coordinateMaskLng = (1ULL << numDataBitsCoordinate) - 1;
  static constexpr T coordinateMaskLat = coordinateMaskLng
                                         << numDataBitsCoordinate;
  static constexpr double maxCoordinateEncoded =
      (double)(1 << numDataBitsCoordinate);

  // Construct GeoPoint and ensure valid coordinate values
  GeoPoint(double lat, double lng) {
    if (lat < -COORDINATE_LAT_MAX || lat > COORDINATE_LAT_MAX)
      throw CoordinateOutOfRangeException(lat, true);
    if (lng < -COORDINATE_LNG_MAX || lng > COORDINATE_LNG_MAX)
      throw CoordinateOutOfRangeException(lng, false);

    lat_ = lat;
    lng_ = lng;
  }

  constexpr double getLat() { return lat_; }

  constexpr double getLng() { return lng_; }

  // Convert the value of this GeoPoint object to a single bitstring.
  // The conversion will reduce the precision and thus change the value.
  // However the lost precision should only be in the range of centimeters.
  T toBitRepresentation();

  // Restore a GeoPoint object from a single bitstring produced by the above
  // function. Due to the reduction of precision this object will not have the
  // identical value.
  static GeoPoint fromBitRepresentation(T bits);

  // Construct a GeoPoint from a Literal if this Literal represents a WKT POINT,
  // otherwise return nothing.
  static std::optional<GeoPoint> parseFromLiteral(
      const ad_utility::triple_component::Literal& value);

  std::string toStringRepresentation() {
    // Extra conversion using std::to_string to get more decimals
    return absl::StrCat("POINT(", std::to_string(getLng()), " ",
                        std::to_string(getLat()), ")");
  }

  std::pair<std::string, const char*> toStringAndType() {
    return std::pair(toStringRepresentation(), GEO_WKT_LITERAL);
  }
};