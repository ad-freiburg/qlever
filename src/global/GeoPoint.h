//  Copyright 2024, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Christoph Ullinger <ullingec@informatik.uni-freiburg.de>

#pragma once

#include <absl/strings/str_cat.h>

#include <string>

#include "global/Constants.h"
#include "util/SourceLocation.h"

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
  double lat;
  double lng;

 public:
  GeoPoint(double lat, double lng) {
    if (lat < -COORDINATE_LAT_MAX || lat > COORDINATE_LAT_MAX)
      throw CoordinateOutOfRangeException(lat, true);
    if (lng < -COORDINATE_LNG_MAX || lng > COORDINATE_LNG_MAX)
      throw CoordinateOutOfRangeException(lng, false);

    this->lat = lat;
    this->lng = lng;
  }

  double getLat() { return this->lat; }

  double getLng() { return this->lng; }

  std::string toStringRepresentation() {
    return absl::StrCat("POINT(", std::to_string(this->getLng()), " ",
                        std::to_string(this->getLat()), ")");
  }

  std::pair<std::string, const char*> toStringAndType() {
    return std::pair(toStringRepresentation(), GEO_WKT_LITERAL);
  }

  std::string toFullStringRepresentation() {
    // TODO<ullingerc>: use utility functions instead of manual string
    // operations
    return absl::StrCat("\"", toStringRepresentation(), "\"^^<",
                        GEO_WKT_LITERAL, ">");
  }
};
