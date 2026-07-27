//  Copyright 2024, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Christoph Ullinger <ullingec@informatik.uni-freiburg.de>

#ifndef QLEVER_SRC_PARSER_GEOPOINT_H
#define QLEVER_SRC_PARSER_GEOPOINT_H

#include <absl/strings/str_cat.h>

#include <string>

#include "backports/three_way_comparison.h"
#include "rdfTypes/Literal.h"
#include "util/BitUtils.h"
#include "util/SourceLocation.h"

/// Exception type for construction of GeoPoints that have invalid values
struct CoordinateOutOfRangeException : public std::exception {
 private:
  std::string errorMessage_;

 public:
  explicit CoordinateOutOfRangeException(
      double value, bool isLat,
      ad_utility::source_location s = AD_CURRENT_SOURCE_LOC()) {
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

  CPP_template(typename H,
               typename G)(requires ql::concepts::same_as<G, GeoPoint>) friend H
      AbslHashValue(H h, const G& g) {
    return H::combine(std::move(h), g.lat_, g.lng_);
  }

  // A GeoPoint has to store two values (lat and lng).
  // For simplicity in the binary encoding each uses half of the available bits.
  static constexpr T numDataBits = 60;
  static constexpr T numDataBitsCoordinate = numDataBits / 2;
  static constexpr T coordinateMaskLng =
      ad_utility::bitMaskForLowerBits(numDataBitsCoordinate);
  static constexpr T coordinateMaskLat = coordinateMaskLng
                                         << numDataBitsCoordinate;
  static constexpr T coordinateMaskFreeBits =
      ad_utility::bitMaskForHigherBits(sizeof(T) * 8 - numDataBits);
  static constexpr double maxCoordinateEncoded =
      static_cast<double>(coordinateMaskLng);

  // Construct GeoPoint and ensure valid coordinate values
  GeoPoint(double lat, double lng);

  constexpr double getLat() const { return lat_; }

  constexpr double getLng() const { return lng_; }

  // Convert the value of this GeoPoint object to a single bitstring.
  // The conversion will reduce the precision and thus change the value.
  // However the lost precision should only be in the range of centimeters.
  // Guarantees to only use the lower `numDataBits` (currently 60 bits),
  // with lng stored in the lower 30 and lat stored in the upper 30 bits of
  // the lower 60.
  T toBitRepresentation() const;

  // Restore a GeoPoint object from a single bitstring produced by the above
  // function. Due to the reduction of precision this object will not have the
  // identical value. Ignores the upper 4 bits (only uses the lower
  // `numDataBits`)
  static GeoPoint fromBitRepresentation(T bits);

  // Construct a GeoPoint from a Literal if this Literal represents a WKT POINT,
  // otherwise return nothing.
  static std::optional<GeoPoint> parseFromLiteral(
      const ad_utility::triple_component::Literal& value,
      bool checkDatatype = true);

  std::string toStringRepresentation() const;

  std::pair<std::string, const char*> toStringAndType() const;

  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(GeoPoint, lat_, lng_)
};

#endif  // QLEVER_SRC_PARSER_GEOPOINT_H
