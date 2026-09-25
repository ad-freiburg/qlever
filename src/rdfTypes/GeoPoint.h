//  Copyright 2024, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Christoph Ullinger <ullingec@informatik.uni-freiburg.de>

#ifndef QLEVER_SRC_PARSER_GEOPOINT_H
#define QLEVER_SRC_PARSER_GEOPOINT_H

#include <absl/strings/str_cat.h>

#include <atomic>
#include <cstdint>
#include <string>
#include <utility>

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

// The two ways in which the quantized latitude and longitude of a `GeoPoint`
// can be arranged in its bit representation, see `GeoPoint` below. The
// encoding is a property of an index: all points of an index (and all points
// that a process creates while it uses that index) must use the same encoding.
// See `GeoPointEncoding.h` for the names under which the encodings appear in
// the configuration of an index and as an option of `qlever-index`.
enum class GeoPointEncodingEnum : uint8_t {
  // The bits of the two coordinates are interleaved (Z-order). This is the
  // default, and the only encoding for which a geographic rectangle maps to a
  // small set of ranges of bit representations.
  ZOrder = 0,
  // The latitude is in the upper and the longitude in the lower half of the
  // bits, so the bit representations are sorted by latitude first. This is
  // how points were encoded until 2026-09-26. It is deprecated and only
  // supported for compatibility with indexes (and software that decodes the
  // `Id`s of an index) from before that date.
  LatMajor = 1,
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

  // A GeoPoint stores two values (lat and lng), each quantized to 30 bits.
  // How the two 30-bit values are arranged in the 60 data bits depends on the
  // encoding (see `GeoPointEncodingEnum` and `encoding()` below):
  //
  // `ZOrder`: The bits are interleaved (Morton or Z-order code): bit `i` of the
  // latitude sits at position `2 * i + 1`, bit `i` of the longitude at
  // position `2 * i`. In this order, the points of any quadtree cell (a square
  // of side `2^k` in the quantized coordinate space, aligned at a multiple of
  // `2^k`) form one contiguous range of bit representations, so that a
  // geographic rectangle maps to a small set of ranges. This is what makes a
  // prefilter on the `Id`s of points effective.
  //
  // `LatMajor`: The latitude is in the upper 30, the longitude in the lower 30
  // of the 60 bits. A geographic rectangle then only maps to one range per
  // quantized latitude, or, as a single range, to the whole latitude band.
  static constexpr T numDataBits = 60;
  static constexpr T numDataBitsCoordinate = numDataBits / 2;
  static constexpr T coordinateMaskFreeBits =
      ad_utility::bitMaskForHigherBits(sizeof(T) * 8 - numDataBits);
  // The largest quantized coordinate value (30 one-bits).
  static constexpr T maxCoordinateEncoded =
      ad_utility::bitMaskForLowerBits(numDataBitsCoordinate);

  // Quantize a coordinate in `[-maxValue, maxValue]` to an integer in
  // `[0, maxCoordinateEncoded]` and back. The quantization step is
  // `2 * maxValue / maxCoordinateEncoded` (about 1.7e-7 degrees for the
  // latitude, 3.4e-7 degrees for the longitude, i.e. a few centimeters).
  static T quantizeCoordinate(double value, double maxValue);
  static double dequantizeCoordinate(T quantized, double maxValue);

  // Interleave two quantized coordinates into a bit representation and take
  // it apart again (see above).
  static constexpr T interleaveCoordinates(T lat, T lng) {
    return (spreadBits(lat) << 1) | spreadBits(lng);
  }
  static constexpr std::pair<T, T> deinterleaveCoordinates(T bits) {
    return {compactBits(bits >> 1), compactBits(bits)};
  }

  // Combine two quantized coordinates into a bit representation with the
  // given `encoding`, and split a bit representation into the two quantized
  // coordinates again (latitude first). The free upper bits of `bits` are
  // ignored.
  static constexpr T combineCoordinates(T lat, T lng,
                                        GeoPointEncodingEnum encoding) {
    if (encoding == GeoPointEncodingEnum::ZOrder) {
      return interleaveCoordinates(lat, lng);
    }
    return ((lat & maxCoordinateEncoded) << numDataBitsCoordinate) |
           (lng & maxCoordinateEncoded);
  }
  static constexpr std::pair<T, T> splitCoordinates(
      T bits, GeoPointEncodingEnum encoding) {
    bits &= ~coordinateMaskFreeBits;
    if (encoding == GeoPointEncodingEnum::ZOrder) {
      return deinterleaveCoordinates(bits);
    }
    return {bits >> numDataBitsCoordinate, bits & maxCoordinateEncoded};
  }

  // The encoding of all points of this process, which is the encoding of the
  // index that the process builds or uses. It is set when an index is built
  // (from the option of `qlever-index`) or loaded (from the configuration of
  // the index), and must not change while points are encoded or decoded.
  //
  // NOTE: This is a process-wide setting, so a process cannot use two indexes
  // with different encodings at the same time.
  static GeoPointEncodingEnum encoding() { return encoding_.load(); }
  static void setEncoding(GeoPointEncodingEnum encoding) {
    encoding_.store(encoding);
  }

 private:
  static inline std::atomic encoding_{GeoPointEncodingEnum::ZOrder};

  // Spread the lower 30 bits of `x` to the even bit positions 0, 2, ..., 58,
  // and the inverse (which ignores the odd bits).
  static constexpr T spreadBits(T x) {
    x &= maxCoordinateEncoded;
    x = (x | (x << 16)) & 0x0000FFFF0000FFFFULL;
    x = (x | (x << 8)) & 0x00FF00FF00FF00FFULL;
    x = (x | (x << 4)) & 0x0F0F0F0F0F0F0F0FULL;
    x = (x | (x << 2)) & 0x3333333333333333ULL;
    x = (x | (x << 1)) & 0x5555555555555555ULL;
    return x;
  }
  static constexpr T compactBits(T x) {
    x &= 0x5555555555555555ULL;
    x = (x | (x >> 1)) & 0x3333333333333333ULL;
    x = (x | (x >> 2)) & 0x0F0F0F0F0F0F0F0FULL;
    x = (x | (x >> 4)) & 0x00FF00FF00FF00FFULL;
    x = (x | (x >> 8)) & 0x0000FFFF0000FFFFULL;
    x = (x | (x >> 16)) & 0x00000000FFFFFFFFULL;
    return x & maxCoordinateEncoded;
  }

 public:
  // Construct GeoPoint and ensure valid coordinate values
  GeoPoint(double lat, double lng);

  constexpr double getLat() const { return lat_; }

  constexpr double getLng() const { return lng_; }

  // Convert the value of this GeoPoint object to a single bitstring.
  // The conversion will reduce the precision and thus change the value.
  // However the lost precision should only be in the range of centimeters.
  // Guarantees to only use the lower `numDataBits` (currently 60 bits), with
  // the quantized lat and lng arranged according to `encoding()` (see above).
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
