// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_GEOPOINTENCODING_H
#define QLEVER_SRC_INDEX_GEOPOINTENCODING_H

#include <array>
#include <string_view>

#include "rdfTypes/GeoPoint.h"
#include "util/EnumWithStrings.h"

namespace ad_utility {

// The encoding of the `GeoPoint`s of an index (see `GeoPointEncodingEnum`),
// with the names under which it appears as the value of the key
// `geo-point-encoding` in the configuration of an index and as the value of the
// option `--geo-point-encoding` of `qlever-index`. The enum itself is defined
// in `GeoPoint.h`, which is included almost everywhere and should therefore
// not depend on the (heavy) includes of `EnumWithStrings.h`.
class GeoPointEncoding
    : public EnumWithStrings<GeoPointEncoding, GeoPointEncodingEnum> {
 public:
  using Enum = GeoPointEncodingEnum;

  static constexpr std::array<std::pair<Enum, std::string_view>, 2>
      descriptions_{{{Enum::ZOrder, "z-order"}, {Enum::LatMajor, "lat-major"}}};

  static const GeoPointEncoding ZOrder;
  static const GeoPointEncoding LatMajor;

  static constexpr std::string_view typeName() { return "geo point encoding"; }

  using EnumWithStrings::EnumWithStrings;
};

const inline GeoPointEncoding GeoPointEncoding::ZOrder{
    GeoPointEncoding::Enum::ZOrder};
const inline GeoPointEncoding GeoPointEncoding::LatMajor{
    GeoPointEncoding::Enum::LatMajor};

// The key under which the encoding is stored in the configuration of an index.
// An index in the format `qlever::indexFormatVersionWithLatMajorGeoPoints`
// predates this key and has no entry for it; its points are `LatMajor`.
inline constexpr std::string_view GEO_POINT_ENCODING_KEY = "geo-point-encoding";

// The warning that is shown when an index is built or used with the deprecated
// `LatMajor` encoding.
inline constexpr std::string_view LAT_MAJOR_GEO_POINT_ENCODING_WARNING =
    "The geo points of this index use the encoding \"lat-major\", which is "
    "deprecated and will not be supported for much longer; with this "
    "encoding, a spatial prefilter on points can only restrict the latitude; "
    "please build the index with the default encoding \"z-order\" (just omit "
    "the option `--geo-point-encoding` of `qlever-index`)";

}  // namespace ad_utility

#endif  // QLEVER_SRC_INDEX_GEOPOINTENCODING_H
