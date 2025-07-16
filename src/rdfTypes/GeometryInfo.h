// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_UTIL_GEOMETRYINFO_H
#define QLEVER_SRC_UTIL_GEOMETRYINFO_H

#include <cstdint>
#include <cstdio>
#include <string>

#include "concepts/concepts.hpp"
#include "global/ValueId.h"
#include "rdfTypes/GeoPoint.h"
#include "util/BitUtils.h"

namespace ad_utility {

// These encapsulating structs are required for the `RequestedInfo` templates
// used in the `GeometryInfo` class and the `GeometryInfoValueGetter`.

// Represents the centroid of a geometry as a `GeoPoint`.
struct Centroid {
  GeoPoint centroid_;

  Centroid(GeoPoint centroid) : centroid_{centroid} {};
  Centroid(double lat, double lng) : centroid_{lat, lng} {};
};

// The individual coordinates describing the bounding box.
enum class BoundingCoordinate { MIN_X, MIN_Y, MAX_X, MAX_Y };

// Represents the bounding box of a geometry by two `GeoPoint`s for lower left
// corner and upper right corner.
struct BoundingBox {
  GeoPoint lowerLeft_;
  GeoPoint upperRight_;

  std::string asWkt() const;

  // Extract the minimum or maximum coordinates
  template <BoundingCoordinate RequestedCoordinate>
  double getBoundingCoordinate() const;
};

// The encoded bounding box is a pair of the bit encodings of the
// `BoundingBox`'s two `GeoPoint`s. Due to `std::pair` not being trivially
// copyable, this is implemented as a `struct`.
struct EncodedBoundingBox {
  uint64_t lowerLeftEncoded_;
  uint64_t upperRightEncoded_;
};

// Represents the WKT geometry type, for the meaning see `libspatialjoin`'s
// `WKTType`.
struct GeometryType {
  uint8_t type_;
  GeometryType(uint8_t type) : type_{type} {};

  // Returns an IRI without brackets of the OGC Simple Features geometry type.
  std::optional<std::string_view> asIri() const;
};

// Forward declaration for concept
class GeometryInfo;

// Concept for the `RequestedInfo` template parameter: any of these types is
// allowed to be requested.
template <typename T>
CPP_concept RequestedInfoT =
    SameAsAny<T, GeometryInfo, Centroid, BoundingBox, GeometryType>;

// The version of the `GeometryInfo`: to ensure correctness when reading disk
// serialized objects of this class.
constexpr uint64_t GEOMETRY_INFO_VERSION = 1;

// A geometry info object holds precomputed details on WKT literals.
// IMPORTANT: Every modification of the attributes of this class will be an
// index-breaking change regarding the `GeoVocabulary`. Please update the
// `GEOMETRY_INFO_VERSION` constant above accordingly, which will invalidate all
// indices using such a vocabulary.
class GeometryInfo {
 private:
  // `GeometryInfo` must ensure that its attributes' binary representation
  // cannot be all-zero. This is currently used by the disk serialization of
  // `GeoVocabulary` to represent invalid literals.
  EncodedBoundingBox boundingBox_;
  uint64_t geometryTypeAndCentroid_;

  // TODO<ullingerc>: Implement the behavior for the following two
  // attributes
  //   double metricSize_ = 0;
  //   int64_t parsedGeometryOffset_ = -1;

  static constexpr uint64_t bitMaskGeometryType =
      bitMaskForHigherBits(ValueId::numDatatypeBits);
  static constexpr uint64_t bitMaskCentroid =
      bitMaskForLowerBits(ValueId::numDataBits);

 public:
  GeometryInfo(uint8_t wktType, const BoundingBox& boundingBox,
               Centroid centroid);

  GeometryInfo(const GeometryInfo& other) = default;

  // Parse an arbitrary WKT literal and compute all attributes. Return
  // `std:nullopt` if `wkt` cannot be parsed.
  static std::optional<GeometryInfo> fromWktLiteral(std::string_view wkt);

  // Create geometry info for a GeoPoint object.
  static GeometryInfo fromGeoPoint(const GeoPoint& point);

  // Extract the WKT geometry type from geometryTypeAndCentroid_.
  GeometryType getWktType() const;

  // Parse an arbitrary WKT literal and return only the geometry type.
  static std::optional<GeometryType> getWktType(std::string_view wkt);

  // Extract centroid from geometryTypeAndCentroid_ and convert it to GeoPoint.
  Centroid getCentroid() const;

  // Parse an arbitrary WKT literal and compute only the centroid.
  static std::optional<Centroid> getCentroid(std::string_view wkt);

  // Convert the bounding box to GeoPoints.
  BoundingBox getBoundingBox() const;

  // Parse an arbitrary WKT literal and compute only the bounding box.
  static std::optional<BoundingBox> getBoundingBox(std::string_view wkt);

  // Extract the requested information from this object.
  template <typename RequestedInfo = GeometryInfo>
  requires RequestedInfoT<RequestedInfo> RequestedInfo getRequestedInfo() const;

  // Parse the given WKT literal and compute only the requested information.
  template <typename RequestedInfo = GeometryInfo>
  requires RequestedInfoT<RequestedInfo>
  static std::optional<RequestedInfo> getRequestedInfo(std::string_view wkt);
};

// For the disk serialization we require that a `GeometryInfo` is trivially
// copyable.
static_assert(std::is_trivially_copyable_v<GeometryInfo>);

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_GEOMETRYINFO_H
