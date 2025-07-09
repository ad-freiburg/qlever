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
 private:
  GeoPoint centroid_;

 public:
  Centroid(GeoPoint centroid) : centroid_{centroid} {};
  Centroid(double lat, double lng) : centroid_{lat, lng} {};

  GeoPoint centroid() const { return centroid_; }
};

// Represents the bounding box of a geometry by two `GeoPoint`s for lower left
// corner and upper right corner.
struct BoundingBox {
 private:
  GeoPoint lowerLeft_;
  GeoPoint upperRight_;

 public:
  BoundingBox(GeoPoint lowerLeft, GeoPoint upperRight);

  GeoPoint lowerLeft() const { return lowerLeft_; }
  GeoPoint upperRight() const { return upperRight_; }
  std::pair<GeoPoint, GeoPoint> pair() const {
    return {lowerLeft_, upperRight_};
  }

  // Return a `POLYGON` WKT literal without quotes or datatype representing this
  // bounding box.
  std::string asWkt() const;
};

// The encoded bounding box is a pair of the bit encodings of the
// `BoundingBox`'s two `GeoPoint`s.
using EncodedBoundingBox = std::pair<uint64_t, uint64_t>;

// Represents the WKT geometry type, for the meaning see `libspatialjoin`'s
// `WKTType`.
struct GeometryType {
 private:
  uint8_t type_;

 public:
  GeometryType(uint8_t type);

  uint8_t type() const { return type_; }

  // Returns an IRI without brackets of the OGC Simple Features geometry type.
  std::optional<std::string_view> asIri() const;
};

// Represents the length of the geometry in meters.
struct MetricLength {
 private:
  double length_;

 public:
  MetricLength(double length);

  double length() const { return length_; }
};

// Forward declaration for concept
class GeometryInfo;

// Concept for the `RequestedInfo` template parameter: any of these types is
// allowed to be requested.
template <typename T>
CPP_concept RequestedInfoT = SameAsAny<T, GeometryInfo, Centroid, BoundingBox,
                                       GeometryType, MetricLength>;

// A geometry info object holds precomputed details on WKT literals.
// IMPORTANT: Every modification of the attributes of this class will be an
// index-breaking change regarding the GeoVocabulary. Please update the index
// version accordingly.
class GeometryInfo {
 private:
  EncodedBoundingBox boundingBox_;
  uint64_t geometryTypeAndCentroid_;

  double metricLength_ = 0.0;
  // TODO: Support length for polygon etc. as requested by standard

  // TODO<ullingerc>: Implement the behavior for the following two
  // attributes
  // float metricArea_ = 0.0;
  //   int64_t parsedGeometryOffset_ = -1;

  static constexpr uint64_t bitMaskGeometryType =
      bitMaskForHigherBits(ValueId::numDatatypeBits);
  static constexpr uint64_t bitMaskCentroid =
      bitMaskForLowerBits(ValueId::numDataBits);

 public:
  GeometryInfo(uint8_t wktType, const BoundingBox& boundingBox,
               Centroid centroid, MetricLength metricLength);

  // Parse an arbitrary WKT literal and compute all attributes.
  static GeometryInfo fromWktLiteral(const std::string_view& wkt);

  // Create geometry info for a GeoPoint object.
  static GeometryInfo fromGeoPoint(const GeoPoint& point);

  // Extract the WKT geometry type from geometryTypeAndCentroid_.
  GeometryType getWktType() const;

  // Parse an arbitrary WKT literal and return only the geometry type.
  static GeometryType getWktType(const std::string_view& wkt);

  // Extract centroid from geometryTypeAndCentroid_ and convert it to GeoPoint.
  Centroid getCentroid() const;

  // Parse an arbitrary WKT literal and compute only the centroid.
  static Centroid getCentroid(const std::string_view& wkt);

  // Convert the bounding box to GeoPoints.
  BoundingBox getBoundingBox() const;

  // Parse an arbitrary WKT literal and compute only the bounding box.
  static BoundingBox getBoundingBox(const std::string_view& wkt);

  // Extract the length in meters.
  MetricLength getMetricLength() const;

  // Parse an arbitrary WKT literal and compute only the length in meters.
  static MetricLength getMetricLength(const std::string_view& wkt);

  // Extract the requested information from this object.
  template <typename RequestedInfo = GeometryInfo>
  requires RequestedInfoT<RequestedInfo> RequestedInfo getRequestedInfo() const;

  // Parse the given WKT literal and compute only the requested information.
  template <typename RequestedInfo = GeometryInfo>
  requires RequestedInfoT<RequestedInfo>
  static RequestedInfo getRequestedInfo(const std::string_view& wkt);
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_GEOMETRYINFO_H
