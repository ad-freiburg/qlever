// Copyright 2025 - 2026 The QLever Authors, in particular:
//
// 2025 - 2026 Christoph Ullinger <ullingec@cs.uni-freiburg.de>, UFR
// 2026 Yannik Schnell github@yarox-1, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_GEOMETRYINFO_H
#define QLEVER_SRC_UTIL_GEOMETRYINFO_H

#include <array>
#include <cstdint>
#include <cstdio>
#include <string>
#include <string_view>

#include "backports/StartsWithAndEndsWith.h"
#include "backports/span.h"
#include "backports/three_way_comparison.h"
#include "concepts/concepts.hpp"
#include "global/Constants.h"
#include "global/Id.h"
#include "rdfTypes/GeoPoint.h"
#include "util/BitUtils.h"

// Forward declaration of `libspatialjoin`'s `CRSType` (the util is not
// included in this header).
//
// NOTE: The underlying type must match the definition in the util's
// `geo/Geo.h`. A mismatch does not compile, because `GeometryInfo.cpp`
// includes both declarations.
namespace util::geo {
enum class CRSType : uint8_t;
}

namespace ad_utility {

// These encapsulating structs are required for the `RequestedInfo` templates
// used in the `GeometryInfo` class and the `GeometryInfoValueGetter`.

// Represents the centroid of a geometry as a `GeoPoint`.
struct Centroid {
 private:
  GeoPoint centroid_;

 public:
  explicit Centroid(GeoPoint centroid) : centroid_{centroid} {}
  Centroid(double lat, double lng) : centroid_{lat, lng} {}

  GeoPoint centroid() const { return centroid_; }
};

// The individual coordinates describing the bounding box.
enum class BoundingCoordinate { MIN_X, MIN_Y, MAX_X, MAX_Y };

// Corners of a bounding box.
enum class BoundingBoxCorner { LOWER_LEFT, UPPER_RIGHT };

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
 private:
  uint8_t type_;

 public:
  explicit GeometryType(uint8_t type);

  uint8_t type() const { return type_; }

  // Returns an IRI without brackets of the OGC Simple Features geometry type.
  std::optional<std::string_view> asIri() const;

  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL_CONSTEXPR(GeometryType, type_)
};

// Represents the source CRS type (before parsing/projecting), for the meaning
// see `libspatialjoin`'s `CRSType`.
struct SourceCrsType {
 private:
  // The default (for the C++17 default constructor) is CRS84, the value of
  // `util::geo::CRSType::CRS84` (the util is not included in this header).
  uint16_t type_ = 1;

 public:
  explicit SourceCrsType(uint16_t type);
  explicit SourceCrsType(::util::geo::CRSType type);
#ifdef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
  // Required for `bit_cast`.
  SourceCrsType() = default;
#endif

  uint16_t type() const { return type_; }

  // Returns the representing IRI.
  std::optional<std::string_view> asIri() const;

  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL_CONSTEXPR(SourceCrsType, type_)
};

// Represents the number of child geometries inside a collection geometry type.
struct NumGeometries {
 private:
  uint32_t numGeometries_;

 public:
  NumGeometries(uint32_t numGeometries) : numGeometries_{numGeometries} {}

  uint32_t numGeometries() const { return numGeometries_; }

  constexpr bool operator==(const NumGeometries& other) const {
    return numGeometries_ == other.numGeometries_;
  }
};

// Represents the length of the geometry in meters.
struct MetricLength {
 private:
  double length_;

 public:
  explicit MetricLength(double length);
#ifdef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
  // Required for `bit_cast`.
  MetricLength() = default;
#endif

  double length() const { return length_; }
};

// Represents the area of the WKT geometry in square meters on the earth's
// surface. The value `NaN` is allowed here to express that the given polygon
// (or multipolygon, collection) is malformed semantically not syntactically
// s.t. we cannot determine an area. This is allowed as such polygons may still
// have some of the other properties of `GeometryInfo`.
struct MetricArea {
 private:
  double area_;

 public:
  explicit MetricArea(double area);
#ifdef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
  // Required for `bit_cast`.
  MetricArea() = default;
#endif

  double area() const { return area_; }

  bool isValid() const { return !std::isnan(area_); }
};

// The serialization format of a geometry literal. For now, only WKT literals
// are supported (in the future possibly e.g. GeoJSON or KML). The value zero
// is `WKT`, because all records written before this field was taken into use
// are WKT literals, see the rules at `GEOMETRY_INFO_VERSION`.
enum class SerializationType : uint16_t { WKT };

// Class for internal exception handling of errors from `s2geometry`.
class InvalidPolygonError : public std::runtime_error {
 public:
  explicit InvalidPolygonError()
      : std::runtime_error{
            "Computation encountered an invalid polygon geometry."} {}
};

// Forward declaration for concept
class GeometryInfo;

// Concept for the `RequestedInfo` template parameter: any of these types is
// allowed to be requested.
template <typename T>
CPP_concept RequestedInfoT =
    SameAsAny<T, GeometryInfo, Centroid, BoundingBox, GeometryType,
              SourceCrsType, NumGeometries, MetricLength, MetricArea,
              SerializationType>;

// Where the actual geometries are required, this type can be used.
using GeoPointOrWkt = std::variant<GeoPoint, std::string>;

// The version of the binary representation of `GeometryInfo`, which the
// `GeoVocabulary` writes to disk. The history of the versions that can still be
// read:
//
// 5: The bytes `[28, 32)` are padding with arbitrary content (they were only
//    zeroed explicitly in later builds of this version).
// 6: The bytes `[28, 32)` are the two 16-bit fields `sourceCrs_` (initially
//    reserved and zero) and `serializationType_`, see below.
//
// From version 6 on, the binary representation is append-only, so that adding
// a field does not invalidate existing indices:
//
// - Never change the order, size, encoding or meaning of an existing field.
// - A new field is appended at the end of the class. Bump the
//   `GEOMETRY_INFO_VERSION` and append the new `sizeof(GeometryInfo)` to
//   `GEOMETRY_INFO_SIZE_PER_VERSION`. When reading a record of an older
//   version, the bytes of the new field are zero (see `fromSerialized`).
// - A new field must not introduce implicit padding bytes (neither before it
//   nor at the end of the class due to its alignment), because they would be
//   written to disk with arbitrary content, such that they could never be
//   taken into use later (like the padding in version 5). If necessary, add
//   explicit reserved fields initialized to zero instead. Add the new field
//   to the `static_assert` in the constructor of `GeometryInfo`, which checks
//   this.
// - A reserved field can be taken into use without bumping the version, because
//   it is zero in all records written before.
// - Consequently, a zero value of a field that was added (or taken into use)
//   after version 5 must mean "not computed", and every reader of such a field
//   must handle this case, typically by computing the value from the literal.
//   Alternatively, zero may be a value that is correct for all records written
//   before (like `SerializationType::WKT`).
// - Only a change that violates these rules is index-breaking: bump the
//   `GEOMETRY_INFO_VERSION`, raise `MIN_LOADABLE_GEOMETRY_INFO_VERSION` to it,
//   and remove the handling of the dropped versions from `fromSerialized` and
//   `GEOMETRY_INFO_SIZE_PER_VERSION`.
constexpr uint64_t GEOMETRY_INFO_VERSION = 6;
constexpr uint64_t MIN_LOADABLE_GEOMETRY_INFO_VERSION = 5;

// The size of the binary representation of `GeometryInfo` for each loadable
// version, starting at `MIN_LOADABLE_GEOMETRY_INFO_VERSION`.
inline constexpr std::array<size_t, GEOMETRY_INFO_VERSION -
                                        MIN_LOADABLE_GEOMETRY_INFO_VERSION + 1>
    GEOMETRY_INFO_SIZE_PER_VERSION{48, 48};

// Return true iff a `GeometryInfo` of the given `version` can be read.
constexpr bool isLoadableGeometryInfoVersion(uint64_t version) {
  return version >= MIN_LOADABLE_GEOMETRY_INFO_VERSION &&
         version <= GEOMETRY_INFO_VERSION;
}

// The size of the binary representation of `GeometryInfo` for the given
// loadable `version`.
constexpr size_t geometryInfoSize(uint64_t version) {
  return GEOMETRY_INFO_SIZE_PER_VERSION.at(version -
                                           MIN_LOADABLE_GEOMETRY_INFO_VERSION);
}

// A geometry info object holds precomputed details on WKT literals.
// IMPORTANT: The binary representation of this class is written to disk by the
// `GeoVocabulary`. Every modification of its attributes must follow the rules
// at `GEOMETRY_INFO_VERSION` above.
class GeometryInfo {
 private:
  // `GeometryInfo` must ensure that its attributes' binary representation
  // cannot be all-zero. This is currently used by the disk serialization of
  // `GeoVocabulary` to represent invalid literals.
  EncodedBoundingBox boundingBox_;
  uint64_t geometryTypeAndCentroid_;
  uint32_t numGeometries_;
  // The CRS named by the IRI of the literal (before the projection to CRS84),
  // see `SourceCrsType`. This was the reserved field `reservedSourceCrs_` in
  // the first builds of version 6, so it is zero ("not computed") in their
  // records and in those of version 5 (see `fromSerialized`).
  SourceCrsType sourceCrs_;
  // The serialization format of the literal, see `SerializationType`.
  SerializationType serializationType_ = SerializationType::WKT;
  MetricLength metricLength_;
  MetricArea metricArea_;

  // TODO<ullingerc>: Implement the behavior for the following two
  // attributes
  //   int64_t parsedGeometryOffset_ = -1;

  static constexpr uint64_t bitMaskGeometryType =
      bitMaskForHigherBits(ValueId::numDatatypeBits);
  static constexpr uint64_t bitMaskCentroid =
      bitMaskForLowerBits(ValueId::numDataBits);

 public:
  GeometryInfo(uint8_t wktType, const BoundingBox& boundingBox,
               Centroid centroid, NumGeometries numGeometries,
               SourceCrsType sourceCrs, MetricLength metricLength,
               MetricArea metricArea);
#ifdef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
  // Required for `bit_cast`.
  GeometryInfo() = default;
#endif

  GeometryInfo(const GeometryInfo& other) = default;
  GeometryInfo& operator=(const GeometryInfo& other) = default;

  // Parse an arbitrary WKT literal and compute all attributes. Return
  // `std::nullopt` if `wkt` cannot be parsed (in the strict parsing mode).
  static std::optional<GeometryInfo> fromWktLiteral(std::string_view wkt);

  // Read a `GeometryInfo` from `bytes`, its binary representation of the given
  // loadable `version` (of size `geometryInfoSize(version)`). The fields that
  // the `version` did not have yet are zero.
  static GeometryInfo fromSerialized(ql::span<const uint8_t> bytes,
                                     uint64_t version);

  // Check if `wkt` (a WKT literal with quotes and datatype) is valid according
  // to the strict parsing mode of `pb_util`. This is a purely syntactic check.
  //
  // NOTE: A valid literal does not necessarily yield a geometry in
  // `fromWktLiteral`. In particular, `EMPTY` geometries (except `POINT EMPTY`,
  // which the strict mode rejects), an unsupported CRS IRI, and coordinates
  // out of range are all valid.
  static bool isValidWktStrict(std::string_view wkt);

  // Create geometry info for a GeoPoint object.
  static GeometryInfo fromGeoPoint(const GeoPoint& point);

  // Extract the WKT geometry type from geometryTypeAndCentroid_.
  GeometryType getWktType() const;

  // Parse an arbitrary WKT literal and return only the geometry type.
  static std::optional<GeometryType> getWktType(std::string_view wkt);

  // Get the source CRS type. Empty iff this object was read from an index that
  // predates `sourceCrs_` (stored as zero, see `fromSerialized`): then compute
  // it from the literal. Zero is unambiguous, because a literal with an
  // unsupported CRS has no `GeometryInfo` at all.
  std::optional<SourceCrsType> getSourceCrsType() const;

  // Parse an arbitrary WKT literal and return only the contained CRS type.
  static std::optional<SourceCrsType> getSourceCrsType(std::string_view wkt);

  // Extract centroid from geometryTypeAndCentroid_ and convert it to GeoPoint.
  Centroid getCentroid() const;

  // Parse an arbitrary WKT literal and compute only the centroid.
  static std::optional<Centroid> getCentroid(std::string_view wkt);

  // Convert the bounding box to GeoPoints.
  BoundingBox getBoundingBox() const;

  // Parse an arbitrary WKT literal and compute only the bounding box.
  static std::optional<BoundingBox> getBoundingBox(std::string_view wkt);

  // Extract the metric area.
  MetricArea getMetricArea() const;

  // Parse an arbitrary WKT literal and compute only the metric area.
  static std::optional<MetricArea> getMetricArea(std::string_view wkt);

  // Get the number of child geometries contained in this geometry.
  NumGeometries getNumGeometries() const;

  // Parse an arbitrary WKT literal and compute only the number of child
  // geometries.
  static std::optional<NumGeometries> getNumGeometries(std::string_view wkt);

  // Extract the length in meters.
  MetricLength getMetricLength() const;

  // Parse an arbitrary WKT literal and compute only the length in meters.
  static std::optional<MetricLength> getMetricLength(
      const std::string_view& wkt);

  // Get the serialization format of the literal.
  SerializationType getSerializationType() const;

  // Return the serialization format of an arbitrary WKT literal, which is
  // always `WKT`.
  static std::optional<SerializationType> getSerializationType(
      std::string_view wkt);

  // Extract the requested information from this object. Empty iff it is not
  // stored in this object (see `getSourceCrsType`).
  CPP_template(typename RequestedInfo = GeometryInfo)(
      requires RequestedInfoT<RequestedInfo>)
  std::optional<RequestedInfo> getRequestedInfo() const;

  // Parse the given WKT literal and compute only the requested information.
  CPP_template(typename RequestedInfo = GeometryInfo)(
      requires RequestedInfoT<RequestedInfo>)
  static std::optional<RequestedInfo> getRequestedInfo(std::string_view wkt);
};

// The binary representation of a `GeometryInfo` must not contain implicit
// padding, because the `GeoVocabulary` writes it to disk and padding bytes
// would depend on uninitialized memory. For a standard-layout class, the
// members are laid out in the order of their declaration, so there is no
// padding iff its size is the sum of the sizes of its members. That is checked
// in the constructor (see `GeometryInfo.cpp`), because the members are private.
static_assert(std::is_standard_layout_v<GeometryInfo>);

// For the disk serialization we require that a `GeometryInfo` is trivially
// copyable.
static_assert(std::is_trivially_copyable_v<GeometryInfo>);

// A new field must also be added to `GEOMETRY_INFO_SIZE_PER_VERSION`, see the
// rules at `GEOMETRY_INFO_VERSION`.
static_assert(GEOMETRY_INFO_SIZE_PER_VERSION.back() == sizeof(GeometryInfo));

// Return true iff `word` is a WKT literal (with quotes and datatype suffix).
// This is the criterion by which the `SplitGeoVocabulary` routes words into
// the geo vocabulary (see `GeoSplitFunc`).
inline bool isWktLiteral(std::string_view word) {
  return ql::starts_with(word, "\"") && ql::ends_with(word, GEO_LITERAL_SUFFIX);
}

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_GEOMETRYINFO_H
