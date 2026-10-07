// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_SPATIALJOINCACHEDINDEX_H
#define QLEVER_SRC_ENGINE_SPATIALJOINCACHEDINDEX_H

#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "backports/span.h"
#include "engine/idTable/IdTable.h"
#include "index/Index.h"
#include "rdfTypes/Variable.h"
#include "util/HashMap.h"
#include "util/Serializer/SerializeHashMap.h"
#include "util/Serializer/SerializeString.h"
#include "util/Serializer/SerializeVector.h"
#include "util/Serializer/Serializer.h"

// Forward declarations
class MutableS2ShapeIndex;
class S2Polyline;

// This class holds a set of `MutableS2ShapeIndex`es (called "segments") that
// are created once by the named cached result mechanism and are then kept
// constant and persisted across queries. Each segment is immutable once it is
// built and can be shared between several `SpatialJoinCachedIndex`es. A
// `SpatialJoinCachedIndex` that was created from the geometries of an
// `IdTable` can be cheaply adapted to a modified version of that table via
// `extend` (which reuses all existing segments and only builds one new segment
// for the new rows).
//
// As `MutableS2ShapeIndex` doesn't support additional payloads, the
// association between the rows of the `IdTable` and the shapes in the segments
// is stored in `rowToShape_` (and its inverse `shapeToRow_`). A shape of a
// segment that is not referenced by any row is "dead" (the row has been
// deleted), it is skipped by queries.
class SpatialJoinCachedIndex {
 public:
  using Segments = std::vector<std::shared_ptr<const MutableS2ShapeIndex>>;

  // The value in `rowToShape_` for a row that has no shape (for example
  // because it contains no linestring).
  static constexpr uint64_t NO_SHAPE = std::numeric_limits<uint64_t>::max();

  // The value in `shapeToRow_` (and the value of `baseRowOfNewRow` in `extend`)
  // for a shape that is not referenced by any row (and for a row that has no
  // counterpart in the base index, respectively).
  static constexpr size_t NO_ROW = std::numeric_limits<size_t>::max();

 private:
  // The `geometryColumn_` indicates the variable name of the column from which
  // geometries are indexed.
  Variable geometryColumn_;

  // The maximum error in meters of the Douglas-Peucker simplification that was
  // applied to the geometries before indexing them (`std::nullopt` means no
  // simplification). It is stored s.t. `extend` can simplify new geometries in
  // exactly the same way.
  std::optional<double> simplificationErrorInMeters_;

  // The immutable shape indices.
  Segments segments_;

  // For each row of the `IdTable` from which this index was created, the shape
  // that represents the geometry of the row, encoded as `(segment << 32) |
  // shapeId`, or `NO_SHAPE` if the row has no shape.
  std::vector<uint64_t> rowToShape_;

  // For each segment, and for each shape id in that segment, the row of the
  // `IdTable` that references the shape, or `NO_ROW` if no row references it.
  // This is the inverse of `rowToShape_` and is computed from it.
  std::vector<std::vector<size_t>> shapeToRow_;

 public:
  // Constructor that builds an index (consisting of a single segment) from the
  // geometries in the given column in the `IdTable`. Currently only line
  // strings are supported for the experimental S2 point polyline algorithm. If
  // `simplificationErrorInMeters` has a value, geometries are simplified using
  // the Douglas-Peucker algorithm with the given maximum error in meters before
  // indexing; `std::nullopt` means no simplification.
  SpatialJoinCachedIndex(
      Variable geometryColumn, ColumnIndex col, const IdTableView<0>& restable,
      const Index& index,
      std::optional<double> simplificationErrorInMeters = std::nullopt);

  // Getters
  const Variable& getGeometryColumn() const { return geometryColumn_; }
  const std::optional<double>& simplificationErrorInMeters() const {
    return simplificationErrorInMeters_;
  }

  // Return all segments of this index. A query has to be run on each of them.
  const Segments& segments() const { return segments_; }

  // Return the number of rows of the `IdTable` that this index refers to.
  size_t numRows() const { return rowToShape_.size(); }

  // Return the number of segments.
  size_t numSegments() const { return segments_.size(); }

  // Return the total number of shapes in all segments, including the dead
  // ones.
  size_t numShapes() const;

  // Return the number of shapes that are referenced by a row, i.e. the number
  // of shapes that are not dead.
  size_t numLiveShapes() const;

  // From a shape id returned by querying the segment with the given index,
  // obtain the row index in the `IdTable` from which this index was created.
  // Return `std::nullopt` if the shape is dead.
  // Note: For efficiency reasons (this might be called in a tight loop), this
  // function is inlined.
  std::optional<size_t> getRow(size_t segment, int shapeId) const {
    size_t row = shapeToRow_[segment][static_cast<size_t>(shapeId)];
    return row == NO_ROW ? std::nullopt : std::optional<size_t>{row};
  }

  // Return a copy of this index for the case that the rows of the `IdTable`
  // from which this index was created are permuted, where `newRowOfOldRow[r]`
  // is the row to which the row `r` was moved. The contained (immutable)
  // segments are shared with this index, only the mapping between shapes and
  // rows differs. `newRowOfOldRow` has to contain one entry for each row of
  // this index.
  //
  // NOTE: This is currently required when an `IdTable` that contains
  // `LocalVocabEntry`s is serialized into a blob, because its rows are then
  // rewritten and sorted again, see `rewriteToSecondaryVocab` in
  // `NamedCacheSecondaryVocabRewriter.h`.
  SpatialJoinCachedIndex withPermutedRows(
      ql::span<const size_t> newRowOfOldRow) const;

  // Return an index for the rows of `newTable` (in the column `col`) that
  // reuses all segments of `base`. The vector `baseRowOfNewRow` contains for
  // each row of `newTable` the row of the table of `base` that it corresponds
  // to (it inherits the shape of that row), or `NO_ROW` if the row is new.
  // The new rows that contain a linestring are indexed in one new segment
  // (using the simplification of `base`). A row of `base` that no row of
  // `newTable` refers to becomes a dead shape. Each row of `base` may be
  // referred to at most once.
  static SpatialJoinCachedIndex extend(const SpatialJoinCachedIndex& base,
                                       ql::span<const size_t> baseRowOfNewRow,
                                       const IdTableView<0>& newTable,
                                       ColumnIndex col, const Index& index);

  // Retrieves and parses a line string from the given cell of an `IdTable`
  // and converts it to an `S2Polyline`. Used when populating the index above.
  // This function is only `public` for testing purposes and should otherwise
  // not be used outside of this class.
  static std::optional<S2Polyline> getPolyline(const IdTableView<0>& restable,
                                               size_t row, ColumnIndex col,
                                               const Index& index);

  // Write this index to the `serializer` in the format of version 2 (see
  // `readFromSerializer`). The result only depends on the logical content of
  // the index, in particular, it is deterministic. The layout is:
  //
  //   Variable geometryColumn_
  //   uint8_t hasSimplification
  //   double simplificationErrorInMeters  (only if hasSimplification == 1)
  //   uint64_t numSegments
  //   for each segment: std::string (the encoded `MutableS2ShapeIndex`)
  //   std::vector<uint64_t> rowToShape_   (aligned raw array)
  CPP_template(typename Serializer)(
      requires ad_utility::serialization::WriteSerializer<
          Serializer>) void writeToSerializer(Serializer& serializer) const {
    serializer << geometryColumn_;
    serializer << static_cast<uint8_t>(
        simplificationErrorInMeters_.has_value());
    if (simplificationErrorInMeters_.has_value()) {
      serializer << simplificationErrorInMeters_.value();
    }
    serializer << static_cast<uint64_t>(segments_.size());
    for (const auto& segment : segments_) {
      serializer << encodeSegment(*segment);
    }
    serializer << rowToShape_;
  }

  // Read an index from the `serializer` that was written with the given
  // `entriesFormatVersion` (see `NamedResultCacheSerializer.h`). The version
  // 2 is the format written by `writeToSerializer`. The version 1 is the
  // legacy format that consists of the geometry column, one string with the
  // encoded `MutableS2ShapeIndex`, and a hash map from shape ids to rows; it is
  // converted to an index with one segment and no simplification. `numRows` is
  // the number of rows of the `IdTable` that the index refers to.
  CPP_template(typename Serializer)(
      requires ad_utility::serialization::ReadSerializer<
          Serializer>) static SpatialJoinCachedIndex
      readFromSerializer(Serializer& serializer, size_t numRows,
                         uint16_t entriesFormatVersion) {
    AD_CONTRACT_CHECK(entriesFormatVersion == 1 || entriesFormatVersion == 2);
    Variable geometryColumn{"?dummyCol"};
    serializer >> geometryColumn;
    if (entriesFormatVersion == 1) {
      std::string encodedSegment;
      serializer >> encodedSegment;
      ad_utility::HashMap<size_t, size_t> shapeToRow;
      serializer >> shapeToRow;
      std::vector<uint64_t> rowToShape(numRows, NO_SHAPE);
      for (const auto& [shapeId, row] : shapeToRow) {
        AD_CORRECTNESS_CHECK(row < numRows && shapeId < (1ULL << 32),
                             "The serialized geo index is corrupt");
        rowToShape[row] = shapeId;
      }
      Segments segments;
      segments.push_back(decodeSegment(encodedSegment));
      return SpatialJoinCachedIndex{std::move(geometryColumn), std::nullopt,
                                    std::move(segments), std::move(rowToShape)};
    }
    uint8_t hasSimplification;
    serializer >> hasSimplification;
    std::optional<double> simplification;
    if (hasSimplification != 0) {
      double value;
      serializer >> value;
      simplification = value;
    }
    uint64_t numSegments;
    serializer >> numSegments;
    Segments segments;
    for (uint64_t i = 0; i < numSegments; ++i) {
      std::string encodedSegment;
      serializer >> encodedSegment;
      segments.push_back(decodeSegment(encodedSegment));
    }
    std::vector<uint64_t> rowToShape;
    serializer >> rowToShape;
    AD_CORRECTNESS_CHECK(rowToShape.size() == numRows,
                         "The serialized geo index is corrupt");
    return SpatialJoinCachedIndex{std::move(geometryColumn), simplification,
                                  std::move(segments), std::move(rowToShape)};
  }

 private:
  // Construct from the given parts, and compute `shapeToRow_` from
  // `rowToShape` (see `computeShapeToRow`).
  SpatialJoinCachedIndex(Variable geometryColumn,
                         std::optional<double> simplificationErrorInMeters,
                         Segments segments, std::vector<uint64_t> rowToShape);

  // Compute `shapeToRow_` from `segments_` and `rowToShape_`. Throw an
  // exception if `rowToShape_` refers to a segment or shape that does not
  // exist, or if a shape is referenced by more than one row.
  void computeShapeToRow();

  // Serialize the `MutableS2ShapeIndex` as well as the contained shapes.
  static std::string encodeSegment(const MutableS2ShapeIndex& segment);

  // Create a `MutableS2ShapeIndex` from a string that has been obtained via
  // `encodeSegment` previously.
  static std::shared_ptr<const MutableS2ShapeIndex> decodeSegment(
      std::string_view encoded);
};

#endif  // QLEVER_SRC_ENGINE_SPATIALJOINCACHEDINDEX_H
