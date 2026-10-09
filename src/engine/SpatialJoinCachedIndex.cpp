// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include "engine/SpatialJoinCachedIndex.h"

#include <s2/mutable_s2shape_index.h>
#include <s2/s2error.h>
#include <s2/s2polyline.h>
#include <s2/s2shapeutil_coding.h>

#include <functional>
#include <numeric>

#include "backports/algorithm.h"
#include "index/ExportIds.h"
#include "rdfTypes/GeometryInfoHelpersImpl.h"
#include "util/GeoConverters.h"

namespace {
// Helper that collects the linestrings of some rows of an `IdTable` in a new
// `MutableS2ShapeIndex` (one segment of a `SpatialJoinCachedIndex`).
class SegmentBuilder {
  std::unique_ptr<MutableS2ShapeIndex> index_ =
      std::make_unique<MutableS2ShapeIndex>();

 public:
  // Add the geometry of the given `row` to the segment (simplified if
  // `simplificationErrorInMeters` has a value) and return its shape id, or
  // return `std::nullopt` (and add nothing) if the row contains no linestring.
  std::optional<int> addRow(const IdTableView<0>& restable, size_t row,
                            ColumnIndex col, const Index& index,
                            std::optional<double> simplificationErrorInMeters) {
    auto p = SpatialJoinCachedIndex::getPolyline(restable, row, col, index);
    if (!p.has_value()) {
      return std::nullopt;
    }
    if (simplificationErrorInMeters.has_value()) {
      p = geometryConverters::simplifyPolyline(
          p.value(), simplificationErrorInMeters.value());
    }
    return index_->Add(std::make_unique<S2Polyline::OwningShape>(
        std::make_unique<S2Polyline>(std::move(p.value()))));
  }

  // Return the number of shapes that have been added.
  int numShapes() const { return index_->num_shape_ids(); }

  // Finish the segment and return it. No more rows may be added afterwards.
  std::shared_ptr<const MutableS2ShapeIndex> finish() {
    // By default, the S2 indices are constructed lazily on the first query,
    // which then is slow. The following call avoids this.
    index_->ForceBuild();
    return std::shared_ptr<const MutableS2ShapeIndex>{std::move(index_)};
  }
};
}  // namespace

// ____________________________________________________________________________
uint64_t SpatialJoinCachedIndex::encodeShape(size_t segment, size_t shapeId) {
  AD_CONTRACT_CHECK(segment < MAX_NUM_SEGMENTS &&
                    shapeId < MAX_NUM_SHAPES_PER_SEGMENT);
  return (static_cast<uint64_t>(segment) << SHAPE_ID_BITS) |
         static_cast<uint64_t>(shapeId);
}

// ____________________________________________________________________________
std::optional<S2Polyline> SpatialJoinCachedIndex::getPolyline(
    const IdTableView<0>& restable, size_t row, ColumnIndex col,
    const Index& index) {
  auto id = restable.at(row, col);
  auto str = ql::exportIds::idToStringAndType(index, id, {});
  if (!str.has_value()) {
    return std::nullopt;
  }
  // The `lineFromWKT` function skips the part of the string before the first
  // opening bracket. The geometry type needs to be checked separately.
  if (ad_utility::detail::getWKTType(str.value().first) !=
      ad_utility::detail::WKTType::LINESTRING) {
    return std::nullopt;
  }
  auto line = ad_utility::detail::lineFromWKT<double>(str.value().first);
  return line.empty() ? std::nullopt
                      : std::optional{geometryConverters::toS2Polyline(line)};
}

// ____________________________________________________________________________
SpatialJoinCachedIndex::SpatialJoinCachedIndex(
    Variable geometryColumn, ColumnIndex col, const IdTableView<0>& restable,
    const Index& index, std::optional<double> simplificationErrorInMeters)
    : geometryColumn_{std::move(geometryColumn)},
      simplificationErrorInMeters_{simplificationErrorInMeters} {
  // This constructor builds an index that consists of a single segment.
  constexpr size_t segmentIdx = 0;
  SegmentBuilder builder;
  rowToShape_.assign(restable.size(), NO_SHAPE);
  for (size_t row = 0; row < restable.size(); row++) {
    auto shapeId =
        builder.addRow(restable, row, col, index, simplificationErrorInMeters_);
    if (shapeId.has_value()) {
      rowToShape_[row] =
          encodeShape(segmentIdx, static_cast<size_t>(shapeId.value()));
    }
  }
  segments_.push_back(builder.finish());
  computeShapeToRow();
}

// ____________________________________________________________________________
SpatialJoinCachedIndex::SpatialJoinCachedIndex(
    Variable geometryColumn, std::optional<double> simplificationErrorInMeters,
    Segments segments, std::vector<uint64_t> rowToShape)
    : geometryColumn_{std::move(geometryColumn)},
      simplificationErrorInMeters_{simplificationErrorInMeters},
      segments_{std::move(segments)},
      rowToShape_{std::move(rowToShape)} {
  computeShapeToRow();
}

// ____________________________________________________________________________
SpatialJoinCachedIndex SpatialJoinCachedIndex::fromLegacyFormat(
    Variable geometryColumn, const std::string& encodedSegment,
    const ad_utility::HashMap<size_t, size_t>& shapeToRow, size_t numRows) {
  constexpr size_t segmentIdx = 0;
  std::vector rowToShape(numRows, NO_SHAPE);
  for (const auto& [shapeId, row] : shapeToRow) {
    // The last condition catches a row that is referenced by more than one
    // shape.
    AD_CORRECTNESS_CHECK(row < numRows &&
                             shapeId < MAX_NUM_SHAPES_PER_SEGMENT &&
                             rowToShape[row] == NO_SHAPE,
                         "The serialized geo index is corrupt");
    rowToShape[row] = encodeShape(segmentIdx, shapeId);
  }
  Segments segments;
  segments.push_back(decodeSegment(encodedSegment));
  return SpatialJoinCachedIndex{std::move(geometryColumn), std::nullopt,
                                std::move(segments), std::move(rowToShape)};
}

// ____________________________________________________________________________
void SpatialJoinCachedIndex::computeShapeToRow() {
  shapeToRow_.clear();
  shapeToRow_.reserve(segments_.size());
  // Each shape has to be representable by `encodeShape`.
  AD_CORRECTNESS_CHECK(segments_.size() < MAX_NUM_SEGMENTS,
                       "The geo index has too many segments");
  for (const auto& segment : segments_) {
    AD_CORRECTNESS_CHECK(segment != nullptr);
    AD_CORRECTNESS_CHECK(static_cast<uint64_t>(segment->num_shape_ids()) <=
                             MAX_NUM_SHAPES_PER_SEGMENT,
                         "A segment of the geo index has too many shapes");
    shapeToRow_.emplace_back(static_cast<size_t>(segment->num_shape_ids()),
                             NO_ROW);
  }
  for (size_t row = 0; row < rowToShape_.size(); ++row) {
    uint64_t shape = rowToShape_[row];
    if (shape == NO_SHAPE) {
      continue;
    }
    auto [segment, shapeId] = decodeShape(shape);
    AD_CORRECTNESS_CHECK(
        segment < shapeToRow_.size() && shapeId < shapeToRow_[segment].size(),
        "A row of the geo index refers to a shape that does not exist");
    AD_CORRECTNESS_CHECK(shapeToRow_[segment][shapeId] == NO_ROW,
                         "A shape of the geo index is referenced by more than "
                         "one row");
    shapeToRow_[segment][shapeId] = row;
  }
}

// ____________________________________________________________________________
size_t SpatialJoinCachedIndex::numShapes() const {
  return ::ranges::accumulate(shapeToRow_, size_t{0}, std::plus{},
                              [](const auto& v) { return v.size(); });
}

// ____________________________________________________________________________
size_t SpatialJoinCachedIndex::numLiveShapes() const {
  return static_cast<size_t>(ql::ranges::count_if(
      rowToShape_, [](uint64_t shape) { return shape != NO_SHAPE; }));
}

// _____________________________________________________________________________
SpatialJoinCachedIndex SpatialJoinCachedIndex::withPermutedRows(
    ql::span<const size_t> newRowOfOldRow) const {
  AD_CONTRACT_CHECK(newRowOfOldRow.size() == rowToShape_.size());
  AD_CONTRACT_CHECK(ql::ranges::all_of(
      newRowOfOldRow,
      [numRows = newRowOfOldRow.size()](size_t row) { return row < numRows; }));
  std::vector rowToShape(rowToShape_.size(), NO_SHAPE);
  std::vector<bool> isTargetUsed(rowToShape_.size(), false);
  for (size_t oldRow = 0; oldRow < rowToShape_.size(); ++oldRow) {
    size_t newRow = newRowOfOldRow[oldRow];
    // A target that is used twice would silently drop the shape of one row.
    AD_CONTRACT_CHECK(!isTargetUsed[newRow],
                      "`newRowOfOldRow` must be a permutation, but two rows "
                      "are mapped to the same row");
    isTargetUsed[newRow] = true;
    rowToShape[newRow] = rowToShape_[oldRow];
  }
  return SpatialJoinCachedIndex{geometryColumn_, simplificationErrorInMeters_,
                                segments_, std::move(rowToShape)};
}

// _____________________________________________________________________________
SpatialJoinCachedIndex SpatialJoinCachedIndex::forUpdatedTable(
    const SpatialJoinCachedIndex& base, ql::span<const size_t> baseRowOfNewRow,
    const IdTableView<0>& newTable, ColumnIndex col, const Index& index) {
  std::vector<size_t> rowOrder(newTable.size());
  std::iota(rowOrder.begin(), rowOrder.end(), size_t{0});
  return forUpdatedTable(base, baseRowOfNewRow, newTable, col, index, rowOrder);
}

// _____________________________________________________________________________
SpatialJoinCachedIndex SpatialJoinCachedIndex::forUpdatedTable(
    const SpatialJoinCachedIndex& base, ql::span<const size_t> baseRowOfNewRow,
    const IdTableView<0>& newTable, ColumnIndex col, const Index& index,
    ql::span<const size_t> rowOrder) {
  AD_CONTRACT_CHECK(baseRowOfNewRow.size() == newTable.size());
  AD_CONTRACT_CHECK(rowOrder.size() == newTable.size());
  AD_CONTRACT_CHECK(base.segments_.size() + 1 < MAX_NUM_SEGMENTS,
                    "The geo index has too many segments");
  SegmentBuilder builder;
  const size_t newSegment = base.segments_.size();
  std::vector rowToShape(newTable.size(), NO_SHAPE);
  std::vector<bool> isVisited(newTable.size(), false);
  for (size_t row : rowOrder) {
    AD_CONTRACT_CHECK(row < newTable.size() && !isVisited[row],
                      "`rowOrder` must be a permutation of the rows");
    isVisited[row] = true;
    size_t baseRow = baseRowOfNewRow[row];
    if (baseRow != NO_ROW) {
      AD_CONTRACT_CHECK(baseRow < base.numRows());
      rowToShape[row] = base.rowToShape_[baseRow];
      continue;
    }
    auto shapeId = builder.addRow(newTable, row, col, index,
                                  base.simplificationErrorInMeters_);
    if (shapeId.has_value()) {
      rowToShape[row] =
          encodeShape(newSegment, static_cast<size_t>(shapeId.value()));
    }
  }
  Segments segments = base.segments_;
  if (builder.numShapes() > 0) {
    segments.push_back(builder.finish());
  }
  // NOTE: The constructor checks that no shape is referenced twice, which
  // covers a base row that occurs more than once in `baseRowOfNewRow`.
  return SpatialJoinCachedIndex{base.geometryColumn_,
                                base.simplificationErrorInMeters_,
                                std::move(segments), std::move(rowToShape)};
}

// _____________________________________________________________________________
ad_utility::HashMap<size_t, size_t> SpatialJoinCachedIndex::legacyShapeToRow()
    const {
  AD_CONTRACT_CHECK(shapeToRow_.size() == 1);
  ad_utility::HashMap<size_t, size_t> result;
  for (const auto& [shapeId, row] :
       ::ranges::views::enumerate(shapeToRow_.at(0))) {
    if (row != NO_ROW) {
      result[shapeId] = row;
    }
  }
  return result;
}

// ____________________________________________________________________________
std::string SpatialJoinCachedIndex::encodeSegment(
    const MutableS2ShapeIndex& segment) {
  Encoder encoder;
  s2shapeutil::CompactEncodeTaggedShapes(segment, &encoder);
  segment.Encode(&encoder);
  return std::string{encoder.base(), encoder.length()};
}

// _____________________________________________________________________________
std::shared_ptr<const MutableS2ShapeIndex>
SpatialJoinCachedIndex::decodeSegment(std::string_view encoded) {
  auto segment = std::make_shared<MutableS2ShapeIndex>();
  Decoder decoder(encoded.data(), encoded.size());
  S2Error error;
  bool success = segment->Init(
      &decoder, s2shapeutil::FullDecodeShapeFactory(&decoder, error));
  AD_CORRECTNESS_CHECK(success && error.ok(),
                       "Initializing the S2 index from its serialized form "
                       "failed, probably the input data is corrupt");
  // We call `ForceBuild` when initializing the index, and the serialization
  // preserves the index structure, so the following assertion holds, which
  // ensures that the index is ready for (cheap) usage by queries after
  // deserializing it.
  AD_CORRECTNESS_CHECK(segment->is_fresh());
  return segment;
}
