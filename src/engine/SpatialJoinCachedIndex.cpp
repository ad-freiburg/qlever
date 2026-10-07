// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include "engine/SpatialJoinCachedIndex.h"

#include <s2/mutable_s2shape_index.h>
#include <s2/s2error.h>
#include <s2/s2polyline.h>
#include <s2/s2shapeutil_coding.h>

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

// Combine the segment and the shape id to the format used by `rowToShape_`.
uint64_t makeShape(size_t segment, int shapeId) {
  return (static_cast<uint64_t>(segment) << 32) |
         static_cast<uint32_t>(shapeId);
}
}  // namespace

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
  SegmentBuilder builder;
  rowToShape_.assign(restable.size(), NO_SHAPE);
  for (size_t row = 0; row < restable.size(); row++) {
    auto shapeId =
        builder.addRow(restable, row, col, index, simplificationErrorInMeters_);
    if (shapeId.has_value()) {
      rowToShape_[row] = makeShape(0, shapeId.value());
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
void SpatialJoinCachedIndex::computeShapeToRow() {
  shapeToRow_.clear();
  shapeToRow_.reserve(segments_.size());
  for (const auto& segment : segments_) {
    AD_CORRECTNESS_CHECK(segment != nullptr);
    shapeToRow_.emplace_back(static_cast<size_t>(segment->num_shape_ids()),
                             NO_ROW);
  }
  for (size_t row = 0; row < rowToShape_.size(); ++row) {
    uint64_t shape = rowToShape_[row];
    if (shape == NO_SHAPE) {
      continue;
    }
    size_t segment = shape >> 32;
    size_t shapeId = shape & 0xFFFFFFFFu;
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
  size_t result = 0;
  for (const auto& shapeToRow : shapeToRow_) {
    result += shapeToRow.size();
  }
  return result;
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
  std::vector<uint64_t> rowToShape(rowToShape_.size(), NO_SHAPE);
  for (size_t oldRow = 0; oldRow < rowToShape_.size(); ++oldRow) {
    rowToShape[newRowOfOldRow[oldRow]] = rowToShape_[oldRow];
  }
  return SpatialJoinCachedIndex{geometryColumn_, simplificationErrorInMeters_,
                                segments_, std::move(rowToShape)};
}

// _____________________________________________________________________________
SpatialJoinCachedIndex SpatialJoinCachedIndex::extend(
    const SpatialJoinCachedIndex& base, ql::span<const size_t> baseRowOfNewRow,
    const IdTableView<0>& newTable, ColumnIndex col, const Index& index) {
  AD_CONTRACT_CHECK(baseRowOfNewRow.size() == newTable.size());
  SegmentBuilder builder;
  const size_t newSegment = base.segments_.size();
  std::vector<uint64_t> rowToShape(newTable.size(), NO_SHAPE);
  for (size_t row = 0; row < newTable.size(); ++row) {
    size_t baseRow = baseRowOfNewRow[row];
    if (baseRow != NO_ROW) {
      AD_CONTRACT_CHECK(baseRow < base.numRows());
      rowToShape[row] = base.rowToShape_[baseRow];
      continue;
    }
    auto shapeId = builder.addRow(newTable, row, col, index,
                                  base.simplificationErrorInMeters_);
    if (shapeId.has_value()) {
      rowToShape[row] = makeShape(newSegment, shapeId.value());
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
