// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include "engine/SpatialJoinCachedIndex.h"

#include <s2/mutable_s2shape_index.h>
#include <s2/s2error.h>
#include <s2/s2polyline.h>
#include <s2/s2shapeutil_coding.h>

#include "index/ExportIds.h"
#include "rdfTypes/GeometryInfoHelpersImpl.h"
#include "util/GeoConverters.h"

// An instance of this type erased class holds the actual data for each
// `SpatialJoinCachedIndex`. It contains a `MutableS2ShapeIndex` for querying as
// well as the storage of the geometries since the index works on pointers to
// them. Furthermore, because the index does not support payload, the
// `shapeIndexToRow_` associates s2's shape ids with rows in the respective
// `IdTable`.
class SpatialJoinCachedIndexImpl {
 public:
  MutableS2ShapeIndex s2index_;
  using ShapeIndexToRow = SpatialJoinCachedIndex::ShapeIndexToRow;

  // Construct the index, and return the mapping from shape indices to rows.
  // If `simplificationErrorInMeters` has a value, polyline geometries are
  // simplified before indexing; `std::nullopt` skips simplification.
  ShapeIndexToRow populate(ColumnIndex col, const IdTableView<0>& restable,
                           const Index& index,
                           std::optional<double> simplificationErrorInMeters) {
    ShapeIndexToRow shapeIndexToRow;
    AD_CORRECTNESS_CHECK(s2index_.num_shape_ids() == 0);
    for (size_t row = 0; row < restable.size(); row++) {
      auto p = SpatialJoinCachedIndex::getPolyline(restable, row, col, index);
      if (!p.has_value()) {
        continue;
      }
      if (simplificationErrorInMeters.has_value()) {
        p = geometryConverters::simplifyPolyline(
            p.value(), simplificationErrorInMeters.value());
      }
      auto shapeIndex = s2index_.Add(std::make_unique<S2Polyline::OwningShape>(
          std::make_unique<S2Polyline>(std::move(p.value()))));
      shapeIndexToRow[shapeIndex] = row;
    }
    // By default, the S2 indices are constructed lazily on the first query,
    // which then is slow. The following call avoids this.
    s2index_.ForceBuild();
    return shapeIndexToRow;
  }
};

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
      pimpl_{std::make_shared<SpatialJoinCachedIndexImpl>()} {
  shapeIndexToRow_ =
      pimpl_->populate(col, restable, index, simplificationErrorInMeters);
}

// ____________________________________________________________________________
const Variable& SpatialJoinCachedIndex::getGeometryColumn() const {
  return geometryColumn_;
}

// ____________________________________________________________________________
std::shared_ptr<const MutableS2ShapeIndex> SpatialJoinCachedIndex::getIndex()
    const {
  return {pimpl_, &pimpl_->s2index_};
}

// ____________________________________________________________________________
std::string SpatialJoinCachedIndex::serializeS2Index() const {
  Encoder encoder;
  s2shapeutil::CompactEncodeTaggedShapes(pimpl_->s2index_, &encoder);
  pimpl_->s2index_.Encode(&encoder);
  return std::string{encoder.base(), encoder.length()};
}

// _____________________________________________________________________________
void SpatialJoinCachedIndex::populateFromSerialized(
    std::string_view serializedShapes) {
  AD_CORRECTNESS_CHECK(pimpl_ != nullptr);
  Decoder decoder(serializedShapes.data(), serializedShapes.size());
  S2Error error;
  bool success = pimpl_->s2index_.Init(
      &decoder, s2shapeutil::FullDecodeShapeFactory(&decoder, error));
  AD_CORRECTNESS_CHECK(success && error.ok(),
                       "Initializing the S2 index from its serialized form "
                       "failed, probably the input data is corrupt");
  // We call `ForceBuild` when initializing the index, and the serialization
  // preserves the index structure, so the following assertion holds, which
  // ensures that the index is ready for (cheap) usage by queries after
  // deserializing it.
  AD_CORRECTNESS_CHECK(pimpl_->s2index_.is_fresh());
}

// _____________________________________________________________________________
SpatialJoinCachedIndex::SpatialJoinCachedIndex(TagForSerialization)
    : geometryColumn_("?dummyCol"),
      pimpl_(std::make_shared<SpatialJoinCachedIndexImpl>()) {}
