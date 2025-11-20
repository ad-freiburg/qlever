// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include "engine/SpatialJoinCachedIndex.h"

#include <s2/mutable_s2shape_index.h>
#include <s2/s2polyline.h>

#include "engine/SpatialJoinAlgorithms.h"

// An instance of this type erased class holds the actual data for each
// `SpatialJoinCachedIndex`. It contains a `MutableS2ShapeIndex` for querying as
// well as the storage of the geometries since the index works on pointers to
// them. Furthermore, because the index does not support payload, the
// `shapeIndexToRow_` associates s2's shape ids with rows in the respective
// `IdTable`.
class SpatialJoinCachedIndexImpl {
 public:
  MutableS2ShapeIndex s2index_;
  std::vector<std::pair<S2Polyline, size_t>> lines_;

  // Construct the index, and return the mapping from shape indices to rows.
  ad_utility::HashMap<size_t, size_t> populate(ColumnIndex col,
                                               const IdTable& restable,
                                               const Index& index) {
    AD_CORRECTNESS_CHECK(lines_.empty());
    AD_CORRECTNESS_CHECK(s2index_.num_shape_ids() == 0);
    // Populate the index from the given `IdTable`
    lines_.reserve(restable.size());
    for (size_t row = 0; row < restable.size(); row++) {
      auto p = SpatialJoinAlgorithms::getPolyline(restable, row, col, index);
      if (p.has_value()) {
        // We need to store the geometries ourselves because the index takes a
        // pointer to them.
        lines_.emplace_back(std::move(p.value()), row);
      }
    }
    lines_.shrink_to_fit();
    return populateFromLines();
  }

  ad_utility::HashMap<size_t, size_t> populateFromLines() {
    ad_utility::HashMap<size_t, size_t> shapeIndexToRow;
    for (const auto& [line, row] : lines_) {
      auto shapeIndex =
          s2index_.Add(std::make_unique<S2Polyline::Shape>(&line));
      shapeIndexToRow[shapeIndex] = row;
    }

    // S2 adds geometries to its index data structure lazily and only updates
    // the data structure as soon as a lookup is performed. This is intended to
    // make many subsequent updates fast by not updating the data structure in
    // every step. However since we build the index only once and then use it
    // without making changes, we force the updating of its internal data
    // structure here to ensure good performance also for the first query.
    s2index_.ForceBuild();
    return shapeIndexToRow;
  }
};

// ____________________________________________________________________________
SpatialJoinCachedIndex::SpatialJoinCachedIndex(const Variable& geometryColumn,
                                               ColumnIndex col,
                                               const IdTable& restable,
                                               const Index& index)
    : geometryColumn_{geometryColumn},
      pimpl_{std::make_shared<SpatialJoinCachedIndexImpl>()} {
  shapeIndexToRow_ = pimpl_->populate(col, restable, index);
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
std::string SpatialJoinCachedIndex::serializeShapes() const {
  Encoder encoder;
  for (const auto& [line, row] : pimpl_->lines_) {
    line.Encode(&encoder);
  }
  return std::string{encoder.base(), encoder.length()};
}

// _____________________________________________________________________________
std::vector<size_t> SpatialJoinCachedIndex::serializeLineIndices() const {
  return ::ranges::to<std::vector>(pimpl_->lines_ | ranges::views::values);
}

// _____________________________________________________________________________
void SpatialJoinCachedIndex::populateFromSerialized(
    std::string_view serializedShapes, std::vector<size_t> lineIndices) {
  pimpl_ = std::make_shared<SpatialJoinCachedIndexImpl>();
  pimpl_->lines_.reserve(lineIndices.size());
  Decoder decoder(serializedShapes.data(), serializedShapes.size());
  for (size_t i = 0; i < lineIndices.size(); ++i) {
    pimpl_->lines_.emplace_back(S2Polyline{}, lineIndices[i]);
    bool success = pimpl_->lines_.back().first.Decode(&decoder);
    if (!success) {
      throw std::runtime_error("Could not decode serialized geometry.");
    }
  }
  shapeIndexToRow_ = pimpl_->populateFromLines();
}

// _____________________________________________________________________________
SpatialJoinCachedIndex::SpatialJoinCachedIndex(TagForSerialization)
    : geometryColumn_("?dummyCol"),
      pimpl_(std::make_shared<SpatialJoinCachedIndexImpl>()) {}
