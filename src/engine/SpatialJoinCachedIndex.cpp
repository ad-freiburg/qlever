// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include "engine/SpatialJoinCachedIndex.h"

#include <s2/mutable_s2shape_index.h>
#include <s2/s2polyline.h>

#include "../../cmake-build-gcc-13-relwithdebinfo/_deps/s2-src/src/s2/s2shapeutil_coding.h"
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

  // Construct the index, and return the mapping from shape indices to rows.
  ad_utility::HashMap<size_t, size_t> populate(ColumnIndex col,
                                               const IdTable& restable,
                                               const Index& index) {
    ad_utility::HashMap<size_t, size_t> shapeIndexToRow;
    AD_CORRECTNESS_CHECK(s2index_.num_shape_ids() == 0);
    // Populate the index from the given `IdTable`
    for (size_t row = 0; row < restable.size(); row++) {
      auto p = SpatialJoinAlgorithms::getPolyline(restable, row, col, index);
      if (p.has_value()) {
        auto shapeIndex =
            s2index_.Add(std::make_unique<S2Polyline::OwningShape>(
                std::make_unique<S2Polyline>(std::move(p.value()))));
        shapeIndexToRow[shapeIndex] = row;
      }
    }
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
  s2shapeutil::CompactEncodeTaggedShapes(pimpl_->s2index_, &encoder);
  pimpl_->s2index_.Encode(&encoder);
  return std::string{encoder.base(), encoder.length()};
}

// _____________________________________________________________________________
const SpatialJoinCachedIndex::ShapeIndexToRow&
SpatialJoinCachedIndex::serializeLineIndices() const {
  return shapeIndexToRow_;
}

// _____________________________________________________________________________
void SpatialJoinCachedIndex::populateFromSerialized(
    std::string_view serializedShapes, ShapeIndexToRow shapeIndexToRow) {
  shapeIndexToRow_ = std::move(shapeIndexToRow);
  pimpl_ = std::make_shared<SpatialJoinCachedIndexImpl>();
  Decoder decoder(serializedShapes.data(), serializedShapes.size());
  if (!pimpl_->s2index_.Init(&decoder,
                             s2shapeutil::FullDecodeShapeFactory(&decoder))) {
    throw std::runtime_error("Could not decode the serialized S2 index");
  }
  // To my understanding, the following call should be a noop, as the built
  // index remembers its structure when being decoded.
  pimpl_->s2index_.ForceBuild();
}

// _____________________________________________________________________________
SpatialJoinCachedIndex::SpatialJoinCachedIndex(TagForSerialization)
    : geometryColumn_("?dummyCol"),
      pimpl_(std::make_shared<SpatialJoinCachedIndexImpl>()) {}
