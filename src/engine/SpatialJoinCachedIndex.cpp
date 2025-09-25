// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include "engine/SpatialJoinCachedIndex.h"

#include <s2/mutable_s2shape_index.h>
#include <s2/s2polyline.h>

#include "engine/SpatialJoinAlgorithms.h"

// ____________________________________________________________________________
SpatialJoinCachedIndex::SpatialJoinCachedIndex(const Variable& geometryColumn,
                                               ColumnIndex col,
                                               const IdTable* restable,
                                               const Index& index)
    : geometryColumn_{geometryColumn},
      s2index_{std::make_shared<MutableS2ShapeIndex>()},
      lines_{std::make_shared<std::vector<std::pair<S2Polyline, size_t>>>()} {
  // Populate the index from the given `IdTable`
  lines_->reserve(restable->size());
  for (size_t row = 0; row < restable->size(); row++) {
    auto p = SpatialJoinAlgorithms::getPolyline(restable, row, col, index);
    if (p.has_value()) {
      // We need to store the geometries ourselves because the index takes a
      // pointer to them.
      lines_->emplace_back(std::move(p.value()), row);
    }
  }
  lines_->shrink_to_fit();
  for (const auto& [line, row] : *lines_) {
    auto shapeIndex = s2index_->Add(std::make_unique<S2Polyline::Shape>(&line));
    shapeIndexToRow_[shapeIndex] = row;
  }
  // TODO<ullingerc> Run query against index to force building
};
