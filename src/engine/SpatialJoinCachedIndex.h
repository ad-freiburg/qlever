// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_SPATIALJOINCACHEDINDEX_H
#define QLEVER_SRC_ENGINE_SPATIALJOINCACHEDINDEX_H

#include <memory>

#include "engine/idTable/IdTable.h"
#include "index/Index.h"
#include "rdfTypes/Variable.h"
#include "util/Serializer/Serializer.h"

// Forward declarations
class MutableS2ShapeIndex;
class SpatialJoinCachedIndexImpl;

// This class holds a `MutableS2ShapeIndex` that is created once by the named
// cached result mechanism and is then kept constant and persisted across
// queries.
class SpatialJoinCachedIndex {
 private:
  // The `geometryColumn_` indicates the variable name of the column from which
  // geometries are indexed.
  Variable geometryColumn_;

  // This points to a class holding the actual index data structure along with
  // information necessary to use it. See more details in the `.cpp` file.
  std::shared_ptr<SpatialJoinCachedIndexImpl> pimpl_;

  // As `S2MutableShapeInex` doesn't support additional payloads, the
  //  `shapeIndexToRow_` associates s2's `shape ids` with row indices in the
  //  respective `IdTable` from which this `SpatialJoinCachedIndex` was created.
 public:
  using ShapeIndexToRow = ad_utility::HashMap<size_t, size_t>;

 private:
  ShapeIndexToRow shapeIndexToRow_;

 public:
  // Constructor that builds an index from the geometries in the given column in
  // the `IdTable`. Currently only line strings are supported for the
  // experimental S2 point polyline algorithm.
  SpatialJoinCachedIndex(Variable geometryColumn, ColumnIndex col,
                         const IdTable& restable, const Index& index);

  // Getters
  const Variable& getGeometryColumn() const;
  std::shared_ptr<const MutableS2ShapeIndex> getIndex() const;

  // From an `S2ShapeIndex` (returned by querying this index), obtain the
  // row index in the `IdTable` from which this index was created.
  // Note: For efficiency reasons (this might be called in a tight loop),
  // this function is inlined.
  size_t getRow(size_t shapeIndex) const {
    return shapeIndexToRow_.at(shapeIndex);
  }

  // Construct an empty, not yet valid index, s.t. it later can be filled via
  // `populateFromSerialized` below.
  struct TagForSerialization {};
  SpatialJoinCachedIndex(TagForSerialization);

  // Serialize a `SpatialJoinCachedIndex`. When reading from a serializer, then
  // the target `arg` has to be constructed upfront via the constructor that
  // takes a `TagForSerialization` (see above).
  AD_SERIALIZE_FRIEND_FUNCTION(SpatialJoinCachedIndex) {
    serializer | arg.geometryColumn_;
    if constexpr (ad_utility::serialization::WriteSerializer<S>) {
      serializer << arg.serializeS2Index();
      serializer << arg.shapeIndexToRow_;
    } else {
      decltype(arg.serializeS2Index()) serializedShapes;
      serializer >> serializedShapes;
      serializer >> arg.shapeIndexToRow_;
      arg.populateFromSerialized(serializedShapes);
    }
  }

 private:
  // Serialize the `MutableS2ShapeIndex` as well as the contained shapes. This
  // is used by the serialization function above.
  std::string serializeS2Index() const;

  // Fill the contained `MutableS2ShapeIndex` from a string that has been
  // obtained via `serializeS2Index` previously. This function is only used by
  // the serialization function above.
  void populateFromSerialized(std::string_view serializedS2Index);
};

#endif  // QLEVER_SRC_ENGINE_SPATIALJOINCACHEDINDEX_H
