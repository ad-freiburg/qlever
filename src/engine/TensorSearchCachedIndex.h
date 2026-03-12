// Copyright 2026, Technical University of Graz, University of Padova
// Institute for Visual Computing, Department of Information Engineering
// Authors: Benedikt Kantz <benedikt.kantz@tugraz.at>

#ifndef QLEVER_SRC_ENGINE_TENSORSEARCHCACHEDINDEX_H
#define QLEVER_SRC_ENGINE_TENSORSEARCHCACHEDINDEX_H

#include <memory>
#include <variant>

#include "engine/idTable/IdTable.h"
#include "index/Index.h"
#include "rdfTypes/Variable.h"
#include "util/Serializer/Serializer.h"
#include "TensorSearchConfig.h"
#include "util/TensorData.h"
#include "annoylib.h"
#include "kissrandom.h"
class AnnoyIndexVariants;

// This class holds a `Annoy` Index that is created once by the named
// cached result mechanism and is then kept constant and persisted across
// queries.
class TensorSearchCachedIndex {
 private:
  // The `tensorColum_` indicates the variable name of the column from which
  // tensors are indexed.
  Variable tensorColum_;

  // This points to a class holding the actual index data structure. It is a `std::variant` over the different possible `AnnoyIndex` types, which differ in the used distance metric.
  std::optional<std::shared_ptr<AnnoyIndexVariants>> index_;

  // As `S2MutableShapeInex` doesn't support additional payloads, the
  //  `shapeIndexToRow_` associates s2's `shape ids` with row indices in the
  //  respective `IdTable` from which this `SpatialJoinCachedIndex` was created.
 public:
  using AnnoyIndexToRow = ad_utility::HashMap<size_t, size_t>;
  using AnnoyResult = std::pair<size_t, ad_utility::TensorData>;
  std::vector<AnnoyResult> findNN(const ad_utility::TensorData& query, size_t n) const;
 private:
  AnnoyIndexToRow tensorIndexToRow_;
  TensorSearchConfiguration config_;
  Variable tensorColumn_;
  size_t nTrees_;
  ssize_t nDims_;



 public:
  // Constructor that builds an index from the tensors in the given column in
  // the `IdTable`.
  TensorSearchCachedIndex(Variable tensorColumn, ColumnIndex col, const IdTable& restable, const Index& index, TensorSearchConfiguration config_);
  static TensorSearchCachedIndex fromKeyOrBuild(const std::string& key, Variable tensorColumn, ColumnIndex col, const IdTable& restable, const Index& index, TensorSearchConfiguration config);
  // Getters
  const Variable& getTensorColumn() const;
  std::shared_ptr<const AnnoyIndexVariants> getIndex() const;

  // From an `S2ShapeIndex` (returned by querying this index), obtain the
  // row index in the `IdTable` from which this index was created.
  // Note: For efficiency reasons (this might be called in a tight loop),
  // this function is inlined.
  size_t getRow(size_t shapeIndex) const {
    return tensorIndexToRow_.at(shapeIndex);
  }

  // Construct an empty, not yet valid index, s.t. it later can be filled via
  // `populateFromSerialized` below.
  struct TagForSerialization {};
  TensorSearchCachedIndex(TagForSerialization);

  // Serialize a `TensorSearchCachedIndex`. When reading from a serializer, then
  // the target `arg` has to be constructed upfront via the constructor that
  // takes a `TagForSerialization` (see above).
  AD_SERIALIZE_FRIEND_FUNCTION(TensorSearchCachedIndex) {
    serializer | arg.tensorColum_;
    if constexpr (ad_utility::serialization::WriteSerializer<S>) {
      serializer << arg.shapeIndexToRow_;
      serializer << arg.config_.algorithm_;
      serializer << arg.nTrees_;
      serializer << arg.nDims_;
      serializer << arg.serializeAnnoyIndex();
    } else {
      decltype(arg.serializeAnnoyIndex()) serializedAnnoy;
      serializer >> arg.shapeIndexToRow_;
      serializer >> arg.config_.algorithm_;
      serializer >> arg.nTrees_;
      serializer >> arg.nDims_;
      serializer >> serializedAnnoy;
      arg.populateFromSerialized(serializedAnnoy);
    }
  }

 private:
  // Serialize the `MutableS2ShapeIndex` as well as the contained shapes. This
  // is used by the serialization function above.
  std::string serializeAnnoyIndex() const;

  // Fill the contained `MutableS2ShapeIndex` from a string that has been
  // obtained via `serializeS2Index` previously. This function is only used by
  // the serialization function above.
  void populateFromSerialized(std::string_view serializedAnnoyIndex);

  AnnoyIndexToRow buildIndex(ColumnIndex col, const IdTable& restable, const Index& index);
};

#endif  // QLEVER_SRC_ENGINE_TENSORSEARCHCACHEDINDEX_H
