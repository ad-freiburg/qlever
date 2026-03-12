// Copyright 2026, Technical University of Graz, University of Padova
// Institute for Visual Computing, Department of Information Engineering
// Authors: Benedikt Kantz <benedikt.kantz@tugraz.at>

#ifndef QLEVER_SRC_ENGINE_TENSORSEARCHCACHEDINDEX_H
#define QLEVER_SRC_ENGINE_TENSORSEARCHCACHEDINDEX_H

#include <memory>
#include <variant>

#include "engine/idTable/IdTable.h"
#include "index/Index.h"
#include "global/ValueId.h"
#include "rdfTypes/Variable.h"
#include "util/Serializer/Serializer.h"
#include "TensorSearchConfig.h"
#include "util/TensorData.h"
#include "util/Cache.h"
#include "annoylib.h"
#include "kissrandom.h"

using AnnoyIndexVariants =
    std::variant<Annoy::AnnoyIndex<size_t, float, Annoy::Euclidean, Annoy::Kiss32Random,
                            Annoy::AnnoyIndexMultiThreadedBuildPolicy>,
                 Annoy::AnnoyIndex<size_t, float, Annoy::Angular, Annoy::Kiss32Random,
                            Annoy::AnnoyIndexMultiThreadedBuildPolicy>,
                 Annoy::AnnoyIndex<size_t, float, Annoy::Hamming, Annoy::Kiss32Random,
                            Annoy::AnnoyIndexMultiThreadedBuildPolicy>,
                 Annoy::AnnoyIndex<size_t, float, Annoy::Manhattan, Annoy::Kiss32Random,
                            Annoy::AnnoyIndexMultiThreadedBuildPolicy>>;

// This class holds a `Annoy` Index that is created once by the named
// cached result mechanism and is then kept constant and persisted across
// queries.
class TensorSearchCachedIndex {
 private:
  // This points to a class holding the actual index data structure. It is a `std::variant` over the different possible `AnnoyIndex` types, which differ in the used distance metric.
  std::optional<std::shared_ptr<AnnoyIndexVariants>> index_;

  // As `S2MutableShapeInex` doesn't support additional payloads, the
  //  `shapeIndexToRow_` associates s2's `shape ids` with row indices in the
  //  respective `IdTable` from which this `SpatialJoinCachedIndex` was created.
 public:
  using AnnoyIndexToRow = ad_utility::HashMap< size_t, ValueId>;
  using AnnoyResult = size_t;
  std::vector<AnnoyResult> findNN(const ad_utility::TensorData& query, size_t n) const;
  struct ValueSizeGetter {
    ad_utility::MemorySize operator()(const TensorSearchCachedIndex&) const {
      return ad_utility::MemorySize::bytes(1);
    }
  };
 
  // We use an LRU cache, where the key is the name of the cached result.
  using Key = std::string;
  using Cache = ad_utility::LRUCache<Key, TensorSearchCachedIndex, ValueSizeGetter>;
 private:
  AnnoyIndexToRow tensorIndexToRow_;
  TensorSearchConfiguration config_;
  size_t nTrees_;
  ssize_t nDims_;
  
// See NamedResultCache.h
  static ad_utility::Synchronized<Cache> cache_;

 public:
 
  // Constructor that builds an index from the tensors in the given column in
  // the `IdTable`.
  TensorSearchCachedIndex(ColumnIndex col, const IdTable& restable, const Index& index, TensorSearchConfiguration config_);
  TensorSearchCachedIndex(TensorSearchCachedIndex&&) noexcept = default;
  TensorSearchCachedIndex& operator=(TensorSearchCachedIndex&&) noexcept = default;
  static TensorSearchCachedIndex fromKeyOrBuild(const std::string& key, ColumnIndex col, const IdTable& restable, const Index& index, TensorSearchConfiguration config);

  std::shared_ptr<const AnnoyIndexVariants> getIndex() const;

  // From an `S2ShapeIndex` (returned by querying this index), obtain the
  // row index in the `IdTable` from which this index was created.
  // Note: For efficiency reasons (this might be called in a tight loop),
  // this function is inlined.
  ValueId getRow(size_t annoyIdx) const {
    return tensorIndexToRow_.at(annoyIdx);
  }

 private:

  AnnoyIndexToRow buildIndex(ColumnIndex col, const IdTable& restable, const Index& index);
};

#endif  // QLEVER_SRC_ENGINE_TENSORSEARCHCACHEDINDEX_H
