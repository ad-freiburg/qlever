// Copyright 2026, Technical University of Graz, University of Padova
// Institute for Visual Computing, Department of Information Engineering
// Authors: Benedikt Kantz <benedikt.kantz@tugraz.at>

#ifndef QLEVER_SRC_ENGINE_TENSORSEARCHCACHEDINDEX_H
#define QLEVER_SRC_ENGINE_TENSORSEARCHCACHEDINDEX_H


#include <faiss/IndexFlat.h>
#include <faiss/IndexIVFFlat.h>

#include <memory>
#include <variant>

#include "TensorSearchConfig.h"
#include "engine/idTable/IdTable.h"
#include "global/ValueId.h"
#include "index/Index.h"
#include "rdfTypes/TensorData.h"
#include "rdfTypes/Variable.h"
#include "util/Cache.h"
#include "util/Serializer/Serializer.h"
#include "util/Synchronized.h"

// This class holds a `Faiss` Index that is created once by the named
// cached result mechanism and is then kept constant and persisted across
// queries.
class TensorSearchCachedIndex {
 private:
  std::optional<std::shared_ptr<faiss::IndexFlat>> quant_;
  std::optional<std::shared_ptr<faiss::IndexIVFFlat>> index_;
 public:
  using FaissIndexToRow = ad_utility::HashMap<size_t, ValueId>;

  using FaissResult = std::pair<size_t, float>;
  std::vector<FaissResult> findNN(const ad_utility::TensorData& query,
                                  size_t n) const;

 private:
  FaissIndexToRow tensorIndexToRow_;
  TensorSearchConfiguration config_;
  size_t nTrees_;
  ssize_t dimSize_;

 public:
  // Constructor that builds an index from the tensors in the given column in
  // the `IdTable`.
  TensorSearchCachedIndex(ColumnIndex col, const IdTable& restable,
                          const Index& index,
                          TensorSearchConfiguration config_);
  TensorSearchCachedIndex(TensorSearchCachedIndex&&) noexcept = default;
  TensorSearchCachedIndex& operator=(TensorSearchCachedIndex&&) noexcept =
      default;
  static std::shared_ptr<const TensorSearchCachedIndex> fromKeyOrBuild(
      const std::string& key, ColumnIndex col, const IdTable& restable,
      const Index& index, TensorSearchConfiguration config);

  std::shared_ptr<const faiss::IndexIVFFlat> getIndex() const;

  // From an `S2ShapeIndex` (returned by querying this index), obtain the
  // row index in the `IdTable` from which this index was created.
  // Note: For efficiency reasons (this might be called in a tight loop),
  // this function is inlined.
  ValueId getRow(size_t faissIdx) const {
    return tensorIndexToRow_.at(faissIdx);
  }

 private:
  FaissIndexToRow buildIndex(ColumnIndex col, const IdTable& restable,
                              const Index& index);
};

#endif  // QLEVER_SRC_ENGINE_TENSORSEARCHCACHEDINDEX_H
