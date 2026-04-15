// Copyright 2026, Technical University of Graz, University of Padova
// Institute for Visual Computing, Department of Information Engineering
// Authors: Benedikt Kantz <benedikt.kantz@tugraz.at>

#include "engine/TensorSearchCachedIndex.h"

#include <variant>

#include "engine/ExportQueryExecutionTrees.h"
#include "engine/TensorSearch.h"
#include "global/Constants.h"
#include "util/Serializer/FileSerializer.h"

struct ValueSizeGetter {
  ad_utility::MemorySize operator()(const TensorSearchCachedIndex&) const {
    return ad_utility::MemorySize::bytes(1);
  }
};

// We use an LRU cache, where the key is the name of the cached result.
using Key = std::string;
using Cache =
    ad_utility::LRUCache<Key, TensorSearchCachedIndex, ValueSizeGetter>;

ad_utility::Synchronized<Cache> cache_;
// using namespace  ad_utility::serialization;
using FaissIndexToRow = TensorSearchCachedIndex::FaissIndexToRow;

std::shared_ptr<faiss::IndexFlat> fromDistanceAndSize(
    TensorDistanceAlgorithm dist, int f) {
  switch (dist) {
    case TensorDistanceAlgorithm::DOT_PRODUCT:
      return std::make_shared<faiss::IndexFlatIP>(f);
    case TensorDistanceAlgorithm::EUCLIDEAN_DISTANCE:
      return std::make_shared<faiss::IndexFlatL2>(f);
    // case TensorDistanceAlgorithm::HAMMING_DISTANCE:
    //   return std::make_shared<AnnoyIndexVariants>(
    //       Annoy::AnnoyIndex<size_t, float, Annoy::Hamming,
    //       Annoy::Kiss32Random,
    //                         Annoy::AnnoyIndexMultiThreadedBuildPolicy>{f});
    //   break;
    default:
      throw std::runtime_error(
          "Unsupported distance metric for TensorSearchCachedIndex! Supported "
          "are DOT_PRODUCT and "
          "EUCLIDEAN_DISTANCE.");
  }
}

// ____________________________________________________________________________
TensorSearchCachedIndex::TensorSearchCachedIndex(
    ColumnIndex col, const IdTable& restable, const Index& index,
    TensorSearchConfiguration config_)
    : config_{config_} {
  tensorIndexToRow_ = buildIndex(col, restable, index);
}

FaissIndexToRow TensorSearchCachedIndex::buildIndex(ColumnIndex col,
                                                    const IdTable& restable,
                                                    const Index& index) {
  FaissIndexToRow tensorIndexToRow;
  // Populate the index from the given `IdTable`
  std::optional<ssize_t> firstItemSize;
  // We first collect all tensors and their corresponding row indices, as we
  // need to add them to the index in a batch per the FAISS API.
  std::vector<std::pair<size_t, std::vector<float>>> tensorsToAdd;

  // reserver the size of the vector to avoid unnecessary reallocations, as we
  // know the number of items to be indexed from the size of the `IdTable`
  tensorsToAdd.reserve(restable.size());

  for (size_t row = 0; row < restable.size(); row++) {
    auto id = restable.at(row, col);
    std::optional<ad_utility::TensorData> tensor =
        ExportQueryExecutionTrees::idToTensorData<true>(index, id, {});
    std::optional<std::vector<float>> tensorData;
    if (tensor.has_value()) {
      tensorData = tensor.value().tensorData();
    } else {
      continue;
    }

    if (tensorData.has_value()) {
      ssize_t tensorFirstDimShape = tensor.value().shape().size() > 0
                                        ? (ssize_t)tensor.value().shape()[0]
                                        : -1;
      // check if all tensors have the same size, which is required for building
      // the index
      if (firstItemSize.has_value()) {
        if (firstItemSize.value() != tensorFirstDimShape) {
          throw std::runtime_error({absl::StrCat(
              "Cannot index over vectors with different sizes! Encountered",
              firstItemSize.value(), "and", tensorData.value().size())});
        }

      } else {
        // initialize index with the size of the first tensor encountered
        dimSize_ = tensorFirstDimShape;
        size_t trees = config_.nTrees_.value_or(std::min(
            (size_t)std::sqrt(restable.size()) + 1, restable.size() - 1));
        quant_ = fromDistanceAndSize(config_.dist_, (int)tensorFirstDimShape);
        index_ = std::make_shared<faiss::IndexIVFFlat>(
            quant_.value().get(), (int)tensorFirstDimShape, trees);
      }
      tensorsToAdd.emplace_back(row, tensorData.value());
      tensorIndexToRow.emplace(row, id);
    }
  }
  // If we have an index, add all tensors to it. Otherwise, we can just return
  // an empty mapping, as there are no valid tensors to search over.
  if (index_.has_value()) {
    AD_LOG_INFO << "Building index with " << tensorIndexToRow.size()
                << " items. \n";
    // By default, the annoy indices are constructed lazily on the first query,
    // which then is slow.
    auto data = std::make_unique<float[]>(tensorsToAdd.size() * dimSize_);
    for (size_t i = 0; i < tensorsToAdd.size(); i++) {
      std::copy(tensorsToAdd[i].second.begin(), tensorsToAdd[i].second.end(),
                data.get() + i * dimSize_);
    }
    index_.value()->train(tensorsToAdd.size(), data.get());
    index_.value()->add(tensorsToAdd.size(), data.get());
  } else {
    AD_LOG_INFO << "No valid tensors found to build index.";
  }
  return tensorIndexToRow;
}

std::shared_ptr<const TensorSearchCachedIndex>
TensorSearchCachedIndex::fromKeyOrBuild(const std::string& key, ColumnIndex col,
                                        const IdTable& restable,
                                        const Index& index,
                                        TensorSearchConfiguration config) {
  auto lock = cache_.wlock();
  if (!lock->contains(key)) {
    auto newIndex = TensorSearchCachedIndex(col, restable, index, config);
    lock->insert(key, std::move(newIndex));
  }
  return (*lock)[key];
}

std::shared_ptr<const TensorSearchCachedIndex> TensorSearchCachedIndex::fromKey(
    const std::string& key) {
  auto lock = cache_.wlock();
  if (!lock->contains(key)) {
    for (auto& k : lock->getAllNonpinnedKeys()) {
      AD_LOG_INFO << "We have key " << k << " though... \n";
    }
    throw std::runtime_error({absl::StrCat("Cannot find key \"", key, "\"")});
  }
  return (*lock)[key];
}

std::vector<TensorSearchCachedIndex::FaissResult>
TensorSearchCachedIndex::findNN(const ad_utility::TensorData& query,
                                size_t n) const {
  std::vector<FaissResult> result;
  auto firstDimShape =
      query.shape().size() > 0 ? (ssize_t)query.shape()[0] : -1;
  if (firstDimShape != dimSize_) {
    throw std::runtime_error(
        {absl::StrCat("Query vector has different size than the indexed "
                      "vectors! Encountered ",
                      firstDimShape, " and ", dimSize_)});
  }
  std::vector<faiss::idx_t> nnIndices;
  std::vector<float> nnDistances;
  if (!index_.has_value()) {
    return result;
  }
  nnIndices.resize(n);
  nnDistances.resize(n);
  //  If the user did not specify a search_k value, we use a default that is
  //  based on the number of items in the index based on a simple heuristic.
  size_t search_probe = config_.searchK_.value_or(
      std::min((size_t)std::sqrt(tensorIndexToRow_.size()),
               tensorIndexToRow_.size() - 1));
  index_.value()->nprobe = (int)search_probe;
  index_.value()->search(1, query.tensorData().data(), n, nnDistances.data(),
                         nnIndices.data());

  AD_CORRECTNESS_CHECK(nnIndices.size() == nnDistances.size());
  for (size_t i = 0; i < nnIndices.size(); i++) {
    if (nnIndices[i] < 0) {
      // FAISS returns -1 for empty slots in the index, which can happen if the
      // number of indexed items is smaller than `n`. We ignore these results.
      continue;
    }
    result.emplace_back(nnIndices[i], nnDistances[i]);
  }
  return result;
}
