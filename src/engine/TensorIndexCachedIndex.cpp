// Copyright 2026, Graz Technical University, University of Padova
// Institute for Visual Computing, Department of Information Engineering
// Authors: Benedikt Kantz <benedikt.kantz@tugraz.at>

#include <faiss/IndexFlat.h>
#include <faiss/IndexHNSW.h>
#include <faiss/IndexIVFFlat.h>

#include <variant>

#include "engine/ExportQueryExecutionTrees.h"
#include "engine/TensorIndex.h"
#include "engine/TensorIndexCachedIndex.h"
#include "global/Constants.h"
#include "util/Serializer/FileSerializer.h"

struct ValueSizeGetter {
  ad_utility::MemorySize operator()(const TensorIndexCachedIndex&) const {
    return ad_utility::MemorySize::bytes(1);
  }
};

// We use an LRU cache, where the key is the name of the cached result.
using Key = std::string;
using Cache =
    ad_utility::LRUCache<Key, TensorIndexCachedIndex, ValueSizeGetter>;

ad_utility::Synchronized<Cache> cache_;
// using namespace  ad_utility::serialization;
using FaissIndexToRow = TensorIndexCachedIndex::FaissIndexToRow;

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
          "Unsupported distance metric for TensorIndexCachedIndex! Supported "
          "are DOT_PRODUCT and "
          "EUCLIDEAN_DISTANCE.");
  }
}

// ____________________________________________________________________________
TensorIndexCachedIndex::TensorIndexCachedIndex(
    ColumnIndex col, const IdTable& restable, const Index& index,
    TensorIndexConfiguration config_)
    : config_{config_} {
  tensorIndexToRow_ = buildIndex(col, restable, index);
}

FaissIndexToRow TensorIndexCachedIndex::buildIndex(ColumnIndex col,
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
        size_t default_sizes = std::min((size_t)std::sqrt(restable.size()) + 1,
                                        restable.size() - 1);
        size_t trees = config_.kIVF_.value_or(default_sizes);
        size_t nNeighbours = config_.nNeighbours_.value_or(default_sizes);
        auto metric = config_.dist_ == TensorDistanceAlgorithm::DOT_PRODUCT
                          ? faiss::METRIC_INNER_PRODUCT
                          : faiss::METRIC_L2;
        switch (config_.algo_) {
          case TensorIndexAlgorithm::FAISS_HSNW:
            index_ = std::make_shared<faiss::IndexHNSWFlat>(
                (int)tensorFirstDimShape, nNeighbours, metric);
            break;
          case TensorIndexAlgorithm::FAISS_IVF:
            quant_ =
                fromDistanceAndSize(config_.dist_, (int)tensorFirstDimShape);
            index_ = std::make_shared<faiss::IndexIVFFlat>(
                quant_.value().get(), (int)tensorFirstDimShape, trees, metric);
            break;
          default:
            AD_THROW(
                "Unknown algorithm during Faiss index build, please report "
                "this to the developers!");
        }
      }
      tensorIndexToRow.emplace(tensorsToAdd.size(), row);
      tensorsToAdd.emplace_back(row, tensorData.value());
    }
  }
  // If we have an index, add all tensors to it. Otherwise, we can just return
  // an empty mapping, as there are no valid tensors to search over.
  if (index_.has_value()) {
    AD_LOG_INFO << "Building index with " << tensorIndexToRow.size()
                << " items. \n";
    // By default, the annoy indices are constructed lazily on the first
    // query, which then is slow.
    auto data = std::make_unique<float[]>(tensorsToAdd.size() * dimSize_);
    auto ids = std::make_unique<faiss::idx_t[]>(tensorsToAdd.size());
    for (size_t i = 0; i < tensorsToAdd.size(); i++) {
      std::copy(tensorsToAdd[i].second.begin(), tensorsToAdd[i].second.end(),
                data.get() + i * dimSize_);
      ids[i] = (faiss::idx_t)tensorsToAdd[i].first;
    }
    std::visit(
        [&tensorsToAdd, &data](const auto& idx) -> void {
          idx->train(tensorsToAdd.size(), data.get());
          idx->add(tensorsToAdd.size(), data.get());
        },
        index_.value());
  } else {
    AD_LOG_INFO << "No valid tensors found to build index.";
  }
  return tensorIndexToRow;
}

std::shared_ptr<const TensorIndexCachedIndex>
TensorIndexCachedIndex::fromKeyOrBuild(const std::string& key, ColumnIndex col,
                                        const IdTable& restable,
                                        const Index& index,
                                        TensorIndexConfiguration config) {
  auto lock = cache_.wlock();
  if (!lock->contains(key)) {
    auto newIndex = TensorIndexCachedIndex(col, restable, index, config);
    lock->insert(key, std::move(newIndex));
  }
  return (*lock)[key];
}

std::shared_ptr<const TensorIndexCachedIndex> TensorIndexCachedIndex::fromKey(
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

std::vector<TensorIndexCachedIndex::FaissResult>
TensorIndexCachedIndex::findNN(const ad_utility::TensorData& query,
                                size_t k) const {
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
  nnIndices.resize(k);
  nnDistances.resize(k);
  std::visit(
      [&](const auto& idx) -> void {
        using T = std::decay_t<decltype(idx)>;
        if (config_.searchK_.has_value()) {
          if constexpr (std::is_same_v<T, std::variant<faiss::IndexIVFFlat>>) {
            idx->nprobe = config_.searchK_.value();
          }
          if constexpr (std::is_same_v<T, std::variant<faiss::IndexHNSWFlat>>) {
            idx->efSearch = config_.searchK_.value();
          }
        }
        idx->search(1, query.tensorData().data(), k, nnDistances.data(),
                    nnIndices.data());
      },
      index_.value());

  AD_CORRECTNESS_CHECK(nnIndices.size() == nnDistances.size());
  for (size_t i = 0; i < nnIndices.size(); i++) {
    if (nnIndices[i] < 0) {
      // FAISS returns -1 for empty slots in the index, which can happen if
      // the number of indexed items is smaller than `n`. We ignore these
      // results.
      continue;
    }
    FaissResult res = {(size_t)nnIndices[i], nnDistances[i],
                       tensorIndexToRow_.at(nnIndices[i])};
    result.emplace_back(res);
  }
  return result;
}
