// Copyright 2026, Technical University of Graz, University of Padova
// Institute for Visual Computing, Department of Information Engineering
// Authors: Benedikt Kantz <benedikt.kantz@tugraz.at>

#include "engine/TensorSearchCachedIndex.h"

#include <variant>

#include "engine/ExportQueryExecutionTrees.h"
#include "engine/TensorSearch.h"
#include "global/Constants.h"
#include "util/Serializer/FileSerializer.h"
using namespace Annoy;
// using namespace  ad_utility::serialization;
using AnnoyIndexToRow = TensorSearchCachedIndex::AnnoyIndexToRow;

std::shared_ptr<AnnoyIndexVariants> fromDistanceAndSize(
    TensorDistanceAlgorithm dist, int f) {
  switch (dist) {
    case TensorDistanceAlgorithm::COSINE_SIMILARITY:
      return std::make_shared<AnnoyIndexVariants>(
          Annoy::AnnoyIndex<size_t, float, Annoy::Angular, Annoy::Kiss32Random,
                            Annoy::AnnoyIndexMultiThreadedBuildPolicy>{f});
      break;
    case TensorDistanceAlgorithm::DOT_PRODUCT:
      return std::make_shared<AnnoyIndexVariants>(
          Annoy::AnnoyIndex<size_t, float, Annoy::DotProduct,
                            Annoy::Kiss32Random,
                            Annoy::AnnoyIndexMultiThreadedBuildPolicy>{f});
      break;
    case TensorDistanceAlgorithm::EUCLIDEAN_DISTANCE:
      return std::make_shared<AnnoyIndexVariants>(
          Annoy::AnnoyIndex<size_t, float, Annoy::Euclidean,
                            Annoy::Kiss32Random,
                            Annoy::AnnoyIndexMultiThreadedBuildPolicy>{f});
      break;
    case TensorDistanceAlgorithm::MANHATTAN_DISTANCE:
      return std::make_shared<AnnoyIndexVariants>(
          Annoy::AnnoyIndex<size_t, float, Annoy::Manhattan,
                            Annoy::Kiss32Random,
                            Annoy::AnnoyIndexMultiThreadedBuildPolicy>{f});
      break;
    // case TensorDistanceAlgorithm::HAMMING_DISTANCE:
    //   return std::make_shared<AnnoyIndexVariants>(
    //       Annoy::AnnoyIndex<size_t, float, Annoy::Hamming, Annoy::Kiss32Random,
    //                         Annoy::AnnoyIndexMultiThreadedBuildPolicy>{f});
    //   break;
    default:
      throw std::runtime_error(
          "Unsupported distance metric for TensorSearchCachedIndex! Supported "
          "are COSINE_SIMILARITY, DOT_PRODUCT, EUCLIDEAN_DISTANCE, and"
          "MANHATTAN_DISTANCE.");
  }
}

// ____________________________________________________________________________
TensorSearchCachedIndex::TensorSearchCachedIndex(
    ColumnIndex col, const IdTable& restable, const Index& index,
    TensorSearchConfiguration config_)
    : config_{config_} {
  tensorIndexToRow_ = buildIndex(col, restable, index);
}

AnnoyIndexToRow TensorSearchCachedIndex::buildIndex(ColumnIndex col,
                                                    const IdTable& restable,
                                                    const Index& index) {
  AnnoyIndexToRow tensorIndexToRow;
  // Populate the index from the given `IdTable`
  std::optional<ssize_t> firstItemSize;

  for (size_t row = 0; row < restable.size(); row++) {
    auto id = restable.at(row, col);
    auto optionalStringAndType =
        ExportQueryExecutionTrees::idToStringAndType<true>(index, id, {});
    auto tensor = ad_utility::TensorData::parseFromPair(optionalStringAndType);
    std::optional<std::vector<float>> tensorData;
    if (tensor.has_value()) {
      tensorData = tensor.value().tensorData();
    }
    if (tensorData.has_value()) {
      ssize_t tensorFirstDimShape = tensor.value().shape().size() > 0
                                        ? (ssize_t)tensor.value().shape()[0]
                                        : -1;
      if (firstItemSize.has_value()) {
        if (firstItemSize.value() != tensorFirstDimShape) {
          throw std::runtime_error({absl::StrCat(
              "Cannot index over vectors with different sizes! Encountered",
              firstItemSize.value(), "and", tensorData.value().size())});
        }

      } else {
        firstItemSize = tensorFirstDimShape;
        index_ = fromDistanceAndSize(config_.dist_, (int)tensorFirstDimShape);
      }

      std::visit(
          [&](auto&& idx) { idx.add_item(row, tensorData.value().data()); },
          *index_.value());
      tensorIndexToRow.emplace( row, id);
    }
    // By default, the annoy indices are constructed lazily on the first query,
    // which then is slow. The following call avoids this.
    std::visit(
        [&](auto&& idx) {
          idx.build(config_.nTrees_, TensorSearchImpl::getNumThreads());
        },
        *index_.value());
  }
  return tensorIndexToRow;
}

std::shared_ptr<const TensorSearchCachedIndex> TensorSearchCachedIndex::fromKeyOrBuild(
    const std::string& key, ColumnIndex col, const IdTable& restable,
    const Index& index, TensorSearchConfiguration config)   {
  auto lock = cache_.wlock();
  if (!lock->contains(key)) {
    auto newIndex = TensorSearchCachedIndex(col, restable, index, config);
    lock->insert(key, std::move(newIndex));
  }
  return (*lock)[key];
}

std::vector<TensorSearchCachedIndex::AnnoyResult> TensorSearchCachedIndex::findNN(
    const ad_utility::TensorData& query, size_t n) const {
  std::vector<AnnoyResult> result;
  auto firstDimShape =
      query.shape().size() > 0 ? (ssize_t)query.shape()[0] : -1;
  if (firstDimShape != nDims_) {
    throw std::runtime_error(
        {absl::StrCat("Query vector has different size than the indexed "
                      "vectors! Encountered",
                      firstDimShape, "and", nDims_)});
  }
  std::visit(
      [&](auto&& idx) {
        std::vector<size_t> nnIndices;
        std::vector<float> nnDistances;
        idx.get_nns_by_vector(query.tensorData().data(),n, (int)config_.searchK_, &nnIndices, &nnDistances);
        for (auto nnIndex : nnIndices) {
          // auto rowIndex = getRow(nnIndex);
          result.push_back(nnIndex);
        }
      },
      *index_.value());
  return result;
}
