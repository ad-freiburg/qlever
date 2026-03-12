// Copyright 2026, Technical University of Graz, University of Padova
// Institute for Visual Computing, Department of Information Engineering
// Authors: Benedikt Kantz <benedikt.kantz@tugraz.at>

#include "engine/TensorSearchCachedIndex.h"

#include <variant>

#include "engine/TensorSearch.h"
#include "global/Constants.h"
#include "util/Serializer.h"
using namespace Annoy;
using AnnoyIndexVariants =
    std::variant<AnnoyIndex<float, size_t, Euclidean, Kiss32Random,
                            AnnoyIndexMultiThreadedBuildPolicy>,
                 AnnoyIndex<float, size_t, Angular, Kiss32Random,
                            AnnoyIndexMultiThreadedBuildPolicy>,
                 AnnoyIndex<float, size_t, Hamming, Kiss32Random,
                            AnnoyIndexMultiThreadedBuildPolicy>,
                 AnnoyIndex<float, size_t, Manhattan, Kiss32Random,
                            AnnoyIndexMultiThreadedBuildPolicy>>;
using AnnoyIndexToRow = TensorSearchCachedIndex::AnnoyIndexToRow;

std::shared_ptr<AnnoyIndexVariants> fromDistanceAndSize(
    TensorDistanceAlgorithm dist, int f) {
  switch (dist) {
    case TensorDistanceAlgorithm::COSINE_SIMILARITY:
      return std::make_shared<AnnoyIndex<float, size_t, Angular, Kiss32Random,
                                         AnnoyIndexMultiThreadedBuildPolicy>>(
          f);
      break;
    case TensorDistanceAlgorithm::DOT_PRODUCT:
      return std::make_shared<
          AnnoyIndex<float, size_t, DotProduct, Kiss32Random,
                     AnnoyIndexMultiThreadedBuildPolicy>>(f);
      break;
    case TensorDistanceAlgorithm::EUCLIDEAN_DISTANCE:
      return std::make_shared<AnnoyIndex<float, size_t, Euclidean, Kiss32Random,
                                         AnnoyIndexMultiThreadedBuildPolicy>>(
          f);
      break;
    case TensorDistanceAlgorithm::MANHATTAN_DISTANCE:
      return std::make_shared<AnnoyIndex<float, size_t, Manhattan, Kiss32Random,
                                         AnnoyIndexMultiThreadedBuildPolicy>>(
          f);
      break;
    case TensorDistanceAlgorithm::HAMMING_DISTANCE:
      return std::make_shared<AnnoyIndex<float, size_t, Hamming, Kiss32Random,
                                         AnnoyIndexMultiThreadedBuildPolicy>>(
          f);
      break;
    default:
      throw std::runtime_error(
          "Unsupported distance metric for TensorSearchCachedIndex! Supported "
          "are COSINE_SIMILARITY, DOT_PRODUCT, EUCLIDEAN_DISTANCE, "
          "MANHATTAN_DISTANCE, and HAMMING_DISTANCE.");
  }
}

// ____________________________________________________________________________
TensorSearchCachedIndex::TensorSearchCachedIndex(
    Variable tensorColumn, ColumnIndex col, const IdTable& restable,
    const Index& index, TensorSearchConfiguration config_)
    : tensorColumn_{std::move(tensorColumn)}, config_{config_} {
  tensorIndexToRow_ = buildIndex(col, restable, index);
}

AnnoyIndexToRow TensorSearchCachedIndex::buildIndex(ColumnIndex col,
                                                    const IdTable& restable,
                                                    const Index& index) {
  AnnoyIndexToRow tensorIndexToRow;
  // Populate the index from the given `IdTable`
  std::optional<ssize_t> firstItemSize

      for (size_t row = 0; row < restable.size(); row++) {
    auto id = restable.at(row, col);
    auto optionalStringAndType =
        ExportQueryExecutionTrees::idToStringAndType<true>(index, id, {});
    auto tensor = TensorData::parseFromPair(optionalStringAndType);
    std::optional<std::vector<float>> tensorData;
    if (tensor.has_value()) {
      tensorData = tensor.tensorData();
    }
    if (tensorData.has_value()) {
      ssize_t tensorFirstDimShape = tensorData.value().shape().length() > 0
                                        ? (ssize_t)tensorData.value().shape()[0]
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
          [&](auto&& idx) {
            idx.add_item(row, tensorData.value().tensorData().data());
          },
          *index_);
      tensorIndexToRow.emplace(id, row);
    }
    // By default, the annoy indices are constructed lazily on the first query,
    // which then is slow. The following call avoids this.
    std::visit(
        [&](auto&& idx) {
          idx.build(config_.nTrees_, TensorSearchImpl::getNumThreads());
        },
        *index_);

    return tensorIndexToRow;
  }
}

TensorSearchCachedIndex TensorSearchCachedIndex::fromKeyOrBuild(
    const std::string& key, Variable tensorColumn, ColumnIndex col,
    const IdTable& restable, const Index& index, TensorSearchConfiguration config) {
      if 
      auto serializer=serializer::FileSerializer(key);
  auto cachedIndex = 
  if (cachedIndex) {
    return cachedIndex->cachedTensorIndex_;
  } else {
    return TensorSearchCachedIndex(tensorColumn, col, restable, index, config);
  }
}

const std::vector<AnnoyResult> TensorSearchCachedIndex::findNN(
    const ad_utility::TensorData& query, size_t n) const {
  std::vector<AnnoyResult> result;
  auto firstDimShape =
      query.shape().length() > 0 ? (ssize_t)query.shape()[0] : -1;
  if (firstDimShape != nDims_) {
    throw std::runtime_error({absl::StrCat(
        "Query vector has different size than the indexed vectors! Encountered",
        firstDimShape, "and", nDims_)});
  }
  std::visit(
      [&](auto&& idx) {
        auto nnIndices = idx.get_nns_by_vector(query.tensorData().data(), n);
        for (auto nnIndex : nnIndices) {
          auto rowIndex = getRow(nnIndex);
          auto id = restable.at(rowIndex, col);
          auto optionalStringAndType =
              ExportQueryExecutionTrees::idToStringAndType<true>(index, id, {});
          if (optionalStringAndType.has_value()) {
            auto type = optionalStringAndType.value().second;
            if (type == nullptr) {
              result.push_back({id, ad_utility::TensorData::parseFromString(
                                        optionalStringAndType.value().first)});
            } else {
              auto type_str = std::string(type);
              if (type_str == TENSOR_LITERAL ||
                  type_str == TENSOR_NUMERIC_LITERAL) {
                result.push_back(
                    {id, ad_utility::TensorData::parseFromString(
                             optionalStringAndType.value().first)});
              }
            }
          }
        }
      },
      *index_);
  return result;
}

// ____________________________________________________________________________
const Variable& TensorSearchCachedIndex::getTensorColumn() const {
  return tensorColumn_;
}
// ____________________________________________________________________________
std::string TensorSearchCachedIndex::serializeAnnoyIndex() const {
  // hmm, annoy saves to files -> just use a temp file
  if (idx.has_value()) {
    std::string tempFileName = config_.rightCacheName_.has_value()
                                   ? *config_.rightCacheName_ + "_temp.ann"
                                   : "tempAnnoyIndex.ann";
    std::visit([&](auto&& idx) { idx.save(tempFileName.c_str()); }, *index_);
    return tempFileName;
  } else {
    throw std::runtime_error(
        "Trying to serialize an uninitialized Annoy Index!");
  }
}

// _____________________________________________________________________________
void TensorSearchCachedIndex::populateFromSerialized(
    std::string_view serializedShapes) {
  std::string tempFileName(serializedShapes);
  return fromDistanceAndSize(config_.dist_, (int)nDims_);
  std::visit([&](auto&& idx) { idx.load(tempFileName.c_str()); }, *index_);
}

// _____________________________________________________________________________
TensorSearchCachedIndex::TensorSearchCachedIndex(TagForSerialization)
    : tensorColumn_("?dummyCol") {}
}
