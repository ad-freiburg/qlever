// Copyright 2026, Technical University of Graz, University of Padova
// Institute for Visual Computing, Department of Information Engineering
// Authors: Benedikt Kantz <benedikt.kantz@tugraz.at>

#include <absl/container/flat_hash_set.h>
#include <absl/strings/charconv.h>

#include <cstdint>
#include <limits>
#include <optional>
#include <queue>
#include <tuple>
#include <unordered_set>
#include <variant>

#include "backports/type_traits.h"
#include "engine/ExportQueryExecutionTrees.h"
#include "engine/NamedResultCache.h"
#include "engine/TensorSearch.h"
#include "engine/TensorSearchCachedIndex.h"
#include "engine/TensorSearchConfig.h"
#include "engine/VariableToColumnMap.h"
#include "engine/idTable/IdTable.h"
#include "global/Constants.h"
#include "global/RuntimeParameters.h"
#include "global/ValueId.h"
#include "parser/ParsedQuery.h"
#include "rdfTypes/TensorData.h"
#include "util/AllocatorWithLimit.h"
#include "util/Exception.h"
#include "util/MemorySize/MemorySize.h"

#ifdef QLEVER_USE_TENSOR_BLAS
extern "C" {
  #include <cblas.h>
  #include <openblas_config.h>
}
#endif
#ifdef QLEVER_USE_TENSOR_PARALLEL
#include <omp.h>
#endif
// ____________________________________________________________________________
size_t TensorSearchImpl::getNumThreads() {
  size_t maxHwConcurrency = std::thread::hardware_concurrency();
  size_t userPreference =
      getRuntimeParameter<&RuntimeParameters::tensorSearchMaxNumThreads_>();
  if (userPreference == 0 || maxHwConcurrency < userPreference) {
    return maxHwConcurrency;
  }
  return userPreference;
}
void TensorSearchImpl::initializeGlobalRuntimeParameters() {
  // This function is called at the beginning of the server execution to set the
  // global runtime parameters related to tensor search. This is necessary to
  // ensure that the number of threads used for tensor search is set correctly
  // before any tensor search operations are executed.

#ifdef QLEVER_USE_TENSOR_BLAS
  AD_LOG_INFO << "Using OpenBLAS for tensor operations." << std::endl;
  size_t maxNumThreads = getNumThreads();
  AD_LOG_INFO << "Setting the number of threads for BLAS to " << maxNumThreads
              << " (as specified by the "
              << "tensorSearchMaxNumThreads runtime parameter)" << std::endl;
  openblas_set_num_threads(maxNumThreads);
#else
#ifdef QLEVER_TENSOR_PARALLEL
  AD_LOG_INFO << "Using OpenP implementation for tensor operations."
              << std::endl;
  size_t maxNumThreads = getNumThreads();
  AD_LOG_INFO << "Setting the number of threads for OpenMP to " << maxNumThreads
              << " (as specified by the "
              << "tensorSearchMaxNumThreads runtime parameter)" << std::endl;
  omp_set_num_threads(maxNumThreads);
#else
  AD_LOG_INFO << "Using internal implementation for tensor operations."
              << std::endl;
#endif

#endif
}

// ____________________________________________________________________________
void TensorSearchImpl::addResultTableEntry(IdTable* result,
                                           const IdTable* idTableLeft,
                                           const IdTable* idTableRight,
                                           size_t rowLeft, size_t rowRight,
                                           Id distance) const {
  // this lambda function copies values from copyFrom into the table res only if
  // the column of the value is specified in sourceColumns. If sourceColumns is
  // nullopt, all columns are added. It copies them into the row rowIndRes and
  // column column colIndRes. It returns the column number until which elements
  // were copied
  auto addColumns = [](IdTable* res, const IdTable* copyFrom, size_t rowIndRes,
                       size_t colIndRes, size_t rowIndCopy,
                       std::optional<std::vector<ColumnIndex>> sourceColumns =
                           std::nullopt) {
    size_t nCols = sourceColumns.has_value() ? sourceColumns.value().size()
                                             : copyFrom->numColumns();
    for (size_t i = 0; i < nCols; i++) {
      auto col = sourceColumns.has_value() ? sourceColumns.value()[i] : i;
      res->at(rowIndRes, colIndRes + i) = (*copyFrom).at(rowIndCopy, col);
    }
    return colIndRes + nCols;
  };

  auto resrow = result->numRows();
  result->emplace_back();
  // add columns to result table
  size_t rescol = 0;
  rescol = addColumns(result, idTableLeft, resrow, rescol, rowLeft);
  rescol = addColumns(result, idTableRight, resrow, rescol, rowRight,
                      params_.rightSelectedCols_);

  if (params_.config_.distanceVariable_.has_value()) {
    result->at(resrow, rescol) = distance;
    // rescol isn't used after that in this function, but future updates,
    // which add additional columns, would need to remember to increase
    // rescol at this place otherwise. If they forget to do this, the
    // distance column will be overwritten, the variableToColumnMap will
    // not work and so on
    // rescol += 1;
  }
}

Result TensorSearchImpl::computeTensorSearchResultFaiss() {
  // #ifdef WITH_DENSE_TENSOR_INDEX
  IdTable result{params_.numColumns_, qec_->getAllocator()};

  auto faissIndex = TensorSearchCachedIndex::fromKeyOrBuild(
      params_.cacheKey_, params_.rightJoinCol_, *params_.idTableRight_,
      qec_->getIndex(), params_.config_);
  for (size_t i = 0; i < params_.idTableLeft_->size(); i++) {
    auto id = params_.idTableLeft_->at(i, params_.leftJoinCol_);

    auto optionalStringAndType =
        ExportQueryExecutionTrees::idToStringAndType<true>(qec_->getIndex(), id,
                                                           {});
    auto tensorData =
        ad_utility::TensorData::parseFromPair(optionalStringAndType);
    if (!tensorData.has_value()) {
      continue;
    }
    auto results =
        faissIndex->findNN(tensorData.value(), params_.config_.maxResults_);
    for (const auto& nn : results) {
      addResultTableEntry(&result, params_.idTableLeft_, params_.idTableRight_,
                          i, nn.first, Id::makeFromDouble(nn.second));
    }
  }

  return Result{
      std::move(result), std::vector<ColumnIndex>{},
      Result::getMergedLocalVocab(*params_.resultLeft_, *params_.resultRight_)};
  // #else
  //   throw ad_utility::Exception(
  //       "TensorSearchImpl::computeTensorSearchResultFaiss() called, but
  //       qlever " "was not compiled with the option to use the dense tensor
  //       index.");
  // #endif
}
Result TensorSearchImpl::computeTensorSearchResultNaive() {
  IdTable result{params_.numColumns_, qec_->getAllocator()};

  for (size_t i = 0; i < params_.idTableLeft_->size(); i++) {
    auto id = params_.idTableLeft_->at(i, params_.leftJoinCol_);
    auto optionalStringAndType =
        ExportQueryExecutionTrees::idToStringAndType<true>(qec_->getIndex(), id,
                                                           {});
    auto tensorData =
        ad_utility::TensorData::parseFromPair(optionalStringAndType);
    if (!tensorData.has_value() && optionalStringAndType.has_value()) {
      AD_LOG_WARN << "Could not parse tensor of "
                  << optionalStringAndType.value().first << " at row " << i
                  << ". This item will be ignored for indexing.";
      continue;
    }
    std::map<size_t, float> distanceToRowLeft;
    for (size_t j = 0; j < params_.idTableRight_->size(); j++) {
      auto idRight = params_.idTableRight_->at(j, params_.rightJoinCol_);
      auto optionalStringAndTypeRight =
          ExportQueryExecutionTrees::idToStringAndType<true>(qec_->getIndex(),
                                                             idRight, {});
      auto tensorDataRight =
          ad_utility::TensorData::parseFromPair(optionalStringAndTypeRight);
      if (!tensorDataRight.has_value()) {
        continue;
      }

      auto distance =
          computeDistance(tensorData.value(), tensorDataRight.value());
      distanceToRowLeft[j] = distance;
    }
    // sort indices by distance and take the closest ones
    std::vector<std::pair<size_t, float>> sortedDistanceToRowLeft(
        distanceToRowLeft.begin(), distanceToRowLeft.end());
    std::sort(sortedDistanceToRowLeft.begin(), sortedDistanceToRowLeft.end(),
              [](const auto& a, const auto& b) { return a.second > b.second; });
    for (size_t k = 0; k < std::min((size_t)params_.config_.maxResults_,
                                    sortedDistanceToRowLeft.size());
         k++) {
      addResultTableEntry(
          &result, params_.idTableLeft_, params_.idTableRight_, i,
          sortedDistanceToRowLeft[k].first,
          Id::makeFromDouble(sortedDistanceToRowLeft[k].second));
    }
  }

  return Result{
      std::move(result), std::vector<ColumnIndex>{},
      Result::getMergedLocalVocab(*params_.resultLeft_, *params_.resultRight_)};
}
float TensorSearchImpl::computeDistance(
    const ad_utility::TensorData& left,
    const ad_utility::TensorData& right) const {
  switch (params_.config_.dist_) {
    case TensorDistanceAlgorithm::COSINE_SIMILARITY:
      return ad_utility::TensorData::cosineSimilarity(left, right);
    case TensorDistanceAlgorithm::DOT_PRODUCT:
      return ad_utility::TensorData::dot(left, right);
    case TensorDistanceAlgorithm::EUCLIDEAN_DISTANCE:
      return ad_utility::TensorData::norm(
          ad_utility::TensorData::subtract(left, right));
    default:
      throw ad_utility::Exception(
          "Unknown distance function in TensorSearchImpl::computeDistance");
  }
}
Result TensorSearchImpl::computeTensorSearchResult() {
  switch (params_.config_.algo_) {
    case TensorSearchAlgorithm::FAISS:
      return computeTensorSearchResultFaiss();
    default:
      return computeTensorSearchResultNaive();
  }
}
