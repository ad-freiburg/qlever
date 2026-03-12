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
#include "engine/TensorSearchConfig.h"
#include "engine/VariableToColumnMap.h"
#include "engine/idTable/IdTable.h"
#include "global/Constants.h"
#include "global/RuntimeParameters.h"
#include "global/ValueId.h"
#include "parser/ParsedQuery.h"
#include "util/AllocatorWithLimit.h"
#include "util/Exception.h"
#include "util/TensorData.h"
#include "util/MemorySize/MemorySize.h"

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

// ____________________________________________________________________________

// ____________________________________________________________________________
void TensorSearchImpl::addResultTableEntry(IdTable* result,
                                           const IdTable* idTableLeft,
                                           const IdTable* idTableRight,
                                           size_t rowLeft,
                                           size_t rowRight) const {
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
}

Result TensorSearchImpl::computeTensorSearchResultAnnoy() {
#ifdef WITH_DENSE_TENSOR_INDEX
  IdTable result{params_.numColumns_, qec_->getAllocator()};

  auto annoyIndex = TensorSearchCachedIndex::fromCacheOrBuild(params_, qec_->getAllocator());
  for (size_t i = 0; i < params_.idTableLeft_->size(); i++) {
    auto id = params_.idTableLeft_->at(i, params_.leftJoinCol_);

    auto optionalStringAndType =
        ExportQueryExecutionTrees::idToStringAndType<true>(qec_->getIndex(), id,
                                                           {});
    auto tensorData = TensorData::parseFromPair(optionalStringAndType);
    if (!tensorData.has_value()) {
      continue;
    }
    auto results =
        annoyIndex->findNN(tensorData.value(), params_.config_.maxResults_);
    for (const auto& [nnId, dist] : results) {
      addResultTableEntry(&result, params_.idTableLeft_, params_.idTableRight_,
                          i, nnId);
    }
  }

  return Result{std::move(result), std::vector<ColumnIndex>{},
                Result::getMergedLocalVocab(*params_.resultLeft_, *params_.resultRight_)};
#else
  throw ad_utility::Exception(
      "TensorSearchImpl::computeTensorSearchResultAnnoy() called, but qlever "
      "was not compiled with the option to use the dense tensor index.");
#endif
}
