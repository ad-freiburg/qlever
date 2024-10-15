// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "./Distinct.h"

#include "engine/CallFixedSize.h"
#include "engine/QueryExecutionTree.h"

using std::endl;
using std::string;

// _____________________________________________________________________________
size_t Distinct::getResultWidth() const { return subtree_->getResultWidth(); }

// _____________________________________________________________________________
Distinct::Distinct(QueryExecutionContext* qec,
                   std::shared_ptr<QueryExecutionTree> subtree,
                   const std::vector<ColumnIndex>& keepIndices)
    : Operation(qec), subtree_(std::move(subtree)), _keepIndices(keepIndices) {}

// _____________________________________________________________________________
string Distinct::getCacheKeyImpl() const {
  return absl::StrCat("DISTINCT (", subtree_->getCacheKey(), ") (",
                      absl::StrJoin(_keepIndices, ","), ")");
}

// _____________________________________________________________________________
string Distinct::getDescriptor() const { return "Distinct"; }

// _____________________________________________________________________________
VariableToColumnMap Distinct::computeVariableToColumnMap() const {
  return subtree_->getVariableColumns();
}

template <size_t WIDTH>
cppcoro::generator<IdTable> Distinct::lazyDistinct(
    cppcoro::generator<IdTable> originalGenerator,
    std::vector<ColumnIndex> keepIndices,
    std::optional<IdTable> aggregateTable) {
  std::optional<typename IdTableStatic<WIDTH>::row_type> previousRow =
      std::nullopt;
  for (IdTable& idTable : originalGenerator) {
    IdTable result =
        distinct<WIDTH>(std::move(idTable), keepIndices, previousRow);
    if (!result.empty()) {
      auto view = result.asStaticView<WIDTH>();
      previousRow.emplace(std::as_const(view).back());
      if (aggregateTable.has_value()) {
        aggregateTable.value().insertAtEnd(result);
      } else {
        co_yield result;
      }
    }
  }
  if (aggregateTable.has_value()) {
    co_yield aggregateTable.value();
  }
}

// _____________________________________________________________________________
ProtoResult Distinct::computeResult(bool requestLaziness) {
  LOG(DEBUG) << "Getting sub-result for distinct result computation..." << endl;
  std::shared_ptr<const Result> subRes = subtree_->getResult(true);

  LOG(DEBUG) << "Distinct result computation..." << endl;
  size_t width = subtree_->getResultWidth();
  if (subRes->isFullyMaterialized()) {
    IdTable idTable =
        CALL_FIXED_SIZE(width, &Distinct::distinct, subRes->idTable().clone(),
                        _keepIndices, std::nullopt);
    LOG(DEBUG) << "Distinct result computation done." << endl;
    return {std::move(idTable), resultSortedOn(),
            subRes->getSharedLocalVocab()};
  }

  auto generator = CALL_FIXED_SIZE(
      width, &Distinct::lazyDistinct, std::move(subRes->idTables()),
      _keepIndices,
      requestLaziness ? std::nullopt
                      : std::optional{IdTable{width, allocator()}});
  if (!requestLaziness) {
    IdTable result = cppcoro::getSingleElement(std::move(generator));
    return {std::move(result), resultSortedOn(), subRes->getSharedLocalVocab()};
  }
  return {std::move(generator), resultSortedOn(),
          subRes->getSharedLocalVocab()};
}

// _____________________________________________________________________________
template <size_t WIDTH>
IdTable Distinct::distinct(
    IdTable dynInput, const std::vector<ColumnIndex>& keepIndices,
    std::optional<typename IdTableStatic<WIDTH>::row_type> previousRow) {
  AD_CONTRACT_CHECK(keepIndices.size() <= dynInput.numColumns());
  LOG(DEBUG) << "Distinct on " << dynInput.size() << " elements.\n";
  IdTableStatic<WIDTH> result = std::move(dynInput).toStatic<WIDTH>();

  auto matchesRow = [&keepIndices](const auto& a, const auto& b) {
    for (ColumnIndex i : keepIndices) {
      if (a[i] != b[i]) {
        return false;
      }
    }
    return true;
  };

  // Variant of `std::ranges::unique` that allows to skip the first rows of
  // elements found in the previous table.
  auto first = std::ranges::find_if(result, [&matchesRow,
                                             &previousRow](const auto& row) {
    return !previousRow.has_value() || !matchesRow(row, previousRow.value());
  });
  auto last = result.end();

  auto dest = result.begin();
  if (first == dest) {
    // Optimization to avoid redundant move operations.
    first = std::ranges::adjacent_find(first, last, matchesRow);
    dest = first;
    if (first != last) {
      ++first;
    }
  } else if (first != last) {
    *dest = std::move(*first);
  }

  if (first != last) {
    while (++first != last) {
      if (!matchesRow(*dest, *first)) {
        *++dest = std::move(*first);
      }
    }

    result.erase(++dest, last);
  }

  LOG(DEBUG) << "Distinct done.\n";
  return std::move(result).toDynamic();
}
