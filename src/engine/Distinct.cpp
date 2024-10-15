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

// _____________________________________________________________________________
ProtoResult Distinct::computeResult([[maybe_unused]] bool requestLaziness) {
  LOG(DEBUG) << "Getting sub-result for distinct result computation..." << endl;
  std::shared_ptr<const Result> subRes = subtree_->getResult();

  LOG(DEBUG) << "Distinct result computation..." << endl;
  size_t width = subtree_->getResultWidth();
  IdTable idTable =
      CALL_FIXED_SIZE(width, &Distinct::distinct, subRes->idTable().clone(),
                      _keepIndices, std::nullopt);
  LOG(DEBUG) << "Distinct result computation done." << endl;
  return {std::move(idTable), resultSortedOn(), subRes->getSharedLocalVocab()};
}

// _____________________________________________________________________________
template <size_t WIDTH>
IdTable Distinct::distinct(
    IdTable dynInput, const std::vector<ColumnIndex>& keepIndices,
    std::optional<
        std::reference_wrapper<typename IdTableStatic<WIDTH>::row_type>>
        previousRow) {
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

  auto subrange = std::ranges::unique(
      result, [&matchesRow, &previousRow](const auto& a, const auto& b) {
        return matchesRow(a, b) && (!previousRow.has_value() ||
                                    !matchesRow(previousRow.value().get(), a));
      });
  result.erase(subrange.begin(), subrange.end());
  LOG(DEBUG) << "Distinct done.\n";
  return std::move(result).toDynamic();
}
