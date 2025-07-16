// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include "engine/Distinct.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

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
    : Operation{qec}, subtree_{std::move(subtree)}, keepIndices_{keepIndices} {
  AD_CORRECTNESS_CHECK(subtree_);
  subtree_ = QueryExecutionTree::createSortedTreeAnyPermutation(
      std::move(subtree_), keepIndices_);
}

// _____________________________________________________________________________
string Distinct::getCacheKeyImpl() const {
  return absl::StrCat("DISTINCT (", subtree_->getCacheKey(), ") (",
                      absl::StrJoin(keepIndices_, ","), ")");
}

// _____________________________________________________________________________
string Distinct::getDescriptor() const { return "Distinct"; }

// _____________________________________________________________________________
VariableToColumnMap Distinct::computeVariableToColumnMap() const {
  return subtree_->getVariableColumns();
}

// _____________________________________________________________________________
template <size_t WIDTH>
Result::LazyResult Distinct::lazyDistinct(Result::LazyResult input,
                                          bool yieldOnce) const {
  using namespace ad_utility;
  auto getDistinctResult =
      [this,
       previousRow = std::optional<typename IdTableStatic<WIDTH>::row_type>{
           std::nullopt}](IdTable&& idTable) mutable {
        IdTable result = this->distinct<WIDTH>(std::move(idTable), previousRow);
        if (!result.empty()) {
          previousRow.emplace(result.asStaticView<WIDTH>().back());
        }
        return result;
      };

  if (yieldOnce) {
    return Result::LazyResult{lazySingleValueRange(
        [getDistinctResult,
         aggregateTable = IdTable{subtree_->getResultWidth(), allocator()},
         aggregateVocab = LocalVocab{}, input = std::move(input)]() mutable {
          for (auto& [idTable, localVocab] : input) {
            IdTable result = getDistinctResult(std::move(idTable));
            if (!result.empty()) {
              aggregateVocab.mergeWith(std::array{std::move(localVocab)});
              aggregateTable.insertAtEnd(result);
            }
          }
          return Result::IdTableVocabPair{std::move(aggregateTable),
                                          std::move(aggregateVocab)};
        })};
  }

  return Result::LazyResult{CachingContinuableTransformInputRange(
      std::move(input), [getDistinctResult](auto& idTableAndVocab) mutable {
        IdTable result = getDistinctResult(std::move(idTableAndVocab.idTable_));
        return result.empty()
                   ? Result::IdTableLoopControl::makeContinue()
                   : Result::IdTableLoopControl::yieldValue(
                         Result::IdTableVocabPair{
                             std::move(result),
                             std::move(idTableAndVocab.localVocab_)});
      })};
}

// _____________________________________________________________________________
Result Distinct::computeResult(bool requestLaziness) {
  LOG(DEBUG) << "Getting sub-result for distinct result computation..." << endl;
  std::shared_ptr<const Result> subRes = subtree_->getResult(true);

  LOG(DEBUG) << "Distinct result computation..." << endl;
  size_t width = subtree_->getResultWidth();
  if (subRes->isFullyMaterialized()) {
    IdTable idTable = CALL_FIXED_SIZE(width, &Distinct::outOfPlaceDistinct,
                                      this, subRes->idTable());
    LOG(DEBUG) << "Distinct result computation done." << endl;
    return {std::move(idTable), resultSortedOn(),
            subRes->getSharedLocalVocab()};
  }

  auto generator = CALL_FIXED_SIZE(width, &Distinct::lazyDistinct, this,
                                   subRes->idTables(), !requestLaziness);
  return requestLaziness
             ? Result{std::move(generator), resultSortedOn()}
             : Result{ad_utility::getSingleElement(std::move(generator)),
                      resultSortedOn()};
}

// _____________________________________________________________________________
template <typename T1, typename T2>
bool Distinct::matchesRow(const T1& a, const T2& b) const {
  return ql::ranges::all_of(keepIndices_,
                            [&a, &b](ColumnIndex i) { return a[i] == b[i]; });
}

// _____________________________________________________________________________
template <size_t WIDTH>
IdTable Distinct::distinct(
    IdTable dynInput,
    std::optional<typename IdTableStatic<WIDTH>::row_type> previousRow) const {
  AD_CONTRACT_CHECK(keepIndices_.size() <= dynInput.numColumns());
  LOG(DEBUG) << "Distinct on " << dynInput.size() << " elements.\n";
  IdTableStatic<WIDTH> result = std::move(dynInput).toStatic<WIDTH>();

  // Variant of `ql::ranges::unique` that allows to skip the begin rows of
  // elements found in the previous table.
  auto begin =
      ql::ranges::find_if(result, [this, &previousRow](const auto& row) {
        // Without explicit this clang seems to
        // think the this capture is redundant.
        return !previousRow.has_value() ||
               !this->matchesRow(row, previousRow.value());
      });
  auto end = result.end();

  auto dest = result.begin();
  if (begin == dest) {
    // Optimization to avoid redundant move operations.
    begin = ql::ranges::adjacent_find(begin, end,
                                      [this](const auto& a, const auto& b) {
                                        // Without explicit this clang seems to
                                        // think the this capture is redundant.
                                        return this->matchesRow(a, b);
                                      });
    dest = begin;
    if (begin != end) {
      ++begin;
    }
  } else if (begin != end) {
    *dest = std::move(*begin);
  }

  if (begin != end) {
    while (++begin != end) {
      if (!matchesRow(*dest, *begin)) {
        *++dest = std::move(*begin);
        checkCancellation();
      }
    }
    ++dest;
  }
  checkCancellation();
  result.erase(dest, end);
  checkCancellation();

  LOG(DEBUG) << "Distinct done.\n";
  return std::move(result).toDynamic();
}

// _____________________________________________________________________________
template <size_t WIDTH>
IdTable Distinct::outOfPlaceDistinct(const IdTable& dynInput) const {
  AD_CONTRACT_CHECK(keepIndices_.size() <= dynInput.numColumns());
  LOG(DEBUG) << "Distinct on " << dynInput.size() << " elements.\n";
  auto inputView = dynInput.asStaticView<WIDTH>();
  IdTableStatic<WIDTH> output{dynInput.numColumns(), allocator()};

  auto begin = inputView.begin();
  auto end = inputView.end();
  while (begin < end) {
    int64_t allowedOffset = std::min(end - begin, CHUNK_SIZE);
    begin = ql::ranges::unique_copy(begin, begin + allowedOffset,
                                    std::back_inserter(output),
                                    [this](const auto& a, const auto& b) {
                                      // Without explicit this clang seems to
                                      // think the this capture is redundant.
                                      return this->matchesRow(a, b);
                                    })
                .in;
    checkCancellation();
    // Skip to next unique value
    do {
      allowedOffset = std::min(end - begin, CHUNK_SIZE);
      // This can only be called when dynInput is not empty, so `begin[-1]` is
      // always valid.
      auto lastRow = begin[-1];
      begin = ql::ranges::find_if(begin, begin + allowedOffset,
                                  [this, &lastRow](const auto& row) {
                                    // Without explicit this clang seems to
                                    // think the this capture is redundant.
                                    return !this->matchesRow(row, lastRow);
                                  });
      checkCancellation();
    } while (begin != end && matchesRow(*begin, begin[-1]));
  }

  LOG(DEBUG) << "Distinct done.\n";
  return std::move(output).toDynamic();
}

// _____________________________________________________________________________
std::unique_ptr<Operation> Distinct::cloneImpl() const {
  return std::make_unique<Distinct>(_executionContext, subtree_->clone(),
                                    keepIndices_);
}
