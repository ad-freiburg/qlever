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
    : Operation{qec}, subtree_{std::move(subtree)}, keepIndices_{keepIndices} {}

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
Result::Generator Distinct::lazyDistinct(Result::Generator input,
                                         bool yieldOnce) const {
  IdTable aggregateTable{subtree_->getResultWidth(), allocator()};
  LocalVocab aggregateVocab{};
  std::optional<typename IdTableStatic<WIDTH>::row_type> previousRow =
      std::nullopt;
  for (auto& [idTable, localVocab] : input) {
    IdTable result = distinct<WIDTH>(std::move(idTable), previousRow);
    if (!result.empty()) {
      previousRow.emplace(result.asStaticView<WIDTH>().back());
      if (yieldOnce) {
        aggregateVocab.mergeWith(std::array{std::move(localVocab)});
        aggregateTable.insertAtEnd(result);
      } else {
        co_yield {std::move(result), std::move(localVocab)};
      }
    }
  }
  if (yieldOnce) {
    co_yield {std::move(aggregateTable), std::move(aggregateVocab)};
  }
}

// _____________________________________________________________________________
ProtoResult Distinct::computeResult(bool requestLaziness) {
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

  auto generator =
      CALL_FIXED_SIZE(width, &Distinct::lazyDistinct, this,
                      std::move(subRes->idTables()), !requestLaziness);
  return requestLaziness
             ? ProtoResult{std::move(generator), resultSortedOn()}
             : ProtoResult{cppcoro::getSingleElement(std::move(generator)),
                           resultSortedOn()};
}

// _____________________________________________________________________________
bool Distinct::matchesRow(const auto& a, const auto& b) const {
  return std::ranges::all_of(keepIndices_,
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

  // Variant of `std::ranges::unique` that allows to skip the begin rows of
  // elements found in the previous table.
  auto begin =
      std::ranges::find_if(result, [this, &previousRow](const auto& row) {
        // Without explicit this clang seems to
        // think the this capture is redundant.
        return !previousRow.has_value() ||
               !this->matchesRow(row, previousRow.value());
      });
  auto end = result.end();

  auto dest = result.begin();
  if (begin == dest) {
    // Optimization to avoid redundant move operations.
    begin = std::ranges::adjacent_find(begin, end,
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
    begin = std::ranges::unique_copy(begin, begin + allowedOffset,
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
      begin = std::ranges::find_if(begin, begin + allowedOffset,
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
