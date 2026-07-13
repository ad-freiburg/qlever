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
  AD_LOG_DEBUG << "Getting sub-result for distinct result computation..."
               << endl;
  std::shared_ptr<const Result> subRes = subtree_->getResult(true);

  AD_LOG_DEBUG << "Distinct result computation..." << endl;
  size_t width = subtree_->getResultWidth();
  if (subRes->isFullyMaterialized()) {
    IdTable idTable =
        ad_utility::callFixedSizeVi(width, [&, self = this](auto width) {
          return self->outOfPlaceDistinct<width>(subRes->idTableView());
        });
    AD_LOG_DEBUG << "Distinct result computation done." << endl;
    return {std::move(idTable), resultSortedOn(),
            subRes->getSharedLocalVocab()};
  }

  auto generator =
      ad_utility::callFixedSizeVi(width, [&, self = this](auto width) {
        return self->lazyDistinct<width>(subRes->idTables(), !requestLaziness);
      });
  return requestLaziness
             ? Result{std::move(generator), resultSortedOn()}
             : Result{ad_utility::getSingleElement(std::move(generator)),
                      resultSortedOn()};
}

namespace {
// Return the indices of all rows of the `table` (a table or a view) with
// `index >= startIndex` that differ from their predecessor row in at least
// one of the `keepIndices` columns. The comparison is performed directly on
// the datatype bytes and payload words of the split column storage; only
// where IDs of type `LocalVocabIndex` are involved (those do not compare
// bitwise), the semantic `Id` comparison is used. `checkCancellation` is
// called periodically.
template <typename Table>
std::vector<size_t> indicesOfRowsThatDifferFromPredecessor(
    const Table& table, ql::span<const ColumnIndex> keepIndices,
    size_t startIndex, const auto& checkCancellation) {
  constexpr uint8_t localVocabType =
      static_cast<uint8_t>(Datatype::LocalVocabIndex);
  struct ColumnSpans {
    ql::span<const uint64_t> payloads_;
    ql::span<const uint8_t> types_;
  };
  std::vector<ColumnSpans> columns;
  columns.reserve(keepIndices.size());
  for (ColumnIndex keepIndex : keepIndices) {
    auto column = table.getColumn(keepIndex);
    columns.push_back({column.payloadSpan(), column.datatypeSpan()});
  }

  auto rowsDiffer = [&columns](size_t a, size_t b) {
    for (const auto& [payloads, types] : columns) {
      if (payloads[a] != payloads[b] || types[a] != types[b]) {
        // Bitwise different. This means semantically different, unless an
        // ID of type `LocalVocabIndex` is involved.
        if (types[a] != localVocabType && types[b] != localVocabType) {
          return true;
        }
        if (Id::fromBits({types[a], payloads[a]}) !=
            Id::fromBits({types[b], payloads[b]})) {
          return true;
        }
      }
    }
    return false;
  };

  std::vector<size_t> result;
  for (size_t i = std::max<size_t>(startIndex, 1); i < table.numRows(); ++i) {
    if (rowsDiffer(i, i - 1)) {
      result.push_back(i);
    }
    if ((i & ((size_t{1} << 16) - 1)) == 0) {
      checkCancellation();
    }
  }
  return result;
}
}  // namespace

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
  AD_LOG_DEBUG << "Distinct on " << dynInput.size() << " elements.\n";
  IdTableStatic<WIDTH> result = std::move(dynInput).toStatic<WIDTH>();
  if (result.empty()) {
    return std::move(result).toDynamic();
  }

  // Compute the indices of the rows that are kept: the first row is kept iff
  // it differs from the last row of the previous table, all other rows are
  // kept iff they differ from their predecessor.
  auto checkCancellationLambda = [this] { checkCancellation(); };
  std::vector<size_t> keptIndices = indicesOfRowsThatDifferFromPredecessor(
      result, keepIndices_, 1, checkCancellationLambda);
  bool keepFirstRow =
      !previousRow.has_value() || !matchesRow(result[0], previousRow.value());
  if (keepFirstRow) {
    keptIndices.insert(keptIndices.begin(), 0);
  }
  checkCancellation();

  if (keptIndices.size() == result.size()) {
    // All rows are distinct, no copying is required at all.
    return std::move(result).toDynamic();
  }

  // Move the kept rows to the front, column by column. Note: The kept
  // indices are ascending and `keptIndices[k] >= k` always holds, so the
  // forward copy is safe.
  for (size_t c = 0; c < result.numColumns(); ++c) {
    decltype(auto) column = result.getColumn(c);
    auto payloads = column.payloadSpan();
    auto types = column.datatypeSpan();
    for (size_t k = 0; k < keptIndices.size(); ++k) {
      payloads[k] = payloads[keptIndices[k]];
      types[k] = types[keptIndices[k]];
    }
  }
  result.resize(keptIndices.size());
  checkCancellation();

  AD_LOG_DEBUG << "Distinct done.\n";
  return std::move(result).toDynamic();
}

// _____________________________________________________________________________
template <size_t WIDTH>
IdTable Distinct::outOfPlaceDistinct(const IdTableView<0>& dynInput) const {
  AD_CONTRACT_CHECK(keepIndices_.size() <= dynInput.numColumns());
  AD_LOG_DEBUG << "Distinct on " << dynInput.size() << " elements.\n";
  auto inputView = dynInput.asStaticView<WIDTH>();
  IdTableStatic<WIDTH> output{dynInput.numColumns(), allocator()};
  if (inputView.empty()) {
    return std::move(output).toDynamic();
  }

  // Compute the indices of the distinct rows (the first row is always kept)
  // and then copy them to the output column by column.
  auto checkCancellationLambda = [this] { checkCancellation(); };
  std::vector<size_t> keptIndices = indicesOfRowsThatDifferFromPredecessor(
      inputView, keepIndices_, 1, checkCancellationLambda);
  keptIndices.insert(keptIndices.begin(), 0);
  checkCancellation();
  output.insertSubsetAtEnd(inputView, keptIndices);
  checkCancellation();

  AD_LOG_DEBUG << "Distinct done.\n";
  return std::move(output).toDynamic();
}

// _____________________________________________________________________________
std::unique_ptr<Operation> Distinct::cloneImpl() const {
  return std::make_unique<Distinct>(_executionContext, subtree_->clone(),
                                    keepIndices_);
}

// ____________________________________________________________________________
IdTable Distinct::outOfPlaceDistinctForTesting(const IdTable& input) const {
  size_t width = input.numColumns();
  return ad_utility::callFixedSizeVi(width, [&, self = this](auto width) {
    return self->outOfPlaceDistinct<width>(input.asStaticView<0>());
  });
}
