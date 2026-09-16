// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: 2015 - 2017 Björn Buchhold (buchhold@cs.uni-freiburg.de)
// Author: 2023 -      Johannes Kalmbach (kalmbach@cs.uni-freiburg.de)

#include "engine/OrderBy.h"

#include <cmath>
#include <sstream>

#include "engine/CallFixedSize.h"
#include "engine/QueryExecutionTree.h"
#include "global/RuntimeParameters.h"
#include "global/ValueIdComparators.h"
#include "index/IdTableUtils.h"
#include "util/TransparentFunctors.h"

// _____________________________________________________________________________
size_t OrderBy::getResultWidth() const { return subtree_->getResultWidth(); }

// _____________________________________________________________________________
OrderBy::OrderBy(QueryExecutionContext* qec,
                 std::shared_ptr<QueryExecutionTree> subtree,
                 std::vector<std::pair<ColumnIndex, bool>> sortIndices)
    : Operation{qec},
      subtree_{std::move(subtree)},
      sortIndices_{std::move(sortIndices)} {
  AD_CONTRACT_CHECK(!sortIndices_.empty());
  AD_CONTRACT_CHECK(ql::ranges::all_of(
      sortIndices_,
      [this](ColumnIndex index) { return index < getResultWidth(); },
      ad_utility::first));
}

// _____________________________________________________________________________
std::string OrderBy::getCacheKeyImpl() const {
  std::ostringstream os;
  os << "ORDER BY on columns:";

  // TODO<joka921> This produces exactly the same format as SORT operations
  // which is crucial for caching. Please refactor those classes to one class
  // (this is only an optimization for sorts on a single column)
  for (auto ind : sortIndices_) {
    os << (ind.second ? "desc(" : "asc(") << ind.first << ") ";
  }
  os << "\n" << subtree_->getCacheKey();
  return std::move(os).str();
}

// _____________________________________________________________________________
std::string OrderBy::getDescriptor() const {
  std::string orderByVars;
  const auto& varCols = subtree_->getVariableColumns();
  for (auto [sortIndex, isDescending] : sortIndices_) {
    for (const auto& [var, varIndex] : varCols) {
      if (sortIndex == varIndex.columnIndex_) {
        using namespace std::string_literals;
        std::string s = isDescending ? " DESC("s : " ASC("s;
        orderByVars += s + var.name() + ")";
      }
    }
  }
  return "OrderBy on" + orderByVars;
}

// _____________________________________________________________________________
size_t OrderBy::getCostEstimate() {
  size_t size = getSizeEstimateBeforeLimit();
  size_t subcost = subtree_->getCostEstimate();
  // If the input is already sorted by the first sort column, the result can
  // often be computed in linear time, see `computeResultForSortedInput`.
  if (isInputSortedOnFirstSortColumn()) {
    return size + subcost;
  }
  size_t logSize =
      std::max(size_t(1), static_cast<size_t>(logb(static_cast<double>(size))));
  return size * logSize + subcost;
}

// _____________________________________________________________________________
bool OrderBy::isInputSortedOnFirstSortColumn() const {
  const auto& sortedOn = subtree_->resultSortedOn();
  return !sortedOn.empty() && sortedOn.front() == sortIndices_.front().first;
}

namespace {
// A contiguous range `[begin_, end_)` of rows of an `IdTable`. If `reversed_`
// is true, the rows are to be output in reverse order.
struct RowRange {
  size_t begin_;
  size_t end_;
  bool reversed_;
};

// If the `column`, which must be sorted in the internal order of the `Id`s
// (that is, by their bits), contains only `Int`s or only `Double`s, possibly
// preceded by `Undefined` values, return the row ranges which, concatenated,
// yield the column in the ascending order of `ORDER BY`. Otherwise return
// `std::nullopt`.
//
// The datatype bits are the most significant bits of an `Id`, so a column that
// is sorted by bits is grouped by datatype, and the check is O(1) apart from
// the binary searches for the range boundaries. The internal order deviates
// from the semantic order as follows (see `valueIdComparators::compareByBits`):
// ints are `[0 .. max, min .. -1]`, doubles are `[0.0 .. +inf, NaN, -0.0 ..
// -inf, -NaN]`, where the sign bit of a `NaN` may be set as well. `ORDER BY`
// puts all `NaN`s after all other doubles (see `makeComparatorForNans`) and
// `Undefined` before everything else.
std::optional<std::vector<RowRange>> getRowRangesForSortedNumericColumn(
    ql::span<const Id> column) {
  // Return the index of the first row at or after `firstDefined` for which the
  // `predicate` is false. The predicate must be monotone on the column.
  auto partitionPoint = [&column](size_t firstDefined, auto predicate) {
    return static_cast<size_t>(
        std::partition_point(column.begin() + firstDefined, column.end(),
                             predicate) -
        column.begin());
  };
  size_t firstDefined =
      partitionPoint(0, [](Id id) { return id.isUndefined(); });
  if (firstDefined == column.size()) {
    return std::nullopt;
  }
  Datatype type = column[firstDefined].getDatatype();
  if (column.back().getDatatype() != type) {
    return std::nullopt;
  }

  std::vector<RowRange> ranges;
  if (firstDefined > 0) {
    ranges.push_back({0, firstDefined, false});
  }
  if (type == Datatype::Int) {
    size_t firstNegative =
        partitionPoint(firstDefined, [](Id id) { return id.getInt() >= 0; });
    ranges.push_back({firstNegative, column.size(), false});
    ranges.push_back({firstDefined, firstNegative, false});
  } else if (type == Datatype::Double) {
    auto isNegative = [](Id id) { return std::signbit(id.getDouble()); };
    auto isNan = [](Id id) { return std::isnan(id.getDouble()); };
    size_t firstPositiveNan = partitionPoint(
        firstDefined, [&](Id id) { return !isNegative(id) && !isNan(id); });
    size_t firstNegative =
        partitionPoint(firstDefined, [&](Id id) { return !isNegative(id); });
    size_t firstNegativeNan = partitionPoint(
        firstDefined, [&](Id id) { return !(isNegative(id) && isNan(id)); });
    ranges.push_back({firstNegative, firstNegativeNan, true});
    ranges.push_back({firstDefined, firstPositiveNan, false});
    ranges.push_back({firstPositiveNan, firstNegative, false});
    ranges.push_back({firstNegativeNan, column.size(), false});
  } else {
    return std::nullopt;
  }
  return ranges;
}
}  // namespace

// _____________________________________________________________________________
std::optional<IdTable> OrderBy::computeResultForSortedInput(
    const IdTableView<0>& input) const {
  if (sortIndices_.size() != 1 || !isInputSortedOnFirstSortColumn()) {
    return std::nullopt;
  }
  auto [column, isDescending] = sortIndices_.front();
  auto ranges = getRowRangesForSortedNumericColumn(input.getColumn(column));
  if (!ranges.has_value()) {
    return std::nullopt;
  }
  if (isDescending) {
    ql::ranges::reverse(ranges.value());
    for (auto& range : ranges.value()) {
      range.reversed_ = !range.reversed_;
    }
  }

  IdTable result{input.numColumns(), allocator()};
  result.reserve(input.numRows());
  for (const auto& [begin, end, reversed] : ranges.value()) {
    if (!reversed) {
      result.insertAtEnd(input, begin, end);
      continue;
    }
    size_t oldSize = result.numRows();
    result.resize(oldSize + (end - begin));
    for (size_t i = 0; i < input.numColumns(); ++i) {
      ql::ranges::reverse_copy(input.getColumn(i).subspan(begin, end - begin),
                               result.getColumn(i).begin() + oldSize);
    }
  }
  return result;
}

// _____________________________________________________________________________
Result OrderBy::computeResult([[maybe_unused]] bool requestLaziness) {
  using std::endl;
  AD_LOG_DEBUG << "Getting sub-result for OrderBy result computation..."
               << endl;
  std::shared_ptr<const Result> subRes = subtree_->getResult();
  const auto& subTable = subRes->idTableView();

  if (auto result = computeResultForSortedInput(subTable)) {
    runtimeInfo().addDetail("sorted-numeric-input", true);
    checkCancellation();
    return {std::move(result).value(), resultSortedOn(),
            subRes->getSharedLocalVocab()};
  }

  // TODO<joka921> proper timeout for sorting operations
  getExecutionContext()->getSortPerformanceEstimator().throwIfEstimateTooLong(
      subTable.numRows(), subTable.numColumns(), deadline_,
      "Sort for COUNT(DISTINCT *)");

  AD_LOG_DEBUG << "OrderBy result computation..." << endl;
  IdTable idTable = subRes->cloneIdTable();

  size_t width = idTable.numColumns();

  // TODO<joka921> Measure (as soon as we have the benchmark merged)
  // whether it is beneficial to manually instantiate the comparison when
  // sorting by only one or two columns.

  // TODO<joka921> In the case of a single variable, it might be more efficient
  // to first sort by the ID values and then "repair" the resulting range by
  // some O(n) algorithms (see `computeResultForSortedInput`), or even by
  // returning lazy generators that yield the repaired order.

  // TODO<joka921> For proper sorting of the local vocab we also need to
  // add some logic for the proper sorting.

  // TODO<joka921> Undefined values should always be at the end, no matter
  // if the ordering is ascending or descending.

  // TODO<joka921> If we know, that all the sort columns contain only datatypes
  // for which the `internal` order is also the `semantic` order, or if a column
  // only contains a single datatype, then we can use more efficient
  // implementations here.

  // Return true iff `rowA` comes before `rowB` in the sort order specified by
  // `sortIndices_`.
  auto comparison = [this](const auto& row1, const auto& row2) -> bool {
    for (auto& [column, isDescending] : sortIndices_) {
      if (row1[column] == row2[column]) {
        continue;
      }
      bool isLessThan =
          toBoolNotUndef(valueIdComparators::compareIds<
                         valueIdComparators::ComparisonForIncompatibleTypes::
                             CompareByType>(
              row1[column], row2[column], valueIdComparators::Comparison::LT));
      return isLessThan != isDescending;
    }
    return false;
  };

  // We cannot use the `CALL_FIXED_SIZE` macro here because the `sort` function
  // is templated not only on the integer `I` (which the `callFixedSize`
  // function deals with) but also on the `comparison`.
  ad_utility::callFixedSizeVi(width, [&idTable, &comparison](auto I) {
    IdTableUtils::sort<I>(&idTable, comparison);
  });
  // We can't check during sort, so reset status here
  cancellationHandle_->resetWatchDogState();
  checkCancellation();
  AD_LOG_DEBUG << "OrderBy result computation done." << endl;
  return {std::move(idTable), resultSortedOn(), subRes->getSharedLocalVocab()};
}

// ___________________________________________________________________
OrderBy::SortedVariables OrderBy::getSortedVariables() const {
  SortedVariables result;
  for (const auto& [colIdx, isDescending] : sortIndices_) {
    using enum AscOrDesc;
    result.emplace_back(subtree_->getVariableAndInfoByColumnIndex(colIdx).first,
                        isDescending ? Desc : Asc);
  }
  return result;
}

// _____________________________________________________________________________
std::unique_ptr<Operation> OrderBy::cloneImpl() const {
  return std::make_unique<OrderBy>(_executionContext, subtree_->clone(),
                                   sortIndices_);
}
