//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "engine/CartesianProductJoin.h"

#include "engine/CallFixedSize.h"

// ____________________________________________________________________________
CartesianProductJoin::CartesianProductJoin(
    QueryExecutionContext* executionContext, Children children)
    : Operation{executionContext}, children_{std::move(children)} {
  AD_CONTRACT_CHECK(!children_.empty());
  AD_CONTRACT_CHECK(std::ranges::all_of(
      children_, [](auto& child) { return child != nullptr; }));

  // Check that the variables of the passed in operations are in fact
  // disjoint.
  auto variablesAreDisjoint = [this]() {
    // Insert all the variables from all the children into a hash set and return
    // false as soon as a duplicate is encountered.
    ad_utility::HashSet<Variable> vars;
    auto checkVarsForOp = [&vars](const Operation& op) {
      return std::ranges::all_of(
          op.getExternallyVisibleVariableColumns() | std::views::keys,
          [&vars](const Variable& variable) {
            return vars.insert(variable).second;
          });
    };
    return std::ranges::all_of(childView(), checkVarsForOp);
  }();
  AD_CONTRACT_CHECK(variablesAreDisjoint);
}

// ____________________________________________________________________________
std::vector<QueryExecutionTree*> CartesianProductJoin::getChildren() {
  std::vector<QueryExecutionTree*> result;
  std::ranges::copy(
      children_ | std::views::transform([](auto& ptr) { return ptr.get(); }),
      std::back_inserter(result));
  return result;
}

// ____________________________________________________________________________
string CartesianProductJoin::asStringImpl(size_t indent) const {
  return "CARTESIAN PRODUCT JOIN " +
         ad_utility::lazyStrJoin(
             std::views::transform(
                 childView(),
                 [indent](auto& child) { return child.asString(indent); }),
             " ");
}

// ____________________________________________________________________________
size_t CartesianProductJoin::getResultWidth() const {
  auto view = childView() | std::views::transform(&Operation::getResultWidth);
  return std::accumulate(view.begin(), view.end(), 0UL, std::plus{});
}

// ____________________________________________________________________________
size_t CartesianProductJoin::getCostEstimate() {
  auto childSizes =
      childView() | std::views::transform(&Operation::getCostEstimate);
  return getSizeEstimate() + std::accumulate(childSizes.begin(),
                                             childSizes.end(), 0UL,
                                             std::plus{});
}

// ____________________________________________________________________________
uint64_t CartesianProductJoin::getSizeEstimateBeforeLimit() {
  auto view = childView() | std::views::transform(&Operation::getSizeEstimate);
  return std::accumulate(view.begin(), view.end(), 1UL, std::multiplies{});
}

// ____________________________________________________________________________
float CartesianProductJoin::getMultiplicity([[maybe_unused]] size_t col) {
  // We could in theory estimate the multiplicity of the variables, but the
  // benefit of this is questionable as we always perform the cartesian product
  // as late as possible without having many options for query planning.
  return 1;
}

// ____________________________________________________________________________
bool CartesianProductJoin::knownEmptyResult() {
  return std::ranges::any_of(childView(), &Operation::knownEmptyResult);
}

// Copy each element from the `inputColumn` `groupSize` times to the
// `targetColumn`. Repeat until the `targetColumn` is copletely filled. Skip the
// first `offset` write operations to the `targetColumn`. Call `checkForTimeout`
// after each write. If `StaticGroupSize != 0`, then the group size is known at
// compile time which allows for more efficient loop processing for very small
// group sizes.
template <size_t StaticGroupSize = 0>
static void writeResultColumn(std::span<Id> targetColumn,
                              std::span<const Id> inputColumn, size_t groupSize,
                              size_t offset, auto& checkForTimeout) {
  if (StaticGroupSize != 0) {
    AD_CORRECTNESS_CHECK(StaticGroupSize == groupSize);
  }
  // Copy each element from the `inputColumn` `groupSize` times to
  // the `targetColumn`, repeat until the `targetColumn` is completely filled.
  size_t numRowsWritten = 0;
  const size_t inputSize = inputColumn.size();
  const size_t targetSize = targetColumn.size();
  // If we have a nonzero offset then we have to compute at which element
  // from the input we have to start the copying and how many repetitions of
  // this element have already happened "before" the offset.
  size_t firstInputElementIdx = offset % (inputSize * groupSize) / groupSize;
  size_t groupStartIdx = offset % groupSize;
  while (true) {
    for (size_t i = firstInputElementIdx; i < inputSize; ++i) {
      auto writeGroup = [&](size_t actualGroupSize) {
        for (size_t u = groupStartIdx; u < actualGroupSize; ++u) {
          if (numRowsWritten == targetSize) {
            return;
          }
          targetColumn[numRowsWritten] = inputColumn[i];
          ++numRowsWritten;
          checkForTimeout();
        }
      };
      if constexpr (StaticGroupSize == 0) {
        writeGroup(groupSize);
      } else {
        writeGroup(StaticGroupSize);
      }
      if (numRowsWritten == targetSize) {
        return;
      }
      // only the first round might be incomplete because of the offset, all
      // subsequent rounds start at 0.
      groupStartIdx = 0;
    }
    // only the first round might be incomplete because of the offset, all
    // subsequent rounds start at 0.
    firstInputElementIdx = 0;
  }
}
// ____________________________________________________________________________
ResultTable CartesianProductJoin::computeResult() {
  IdTable result{getExecutionContext()->getAllocator()};
  result.setNumColumns(getResultWidth());
  std::vector<std::shared_ptr<const ResultTable>> subResults;

  // We don't need to fully materialize the child results if we have a LIMIT
  // specified and an OFFSET of 0.
  // TODO<joka921> We could in theory also apply this optimization if a
  // non-zero OFFSET is specified, but this would make the algorithm more
  // complicated.
  std::optional<LimitOffsetClause> limitIfPresent = getLimit();
  if (!getLimit()._limit.has_value() || getLimit()._offset != 0) {
    limitIfPresent = std::nullopt;
  }

  // Get all child results (possibly with limit, see above).
  for (auto& child : childView()) {
    if (limitIfPresent.has_value() && child.supportsLimit()) {
      child.setLimit(limitIfPresent.value());
    }
    subResults.push_back(child.getResult());
    // Early stopping: If one of the results is empty, we can stop early.
    if (subResults.back()->size() == 0) {
      break;
    }
    // Example for the following calculation: If we have a LIMIT of 1000 and
    // the first child already has a result of size 100, then the second child
    // needs to evaluate only its first 10 results. The +1 is because integer
    // divisions are rounded down by default.
    if (limitIfPresent.has_value()) {
      limitIfPresent.value()._limit =
          limitIfPresent.value()._limit.value() / subResults.back()->size() + 1;
    }
  }

  auto sizesView = std::views::transform(
      subResults, [](const auto& child) { return child->size(); });
  auto totalResultSize = std::accumulate(sizesView.begin(), sizesView.end(),
                                         1UL, std::multiplies{});

  size_t totalSizeIncludingLimit = getLimit().actualSize(totalResultSize);
  size_t offset = getLimit().actualOffset(totalResultSize);

  result.resize(totalSizeIncludingLimit);

  auto checkForTimeout = checkTimeoutAfterNCallsFactory(1'000'000);
  if (totalSizeIncludingLimit != 0) {
    // A `groupSize` of N means that each row of the current result is copied N
    // times adjacent to each other.
    size_t groupSize = 1;
    // The index of the next column in the output that hasn't been written so
    // far.
    size_t resultColIdx = 0;
    for (auto& subResultPtr : subResults) {
      const auto& input = subResultPtr->idTable();
      for (const auto& inputCol : input.getColumns()) {
        decltype(auto) resultCol = result.getColumn(resultColIdx);
        ad_utility::callFixedSize(groupSize, [&]<size_t I>() {
          writeResultColumn<I>(resultCol, inputCol, groupSize, offset,
                               checkForTimeout);
        });
        ++resultColIdx;
      }
      groupSize *= input.numRows();
    }
  }

  // Dereference all the subresult pointers because `getSharedLocalVocabFrom...`
  // requires a range of references, not pointers.
  auto subResultsDeref = std::views::transform(
      subResults, [](auto& x) -> decltype(auto) { return *x; });
  return {std::move(result), resultSortedOn(),
          ResultTable::getSharedLocalVocabFromNonEmptyOf(subResultsDeref)};
}

// ____________________________________________________________________________
VariableToColumnMap CartesianProductJoin::computeVariableToColumnMap() const {
  VariableToColumnMap result;
  // It is crucial that we also count the columns in the inputs to which no
  // variable was assigned. This is managed by the `offset` variable.
  size_t offset = 0;
  for (const auto& child : childView()) {
    for (auto varCol : child.getExternallyVisibleVariableColumns()) {
      varCol.second.columnIndex_ += offset;
      result.insert(std::move(varCol));
    }
    // `getResultWidth` contains all the columns, not only the ones to which a
    // variable is assigned.
    offset += child.getResultWidth();
  }
  return result;
}
