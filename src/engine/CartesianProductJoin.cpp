//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "engine/CartesianProductJoin.h"

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
  return std::accumulate(view.begin(), view.end(), 0ul, std::plus{});
}

// ____________________________________________________________________________
size_t CartesianProductJoin::getCostEstimate() {
  auto childSizes =
      childView() | std::views::transform(&Operation::getCostEstimate);
  return getSizeEstimate() +
         std::accumulate(childSizes.begin(), childSizes.end(), 0ul);
}

// ____________________________________________________________________________
uint64_t CartesianProductJoin::getSizeEstimateBeforeLimit() {
  auto view = childView() | std::views::transform(&Operation::getSizeEstimate);
  return std::accumulate(view.begin(), view.end(), 1ul, std::multiplies{});
}

// ____________________________________________________________________________
float CartesianProductJoin::getMultiplicity([[maybe_unused]] size_t col) {
  // Deliberately a dummy as we always perform this operation last.
  return 1;
}

// ____________________________________________________________________________
bool CartesianProductJoin::knownEmptyResult() {
  return std::ranges::any_of(children_, [](auto& child) {
    return child->getRootOperation()->knownEmptyResult();
  });
}

// ____________________________________________________________________________
ResultTable CartesianProductJoin::computeResult() {
  IdTable idTable{getExecutionContext()->getAllocator()};
  idTable.setNumColumns(getResultWidth());
  std::vector<std::shared_ptr<const ResultTable>> subResults;

  // We don't need to fully materialize the child results if we have a  LIMIT
  // specified and an OFFSET of 0.
  // TODO<joka921> We could in theory also apply this optimization if an
  // non-zero OFFSET is specified, but this would make the algorithm more
  // complicated.
  std::optional<LimitOffsetClause> limitIfPresent = getLimit();
  if (!getLimit()._limit.has_value() || getLimit()._offset != 0) {
    limitIfPresent = std::nullopt;
  }

  // We currently materialize all child results in advance. We could also
  // materialize them lazily, but then we wouldn't know the total result size in
  // advance.
  // TODO<joka921> revisit this once we have implemented lazy results or results
  // that don't need to copy when being dynamically resized.

  for (auto& child : children_) {
    if (limitIfPresent.has_value() &&
        child->getRootOperation()->supportsLimit()) {
      child->getRootOperation()->setLimit(limitIfPresent.value());
    }
    subResults.push_back(child->getResult());
    if (limitIfPresent.has_value()) {
      limitIfPresent.value()._limit =
          limitIfPresent.value()._limit.value() /
              std::max(subResults.back()->size(), 1ul) +
          1;
    }
    // Early stopping: If one of the results is empty, we can stop early.
    if (subResults.back()->size() == 0) {
      break;
    }
  };

  auto sizesView = std::views::transform(
      subResults, [](const auto& child) { return child->size(); });
  auto totalSize = std::accumulate(sizesView.begin(), sizesView.end(), 1ul,
                                   std::multiplies{});

  size_t totalSizeIncludingLimit = getLimit().actualSize(totalSize);
  size_t offset = getLimit().actualOffset(totalSize);

  idTable.resize(totalSizeIncludingLimit);

  auto checkForTimeout = checkTimeoutAfterNCallsFactory(1'000'000);
  if (totalSizeIncludingLimit != 0) {
    size_t stride = 1;
    size_t columnIndex = 0;
    for (auto& subResultPtr : subResults) {
      const auto& input = subResultPtr->idTable();
      const size_t inputSize = input.numRows();
      const size_t startRound = offset % (inputSize * stride) / stride;
      const size_t startOffset = offset % stride;
      for (const auto& column : input.getColumns()) {
        decltype(auto) target = idTable.getColumn(columnIndex);
        // TODO<C++23> Use `std::views::stride`
        size_t k = 0;
        [&, startOffset = startOffset, startRound = startRound]() mutable {
          for (;;) {
            for (size_t i = startRound; i < inputSize; ++i) {
              for (size_t u = startOffset; u < stride; ++u) {
                if (k == totalSizeIncludingLimit) {
                  return;
                }
                target[k] = column[i];
                ++k;
                checkForTimeout();
              }
              startOffset = 0;
            }
            startRound = 0;
          }
        }();
        ++columnIndex;
      }
      stride *= input.numRows();
    }
  }
  auto subResultsDeref = std::views::transform(
      subResults, [](auto& x) -> decltype(auto) { return *x; });
  return {std::move(idTable), resultSortedOn(),
          ResultTable::getSharedLocalVocabFromNonEmptyOf(subResultsDeref)};
}

// ____________________________________________________________________________
VariableToColumnMap CartesianProductJoin::computeVariableToColumnMap() const {
  VariableToColumnMap result;
  size_t offset = 0;
  for (const auto& child : childView()) {
    for (auto varCol : child.getExternallyVisibleVariableColumns()) {
      varCol.second.columnIndex_ += offset;
      result.insert(std::move(varCol));
    }
    offset += child.getResultWidth();
  }
  return result;
}
