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
string CartesianProductJoin::getCacheKeyImpl() const {
  return "CARTESIAN PRODUCT JOIN " +
         ad_utility::lazyStrJoin(
             std::views::transform(
                 childView(), [](auto& child) { return child.getCacheKey(); }),
             " ");
}

// ____________________________________________________________________________
size_t CartesianProductJoin::getResultWidth() const {
  auto view = childView() | std::views::transform(&Operation::getResultWidth);
  return std::reduce(view.begin(), view.end(), 0UL, std::plus{});
}

// ____________________________________________________________________________
size_t CartesianProductJoin::getCostEstimate() {
  auto childSizes =
      childView() | std::views::transform(&Operation::getCostEstimate);
  return getSizeEstimate() +
         std::reduce(childSizes.begin(), childSizes.end(), 0UL, std::plus{});
}

// ____________________________________________________________________________
uint64_t CartesianProductJoin::getSizeEstimateBeforeLimit() {
  auto view = childView() | std::views::transform(&Operation::getSizeEstimate);
  return std::reduce(view.begin(), view.end(), 1UL, std::multiplies{});
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
  // If children were empty, returning false would be the wrong behavior.
  AD_CORRECTNESS_CHECK(!children_.empty());
  return std::ranges::any_of(childView(), &Operation::knownEmptyResult);
}

// ____________________________________________________________________________
size_t CartesianProductJoin::writeResultColumn(std::span<Id> targetColumn,
                                               std::span<const Id> inputColumn,
                                               size_t groupSize,
                                               size_t offset) const {
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
      for (size_t u = groupStartIdx; u < groupSize; ++u) {
        if (numRowsWritten == targetSize) {
          return i - firstInputElementIdx + 1;
        }
        targetColumn[numRowsWritten] = inputColumn[i];
        ++numRowsWritten;
        checkCancellation();
      }
      if (numRowsWritten == targetSize) {
        return i - firstInputElementIdx + 1;
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

auto dynamicCartesianProduct(std::ranges::random_access_range auto values)
    -> cppcoro::generator<const std::vector<std::ranges::iterator_t<
        std::ranges::range_reference_t<decltype(values)>>>>
    requires(std::ranges::random_access_range<
             std::ranges::range_reference_t<decltype(values)>>) {
  using namespace std::ranges;
  if (any_of(values, empty)) {
    co_return;
  }
  std::vector<iterator_t<range_reference_t<decltype(values)>>> currentIterators;
  if (empty(values)) {
    co_yield currentIterators;
    co_return;
  }

  currentIterators.reserve(size(values));
  for (const auto& value : values) {
    currentIterators.push_back(begin(value));
  }

  while (true) {
    co_yield currentIterators;
    // co_yield transform(currentIterators, [](const auto& elem) ->
    // decltype(auto) { return *elem; });
    for (size_t i = 0; i < currentIterators.size(); i++) {
      size_t currentIndex = currentIterators.size() - 1 - i;
      currentIterators[currentIndex]++;
      if (currentIterators[currentIndex] != end(values[currentIndex])) {
        break;
      }
      if (currentIndex == 0) {
        co_return;
      }
      currentIterators[currentIndex] = begin(values[currentIndex]);
    }
  }
}

// ____________________________________________________________________________
ProtoResult CartesianProductJoin::computeResult(bool requestLaziness) {
  if (knownEmptyResult()) {
    return {IdTable{getResultWidth(), getExecutionContext()->getAllocator()},
            resultSortedOn(), LocalVocab{}};
  }
  auto [lazyResult, subResults] = calculateSubResults(requestLaziness);

  LocalVocab staticMergedVocab{};
  staticMergedVocab.mergeWith(
      subResults |
      std::views::transform([](const auto& result) -> const LocalVocab& {
        return result->localVocab();
      }));

  if (!requestLaziness) {
    AD_CORRECTNESS_CHECK(!lazyResult);
    return {writeAllColumns(std::ranges::ref_view(subResults)),
            resultSortedOn(), std::move(staticMergedVocab)};
  }

  size_t offsetFromFront = 0;
  size_t currentSize = 1;
  for (const auto& sr : subResults) {
    size_t newSize = currentSize * sr->idTable().numRows();
    if (newSize > CHUNK_SIZE && currentSize != 1) {
      break;
    }
    currentSize = newSize;
    offsetFromFront++;
  }

  if (lazyResult) {
    return {
        createLazyConsumer(std::move(staticMergedVocab), std::move(lazyResult),
                           std::move(subResults), offsetFromFront),
        resultSortedOn()};
  }
  return {createLazyProducer(std::move(staticMergedVocab),
                             std::move(subResults), offsetFromFront),
          resultSortedOn()};
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

// _____________________________________________________________________________
IdTable CartesianProductJoin::writeAllColumns(
    std::ranges::range auto subResults,
    IdTableWithMetadata* partialIdTable) const {
  IdTable result{getResultWidth(), getExecutionContext()->getAllocator()};
  // TODO<joka921> Find a solution to cheaply handle the case, that only a
  // single result is left. This can probably be done by using the
  // `ProtoResult`.

  auto sizesView = std::views::transform(
      subResults, [](const auto& child) { return child->idTable().size(); });
  auto totalResultSize =
      std::reduce(sizesView.begin(), sizesView.end(), 1UL, std::multiplies{});

  size_t realOffset =
      partialIdTable ? partialIdTable->totalOffset_ : getLimit()._offset;
    
  if (partialIdTable) {
    totalResultSize *= std::min(
        std::max(CHUNK_SIZE, totalResultSize) / totalResultSize,
        partialIdTable->idTable_.size() - partialIdTable->tableOffset_);
    // Make sure the offset doesn't cause a row to be partially consumed.
    if (partialIdTable->totalOffset_ >= totalResultSize) {
      partialIdTable->totalOffset_ -= totalResultSize;
      totalResultSize = 0;
    } else {
      totalResultSize -= partialIdTable->totalOffset_;
      partialIdTable->totalOffset_ = 0;
    }
  }

  size_t totalSizeIncludingLimit =
      partialIdTable ? std::min(totalResultSize, partialIdTable->totalLimit_)
                     : getLimit().actualSize(totalResultSize);

  try {
    result.resize(totalSizeIncludingLimit);
  } catch (
      const ad_utility::detail::AllocationExceedsLimitException& exception) {
    throw std::runtime_error{
        "The memory limit was exceeded during the computation of a "
        "cross-product. Check if this cross-product is intentional or if you "
        "have mistyped a variable name."};
  }

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
        writeResultColumn(resultCol, inputCol, groupSize, realOffset);
        ++resultColIdx;
      }
      groupSize *= input.numRows();
    }
    if (partialIdTable) {
      size_t offsetForPartialTable =
          realOffset + partialIdTable->tableOffset_ * groupSize;
      size_t consumedRows = 0;
      for (const auto& inputCol : partialIdTable->idTable_.getColumns()) {
        decltype(auto) resultCol = result.getColumn(resultColIdx);
        consumedRows = writeResultColumn(resultCol, inputCol, groupSize,
                                         offsetForPartialTable);
        ++resultColIdx;
      }
      partialIdTable->tableOffset_ += consumedRows;
    }
  }
  if (partialIdTable) {
    partialIdTable->totalLimit_ -= result.size();
    partialIdTable->done_ =
        partialIdTable->tableOffset_ >= partialIdTable->idTable_.size() ||
        totalSizeIncludingLimit == 0;
  }
  return result;
}

// _____________________________________________________________________________
std::pair<std::shared_ptr<const Result>,
          std::vector<std::shared_ptr<const Result>>>
CartesianProductJoin::calculateSubResults(bool requestLaziness) {
  std::vector<std::shared_ptr<const Result>> subResults;
  // We don't need to fully materialize the child results if we have a LIMIT
  // specified and an OFFSET of 0.
  // TODO<joka921> We could in theory also apply this optimization if a
  // non-zero OFFSET is specified, but this would make the algorithm more
  // complicated.
  std::optional<LimitOffsetClause> limitIfPresent = getLimit();
  if (!getLimit()._limit.has_value() || getLimit()._offset != 0) {
    limitIfPresent = std::nullopt;
  }

  std::shared_ptr<const Result> lazyResult = nullptr;
  auto children = childView();
  AD_CORRECTNESS_CHECK(!std::ranges::empty(children));
  // Get all child results (possibly with limit, see above).
  for (Operation& child :
       std::ranges::subrange{children.begin(), children.end() - 1}) {
    if (limitIfPresent.has_value() && child.supportsLimit()) {
      child.setLimit(limitIfPresent.value());
    }
    auto result = child.getResult();

    const auto& table = result->idTable();
    // Early stopping: If one of the results is empty, we can stop early.
    if (table.empty()) {
      // Push so the total size will be zero.
      subResults.push_back(std::move(result));
      break;
    }

    // If one of the children is the neutral element (because of a triple with
    // zero variables), we can simply ignore it here.
    if (table.numRows() == 1 && table.numColumns() == 0) {
      continue;
    }
    // Example for the following calculation: If we have a LIMIT of 1000 and
    // the first child already has a result of size 100, then the second child
    // needs to evaluate only its first 10 results. The +1 is because integer
    // divisions are rounded down by default.
    if (limitIfPresent.has_value()) {
      limitIfPresent.value()._limit =
          limitIfPresent.value()._limit.value() / result->idTable().size() + 1;
    }
    subResults.push_back(std::move(result));
  }

  // To preserve order of the columns we can only consume the first child
  // lazily. In the future this restriction may be lifted by permutating the
  // columns afterwards.
  if (limitIfPresent.has_value() && children.back().supportsLimit()) {
    children.back().setLimit(limitIfPresent.value());
  }
  auto result = children.back().getResult(
      false, requestLaziness ? ComputationMode::LAZY_IF_SUPPORTED
                             : ComputationMode::FULLY_MATERIALIZED);
  if (!result->isFullyMaterialized()) {
    lazyResult = std::move(result);
  } else if (result->idTable().numRows() != 1 ||
             result->idTable().numColumns() != 0) {
    subResults.push_back(std::move(result));
  }
  return {std::move(lazyResult), std::move(subResults)};
}

// _____________________________________________________________________________
Result::Generator CartesianProductJoin::createLazyProducer(
    LocalVocab staticMergedVocab,
    std::vector<std::shared_ptr<const Result>> subresults,
    size_t offsetFromFront, std::optional<size_t> limit,
    size_t* offsetPtr) const {
  std::span subspan{subresults};
  auto trailingTables =
      subspan.subspan(std::min(offsetFromFront + 1, subresults.size())) |
      std::views::transform(
          [](const auto& child) -> const IdTable& { return child->idTable(); });
  size_t columnOffset = 0;
  for (const auto& result :
       subspan.subspan(0, std::min(offsetFromFront + 1, subresults.size()))) {
    columnOffset += result->idTable().numColumns();
  }
  size_t actualLimit =
      limit.has_value() ? limit.value() : getLimit().limitOrDefault();
  size_t offset = offsetPtr ? *offsetPtr : getLimit()._offset;
  for (auto& row : dynamicCartesianProduct(std::move(trailingTables))) {
    std::optional<IdTableWithMetadata> itwm =
        offsetFromFront < subresults.size()
            ? std::optional<IdTableWithMetadata>{{subresults
                                                      .at(offsetFromFront)
                                                      ->idTable(),
                                                  offset, 0, actualLimit}}
            : std::nullopt;
    do {
      auto idTable =
          writeAllColumns(subspan.subspan(0, offsetFromFront),
                          itwm.has_value() ? &itwm.value() : nullptr);
      AD_CORRECTNESS_CHECK(actualLimit >= idTable.size());
      actualLimit -= idTable.size();
      if (itwm.has_value()) {
        offset = itwm->totalOffset_;
        // TODO<RobinTF> consider skipping empty id tables.
        size_t currentColumn = columnOffset;
        auto valueView =
            row |
            std::views::transform([](const auto& elem) { return *elem; }) |
            std::views::join;
        for (const auto& value : valueView) {
          std::ranges::fill(idTable.getColumn(currentColumn), value);
          currentColumn++;
          checkCancellation();
        }
      }
      co_yield {std::move(idTable), staticMergedVocab.clone()};
    } while (itwm.has_value() && !itwm->done_);
  }
  if (offsetPtr) {
    *offsetPtr = offset;
  }
}

// _____________________________________________________________________________
Result::Generator CartesianProductJoin::createLazyConsumer(
    LocalVocab staticMergedVocab, std::shared_ptr<const Result> lazyResult,
    std::vector<std::shared_ptr<const Result>> subresults,
    size_t offsetFromFront) const {
  size_t columnOffset = 0;
  for (const auto& subresult : subresults) {
    columnOffset += subresult->idTable().numColumns();
  }
  size_t limit = getLimit().limitOrDefault();
  size_t offset = getLimit()._offset;
  for (auto& [idTable, localVocab] : lazyResult->idTables()) {
    if (offsetFromFront == subresults.size()) {
      localVocab.mergeWith(std::span{&staticMergedVocab, 1});
      IdTableWithMetadata itwm{idTable, offset, 0, limit};
      do {
        auto innerIdTable =
            writeAllColumns(std::ranges::ref_view(subresults), &itwm);
        limit -= innerIdTable.size();
        offset = itwm.totalOffset_;
        co_yield {std::move(innerIdTable), localVocab.clone()};
      } while (!itwm.done_);
    } else {
      // Case that processes each row individually
      for (const auto& row : idTable) {
        for (auto& [innerIdTable, mergedVocab] :
             createLazyProducer(staticMergedVocab.clone(), subresults,
                                offsetFromFront, limit, &offset)) {
          limit -= innerIdTable.size();
          size_t currentColumn = columnOffset;
          for (const auto& value : row) {
            std::ranges::fill(innerIdTable.getColumn(currentColumn), value);
            currentColumn++;
            checkCancellation();
          }
          mergedVocab.mergeWith(std::span{&localVocab, 1});
          co_yield {std::move(innerIdTable), std::move(mergedVocab)};
        }
      }
    }
  }
}
