//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "engine/CartesianProductJoin.h"

#include "engine/CallFixedSize.h"
#include "util/Views.h"

namespace {
constexpr std::string_view recomputeMessage =
    "Cannot re-evaluate child results after applying limits, as the result set "
    "may have changed (which could result in different limits being applied). "
    "Cloning is also forbidden, as it would preserve potentially incorrect "
    "limits.";
}

// ____________________________________________________________________________
CartesianProductJoin::CartesianProductJoin(
    QueryExecutionContext* executionContext, Children children,
    size_t chunkSize)
    : Operation{executionContext},
      children_{std::move(children)},
      chunkSize_{chunkSize} {
  AD_CONTRACT_CHECK(!children_.empty());
  AD_CONTRACT_CHECK(ql::ranges::all_of(
      children_, [](auto& child) { return child != nullptr; }));
  // Ensure estimated largest tree is on the right side.
  ql::ranges::stable_sort(
      children_, {}, [](const auto& tree) { return tree->getSizeEstimate(); });

  // Check that the variables of the passed in operations are in fact
  // disjoint.
  auto variablesAreDisjoint = [this]() {
    // Insert all the variables from all the children into a hash set and return
    // false as soon as a duplicate is encountered.
    ad_utility::HashSet<Variable> vars;
    auto checkVarsForOp = [&vars](const Operation& op) {
      return ql::ranges::all_of(
          op.getExternallyVisibleVariableColumns() | ql::views::keys,
          [&vars](const Variable& variable) {
            return vars.insert(variable).second;
          });
    };
    return ql::ranges::all_of(childView(), checkVarsForOp);
  }();
  AD_CONTRACT_CHECK(variablesAreDisjoint);
}

// ____________________________________________________________________________
std::vector<QueryExecutionTree*> CartesianProductJoin::getChildren() {
  std::vector<QueryExecutionTree*> result;
  ql::ranges::copy(
      children_ | ql::views::transform([](auto& ptr) { return ptr.get(); }),
      std::back_inserter(result));
  return result;
}

// ____________________________________________________________________________
std::string CartesianProductJoin::getCacheKeyImpl() const {
  return "CARTESIAN PRODUCT JOIN " +
         ad_utility::lazyStrJoin(
             ql::views::transform(
                 childView(), [](auto& child) { return child.getCacheKey(); }),
             " ");
}

// ____________________________________________________________________________
size_t CartesianProductJoin::getResultWidth() const {
  auto view = childView() | ql::views::transform(&Operation::getResultWidth);
  return std::reduce(view.begin(), view.end(), 0UL, std::plus{});
}

// ____________________________________________________________________________
size_t CartesianProductJoin::getCostEstimate() {
  auto childSizes =
      childView() | ql::views::transform(&Operation::getCostEstimate);
  return getSizeEstimate() +
         std::reduce(childSizes.begin(), childSizes.end(), 0UL, std::plus{});
}

// ____________________________________________________________________________
uint64_t CartesianProductJoin::getSizeEstimateBeforeLimit() {
  auto view = childView() | ql::views::transform(&Operation::getSizeEstimate);
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
  return ql::ranges::any_of(childView(), &Operation::knownEmptyResult);
}

// ____________________________________________________________________________
void CartesianProductJoin::writeResultColumn(ql::span<Id> targetColumn,
                                             ql::span<const Id> inputColumn,
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
          return;
        }
        targetColumn[numRowsWritten] = inputColumn[i];
        ++numRowsWritten;
        checkCancellation();
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
Result CartesianProductJoin::computeResult(bool requestLaziness) {
  if (knownEmptyResult()) {
    return {IdTable{getResultWidth(), getExecutionContext()->getAllocator()},
            resultSortedOn(), LocalVocab{}};
  }
  auto [subResults, lazyResult] = calculateSubResults(requestLaziness);

  LocalVocab staticMergedVocab{};
  staticMergedVocab.mergeWith(
      subResults |
      ql::views::transform([](const auto& result) -> const LocalVocab& {
        return result->localVocab();
      }));

  if (!requestLaziness) {
    AD_CORRECTNESS_CHECK(!lazyResult);
    return {writeAllColumns(subResults | ql::views::transform(&Result::idTable),
                            getLimitOffset()._offset,
                            getLimitOffset().limitOrDefault()),
            resultSortedOn(), std::move(staticMergedVocab)};
  }

  if (lazyResult) {
    return {createLazyConsumer(std::move(staticMergedVocab),
                               std::move(subResults), std::move(lazyResult)),
            resultSortedOn()};
  }

  // Owning view wrapper to please gcc 11.
  return {produceTablesLazily(std::move(staticMergedVocab),
                              ad_utility::OwningView{std::move(subResults)} |
                                  ql::views::transform(&Result::idTable),
                              getLimitOffset()._offset,
                              getLimitOffset().limitOrDefault()),
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
CPP_template_def(typename R)(requires ql::ranges::random_access_range<R>)
    IdTable CartesianProductJoin::writeAllColumns(
        R idTables, size_t offset, size_t limit, size_t lastTableOffset) const {
  AD_CORRECTNESS_CHECK(offset >= lastTableOffset);
  IdTable result{getResultWidth(), getExecutionContext()->getAllocator()};
  // TODO<joka921> Find a solution to cheaply handle the case, that only a
  // single result is left. This can probably be done by using the
  // `Result`.

  auto sizesView = ql::views::transform(idTables, &IdTable::size);
  auto totalResultSize =
      std::reduce(sizesView.begin(), sizesView.end(), 1UL, std::multiplies{});

  if (!ql::ranges::empty(idTables) && sizesView.back() != 0) {
    totalResultSize += (totalResultSize / sizesView.back()) * lastTableOffset;
  } else {
    AD_CORRECTNESS_CHECK(lastTableOffset == 0);
  }

  LimitOffsetClause limitOffset{limit, offset};
  size_t totalSizeIncludingLimit = limitOffset.actualSize(totalResultSize);
  offset = limitOffset.actualOffset(totalResultSize);

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
    for (const auto& input : idTables) {
      size_t extraOffset =
          &input == &idTables.back() ? lastTableOffset * groupSize : 0;
      for (const auto& inputCol : input.getColumns()) {
        decltype(auto) resultCol = result.getColumn(resultColIdx);
        writeResultColumn(resultCol, inputCol, groupSize, offset - extraOffset);
        ++resultColIdx;
      }
      groupSize *= input.numRows();
    }
  }
  return result;
}

// _____________________________________________________________________________
std::pair<std::vector<std::shared_ptr<const Result>>,
          std::shared_ptr<const Result>>
CartesianProductJoin::calculateSubResults(bool requestLaziness) {
  AD_CONTRACT_CHECK(!forbiddenToRecompute_, recomputeMessage);
  std::vector<std::shared_ptr<const Result>> subResults;
  // We don't need to fully materialize the child results if we have a LIMIT
  // specified and an OFFSET of 0.
  // TODO<joka921> We could in theory also apply this optimization if a
  // non-zero OFFSET is specified, but this would make the algorithm more
  // complicated.
  std::optional<LimitOffsetClause> limitIfPresent = getLimitOffset();
  if (!getLimitOffset()._limit.has_value() || getLimitOffset()._offset != 0) {
    limitIfPresent = std::nullopt;
  }

  std::shared_ptr<const Result> lazyResult = nullptr;
  auto children = childView();
  AD_CORRECTNESS_CHECK(!ql::ranges::empty(children));
  // Get all child results (possibly with limit, see above).
  for (std::shared_ptr<QueryExecutionTree>& childTree : children_) {
    if (limitIfPresent.has_value() && childTree->supportsLimit()) {
      childTree->applyLimit(limitIfPresent.value());
      forbiddenToRecompute_ = true;
    }
    auto& child = *childTree->getRootOperation();
    // To preserve order of the columns we can only consume the first child
    // lazily. In the future this restriction may be lifted by permutating the
    // columns afterward.
    bool isLast = &child == &children.back();
    bool requestLazy = requestLaziness && isLast;
    auto result = child.getResult(
        false, requestLazy ? ComputationMode::LAZY_IF_SUPPORTED
                           : ComputationMode::FULLY_MATERIALIZED);

    if (!result->isFullyMaterialized()) {
      AD_CORRECTNESS_CHECK(isLast);
      lazyResult = std::move(result);
      continue;
    }

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

  return {std::move(subResults), std::move(lazyResult)};
}

// _____________________________________________________________________________
CPP_template_def(typename R)(requires ql::ranges::range<R>) Result::LazyResult
    CartesianProductJoin::produceTablesLazily(LocalVocab mergedVocab,
                                              R idTables, size_t offset,
                                              size_t limit,
                                              size_t lastTableOffset) const {
  using Lc = Result::IdTableLoopControl;
  auto get = [self = this, mergedVocab = std::move(mergedVocab),
              idTables = std::move(idTables), offset, limit,
              lastTableOffset]() mutable -> Result::IdTableLoopControl {
    if (limit == 0) {
      return Lc::makeBreak();
    }
    auto limitWithChunkSize = std::min(limit, self->chunkSize_);
    IdTable idTable =
        self->writeAllColumns(ql::ranges::ref_view(idTables), offset,
                              limitWithChunkSize, lastTableOffset);
    auto tableSize = idTable.size();
    AD_CORRECTNESS_CHECK(tableSize <= limit);
    if (idTable.empty()) {
      return Lc::makeBreak();
    }
    offset += tableSize;
    limit -= tableSize;
    auto makeRes = [&]() {
      return Result::IdTableVocabPair{std::move(idTable), mergedVocab.clone()};
    };
    // If limit was reduced to 0, or the last produced table was smaller
    // than the remaining limit, then all results have been produced
    if (limit > 0 && tableSize >= limitWithChunkSize) {
      // Not the last value
      return Lc::yieldValue(makeRes());
    } else {
      // This value is the last. BreakWithValue to end the loop.
      return Lc::breakWithValue(makeRes());
    }
  };
  return Result::LazyResult(
      ad_utility::InputRangeFromLoopControlGet(std::move(get)));
}

// _____________________________________________________________________________
Result::LazyResult CartesianProductJoin::createLazyConsumer(
    LocalVocab staticMergedVocab,
    ql::span<const std::shared_ptr<const Result>> subresults,
    std::shared_ptr<const Result> lazyResult) const {
  AD_CONTRACT_CHECK(lazyResult);
  std::vector<std::reference_wrapper<const IdTable>> idTables;
  idTables.reserve(subresults.size() + 1);
  for (const auto& result : subresults) {
    idTables.emplace_back(result->idTable());
  }
  auto get = [self = this, staticMergedVocab = std::move(staticMergedVocab),
              limit = getLimitOffset().limitOrDefault(),
              offset = getLimitOffset()._offset, idTables = std::move(idTables),
              lastTableOffset = size_t{0}, producedTableSize = size_t{0},
              idTableOpt = std::optional<Result::IdTableVocabPair>{}](
                 auto& idTableVocabPair) mutable {
    // These things have to be done after handling a single input, so we do them
    // at the beginning of each but the last iteration.
    if (idTableOpt.has_value()) {
      idTables.pop_back();
      lastTableOffset += idTableOpt->idTable_.size();
      limit -= producedTableSize;
      offset += producedTableSize;
      producedTableSize = 0;
    }

    idTableOpt = std::move(idTableVocabPair);
    auto& [idTable, localVocab] = idTableOpt.value();
    if (idTable.empty()) {
      return Result::IdTableLoopControl::makeContinue();
    }

    idTables.emplace_back(idTable);
    localVocab.mergeWith(staticMergedVocab);

    return Result::IdTableLoopControl::yieldAll(
        ad_utility::InputRangeTypeErased{
            ad_utility::OwningView{self->produceTablesLazily(
                std::move(localVocab),
                ql::views::transform(idTables,
                                     ad_utility::staticCast<const IdTable&>),
                offset, limit, lastTableOffset)} |
            ql::views::transform([&producedTableSize](auto& tableAndVocab) {
              producedTableSize += tableAndVocab.idTable_.size();
              return std::move(tableAndVocab);
            })});
  };
  return Result::LazyResult(ad_utility::CachingContinuableTransformInputRange(
      lazyResult->idTables(), std::move(get)));
}

// _____________________________________________________________________________
std::unique_ptr<Operation> CartesianProductJoin::cloneImpl() const {
  AD_CONTRACT_CHECK(!forbiddenToRecompute_, recomputeMessage);
  Children copy;
  copy.reserve(children_.size());
  for (const auto& operation : children_) {
    copy.push_back(operation->clone());
  }
  return std::make_unique<CartesianProductJoin>(_executionContext,
                                                std::move(copy), chunkSize_);
}
