// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "engine/IndexScan.h"

#include <absl/container/inlined_vector.h>
#include <absl/strings/str_join.h>

#include <sstream>
#include <string>
#include <utility>

#include "engine/QueryExecutionTree.h"
#include "index/IndexImpl.h"
#include "parser/ParsedQuery.h"
#include "util/InputRangeUtils.h"
#include "util/Iterators.h"

using std::string;
using LazyScanMetadata = CompressedRelationReader::LazyScanMetadata;

// Return the number of `Variables` given the `TripleComponent` values for
// `subject_`, `predicate` and `object`.
static size_t getNumberOfVariables(const TripleComponent& subject,
                                   const TripleComponent& predicate,
                                   const TripleComponent& object) {
  return static_cast<size_t>(subject.isVariable()) +
         static_cast<size_t>(predicate.isVariable()) +
         static_cast<size_t>(object.isVariable());
}

// _____________________________________________________________________________
IndexScan::IndexScan(QueryExecutionContext* qec, PermutationPtr permutation,
                     LocatedTriplesSharedState locatedTriplesSharedState,
                     const SparqlTripleSimple& triple, Graphs graphsToFilter,
                     std::optional<ScanSpecAndBlocks> scanSpecAndBlocks,
                     VarsToKeep varsToKeep)
    : Operation(qec),
      permutation_(std::move(permutation)),
      locatedTriplesSharedState_(std::move(locatedTriplesSharedState)),
      subject_(triple.s_),
      predicate_(triple.p_),
      object_(triple.o_),
      graphsToFilter_{std::move(graphsToFilter)},
      scanSpecAndBlocks_{
          std::move(scanSpecAndBlocks).value_or(getScanSpecAndBlocks())},
      scanSpecAndBlocksIsPrefiltered_{scanSpecAndBlocks.has_value()},
      numVariables_(getNumberOfVariables(subject_, predicate_, object_)),
      varsToKeep_(std::move(varsToKeep)) {
  AD_CONTRACT_CHECK(permutation_ != nullptr);
  AD_CONTRACT_CHECK(locatedTriplesSharedState_ != nullptr);

  // We previously had `nullptr`s here in unit tests. This is no longer
  // necessary nor allowed.
  AD_CONTRACT_CHECK(qec != nullptr);
  for (auto& [idx, variable] : triple.additionalScanColumns_) {
    additionalColumns_.push_back(idx);
    additionalVariables_.push_back(variable);
  }
  std::tie(sizeEstimateIsExact_, sizeEstimate_) = computeSizeEstimate();

  // Check the following invariant: All the variables must be at the end of the
  // permuted triple. For example in the PSO permutation, either only the O, or
  // the S and O, or all three of P, S, O, or none of them can be variables, all
  // other combinations are not supported.
  auto permutedTriple = getPermutedTriple();
  for (size_t i = 0; i < 3 - numVariables_; ++i) {
    AD_CONTRACT_CHECK(!permutedTriple.at(i)->isVariable());
  }
  for (size_t i = 3 - numVariables_; i < permutedTriple.size(); ++i) {
    AD_CONTRACT_CHECK(permutedTriple.at(i)->isVariable());
  }
}

// _____________________________________________________________________________
IndexScan::IndexScan(QueryExecutionContext* qec,
                     Permutation::Enum permutationType,
                     const SparqlTripleSimple& triple, Graphs graphsToFilter,
                     std::optional<ScanSpecAndBlocks> scanSpecAndBlocks)
    : IndexScan(qec,
                qec->getIndex().getImpl().getPermutationPtr(permutationType),
                qec->locatedTriplesSharedState(), triple,
                std::move(graphsToFilter), std::move(scanSpecAndBlocks)) {}

// _____________________________________________________________________________
IndexScan::IndexScan(QueryExecutionContext* qec, PermutationPtr permutation,
                     LocatedTriplesSharedState locatedTriplesSharedState,
                     const TripleComponent& s, const TripleComponent& p,
                     const TripleComponent& o,
                     std::vector<ColumnIndex> additionalColumns,
                     std::vector<Variable> additionalVariables,
                     Graphs graphsToFilter, ScanSpecAndBlocks scanSpecAndBlocks,
                     bool scanSpecAndBlocksIsPrefiltered, VarsToKeep varsToKeep,
                     bool sizeEstimateIsExact, size_t sizeEstimate)
    : Operation(qec),
      permutation_(std::move(permutation)),
      locatedTriplesSharedState_(std::move(locatedTriplesSharedState)),
      subject_(s),
      predicate_(p),
      object_(o),
      graphsToFilter_(std::move(graphsToFilter)),
      scanSpecAndBlocks_(std::move(scanSpecAndBlocks)),
      scanSpecAndBlocksIsPrefiltered_(scanSpecAndBlocksIsPrefiltered),
      numVariables_(getNumberOfVariables(subject_, predicate_, object_)),
      sizeEstimate_{sizeEstimate},
      sizeEstimateIsExact_{sizeEstimateIsExact},
      additionalColumns_(std::move(additionalColumns)),
      additionalVariables_(std::move(additionalVariables)),
      varsToKeep_{std::move(varsToKeep)} {
  AD_CONTRACT_CHECK(qec != nullptr);
  AD_CONTRACT_CHECK(permutation_ != nullptr);
  AD_CONTRACT_CHECK(locatedTriplesSharedState_ != nullptr);
  determineMultiplicities();
}

// _____________________________________________________________________________
string IndexScan::getCacheKeyImpl() const {
  std::ostringstream os;
  // This string only represents the type of permutation, like "SPO".
  auto permutationString = Permutation::toString(permutation().permutation());

  if (numVariables_ == 3) {
    os << "SCAN FOR FULL INDEX " << permutationString;

  } else {
    os << "SCAN " << permutationString << " with ";
    auto addKey = [&os, &permutationString, this](size_t idx) {
      auto keyString = permutationString.at(idx);
      const auto& key = getPermutedTriple().at(idx)->toRdfLiteral();
      os << keyString << " = \"" << key << "\"";
    };
    for (size_t i = 0; i < 3 - numVariables_; ++i) {
      addKey(i);
      os << ", ";
    }
  }
  // This is important to distinguish special from regular permutations.
  os << " on index " << permutation().onDiskBase();
  if (!additionalColumns_.empty()) {
    os << " Additional Columns: ";
    os << absl::StrJoin(additionalColumns(), " ");
  }

  os << " ";
  graphsToFilter_.format(os, &TripleComponent::toRdfLiteral);

  if (varsToKeep_.has_value()) {
    os << " column subset "
       << absl::StrJoin(getSubsetForStrippedColumns(), ",");
  }
  return std::move(os).str();
}

// _____________________________________________________________________________
bool IndexScan::canResultBeCachedImpl() const {
  return !scanSpecAndBlocksIsPrefiltered_;
};

// _____________________________________________________________________________
string IndexScan::getDescriptor() const {
  return absl::StrCat("IndexScan ", permutation().readableName(), " ",
                      subject_.toString(), " ", predicate_.toString(), " ",
                      object_.toString());
}

// _____________________________________________________________________________
size_t IndexScan::getResultWidth() const {
  if (varsToKeep_.has_value()) {
    return varsToKeep_.value().size();
  }
  return numVariables_ + additionalVariables_.size();
}

// _____________________________________________________________________________
std::vector<ColumnIndex> IndexScan::resultSortedOn() const {
  std::vector<ColumnIndex> result;
  for (auto i : ad_utility::integerRange(ColumnIndex{numVariables_})) {
    result.push_back(i);
  }
  for (size_t i = 0; i < additionalColumns_.size(); ++i) {
    if (additionalColumns_.at(i) == ADDITIONAL_COLUMN_GRAPH_ID) {
      result.push_back(numVariables_ + i);
    }
  }

  if (varsToKeep_.has_value()) {
    auto permutation = getSubsetForStrippedColumns();
    for (auto it = result.begin(); it != result.end(); ++it) {
      if (!ad_utility::contains(permutation, *it)) {
        result.erase(it, result.end());
        return result;
      }
    }
  }
  return result;
}

// _____________________________________________________________________________
std::optional<std::shared_ptr<QueryExecutionTree>>
IndexScan::getUpdatedQueryExecutionTreeWithPrefilterApplied(
    const std::vector<PrefilterVariablePair>& prefilterVariablePairs) const {
  // If there is a LIMIT or OFFSET clause that constrains the scan, we cannot
  // apply prefiltering. Also, if there is no block metadata, there is nothing
  // to prefilter.
  if (!getLimitOffset().isUnconstrained() ||
      scanSpecAndBlocks_.sizeBlockMetadata_ == 0) {
    return std::nullopt;
  }

  // Get the variable by which this `IndexScan` is sorted, and its
  // corresponding column index. If there is no variable, we cannot apply
  // prefiltering (and there typically is no need to).
  auto sortedVarAndColIndex =
      getSortedVariableAndMetadataColumnIndexForPrefiltering();
  if (!sortedVarAndColIndex.has_value()) {
    return std::nullopt;
  }

  // Return a new `IndexScan` with updated `scanSpecAndBlocks_`, by
  // intersecting its block ranges with the block ranges from the applicable
  // prefilters.
  const auto& [sortedVar, colIndex] = sortedVarAndColIndex.value();
  auto it =
      ql::ranges::find(prefilterVariablePairs, sortedVar, ad_utility::second);
  if (it != prefilterVariablePairs.end()) {
    const auto& vocab = getIndex().getVocab();
    const auto& blockMetadataRanges =
        prefilterExpressions::detail::logicalOps::getIntersectionOfBlockRanges(
            it->first->evaluate(
                vocab, getScanSpecAndBlocks().getBlockMetadataSpan(), colIndex),
            scanSpecAndBlocks_.blockMetadata_);

    return makeCopyWithPrefilteredScanSpecAndBlocks(
        {scanSpecAndBlocks_.scanSpec_, blockMetadataRanges});
  }

  // If no prefilter applies, return `std::nullopt`.
  return std::nullopt;
}

// _____________________________________________________________________________
VariableToColumnMap IndexScan::computeVariableToColumnMap() const {
  VariableToColumnMap variableToColumnMap;
  auto isContained = [&](const Variable& var) {
    return !varsToKeep_.has_value() || varsToKeep_.value().contains(var);
  };
  auto addCol = [&isContained, &variableToColumnMap,
                 nextColIdx = ColumnIndex{0}](const Variable& var) mutable {
    if (!isContained(var)) {
      return;
    }
    // All the columns of an index scan only contain defined values.
    variableToColumnMap[var] = makeAlwaysDefinedColumn(nextColIdx);
    ++nextColIdx;
  };

  for (const TripleComponent* const ptr : getPermutedTriple()) {
    if (ptr->isVariable()) {
      addCol(ptr->getVariable());
    }
  }
  ql::ranges::for_each(additionalVariables_, addCol);
  return variableToColumnMap;
}

//______________________________________________________________________________
std::shared_ptr<QueryExecutionTree>
IndexScan::makeCopyWithPrefilteredScanSpecAndBlocks(
    ScanSpecAndBlocks scanSpecAndBlocks) const {
  // Make a (cheap) copy of this `IndexScan`. The size estimates (last two
  // args) are computed next, so just set them to dummy values in this call.
  auto copy = ad_utility::makeExecutionTree<IndexScan>(
      getExecutionContext(), permutation_, locatedTriplesSharedState_, subject_,
      predicate_, object_, additionalColumns_, additionalVariables_,
      graphsToFilter_, std::move(scanSpecAndBlocks), true, varsToKeep_, false,
      size_t{0});

  // Compute the size estimate for the prefiltered `scanSpecAndBlocks`.
  //
  // NOTE: This function is only called when prefiltering actually happened.
  // The code in this functions avoids that the size estimate is computed
  // twice, as it happened in a previous implementation.
  auto indexScan =
      std::dynamic_pointer_cast<IndexScan>(copy->getRootOperation());
  AD_CORRECTNESS_CHECK(indexScan != nullptr);
  std::tie(indexScan->sizeEstimateIsExact_, indexScan->sizeEstimate_) =
      indexScan->computeSizeEstimate();
  return copy;
}

// _____________________________________________________________________________
Result::LazyResult IndexScan::chunkedIndexScan() const {
  return Result::LazyResult{
      ad_utility::CachingTransformInputRange(getLazyScan(), [](auto& table) {
        return Result::IdTableVocabPair{std::move(table), LocalVocab{}};
      })};
}

// _____________________________________________________________________________
IdTable IndexScan::materializedIndexScan() const {
  IdTable idTable = permutation().scan(scanSpecAndBlocks_, additionalColumns(),
                                       cancellationHandle_,
                                       locatedTriplesState(), getLimitOffset());
  AD_LOG_DEBUG << "IndexScan result computation done.\n";
  checkCancellation();
  idTable = makeApplyColumnSubset()(std::move(idTable));
  AD_CORRECTNESS_CHECK(idTable.numColumns() == getResultWidth());
  return idTable;
}

// _____________________________________________________________________________
Result IndexScan::computeResult(bool requestLaziness) {
  AD_LOG_DEBUG << "IndexScan result computation...\n";
  if (requestLaziness) {
    return {chunkedIndexScan(), resultSortedOn()};
  }
  return {materializedIndexScan(), getResultSortedOn(), LocalVocab{}};
}

// _____________________________________________________________________________
const Permutation& IndexScan::permutation() const {
  AD_CONTRACT_CHECK(permutation_ != nullptr);
  return *permutation_;
}

// _____________________________________________________________________________
const LocatedTriplesState& IndexScan::locatedTriplesState() const {
  AD_CONTRACT_CHECK(locatedTriplesSharedState_ != nullptr);
  return *locatedTriplesSharedState_;
}

// _____________________________________________________________________________
std::pair<bool, size_t> IndexScan::computeSizeEstimate() const {
  AD_CORRECTNESS_CHECK(_executionContext);

  // For a full index scan (think `?s ?p ?o`), simply use the total number
  // of triples (read from `<basename>.meta-data.json`) as estimate. See the
  // comment before the declaration of this function for details.
  if (numVariables() == 3 && additionalVariables().empty() &&
      !scanSpecAndBlocksIsPrefiltered_) {
    size_t numTriples = _executionContext->getIndex().numTriples().normal;
    size_t numChanges =
        permutation()
            .getLocatedTriplesForPermutation(locatedTriplesState())
            .numTriples();
    return {numChanges == 0, numTriples};
  }

  // For other scans, sum up the size estimates for each block.
  //
  // NOTE: Starting from C++20, we could use `std::midpoint` to compute the
  // mean of `lower` and `upper` in a safe way.
  auto [lower, upper] = permutation().getSizeEstimateForScan(
      scanSpecAndBlocks_, locatedTriplesState());
  return {lower == upper, lower + (upper - lower) / 2};
}

// _____________________________________________________________________________
size_t IndexScan::getExactSize() const {
  AD_CORRECTNESS_CHECK(_executionContext);
  return permutation().getResultSizeOfScan(scanSpecAndBlocks_,
                                           locatedTriplesState());
}

// _____________________________________________________________________________
size_t IndexScan::getCostEstimate() {
  // If we have a limit present, we only have to read the first
  // `limit + offset` elements.
  return getLimitOffset().upperBound(getSizeEstimateBeforeLimit());
}

// _____________________________________________________________________________
void IndexScan::determineMultiplicities() {
  multiplicity_ = [this]() -> std::vector<float> {
    const auto& idx = getIndex();
    if (numVariables_ == 0) {
      return {};
    } else if (numVariables_ == 1) {
      // There are no duplicate triples in RDF and two elements are fixed.
      return {1.0f};
    } else if (numVariables_ == 2) {
      return idx.getMultiplicities(*getPermutedTriple()[0], permutation(),
                                   locatedTriplesState());
    } else {
      AD_CORRECTNESS_CHECK(numVariables_ == 3);
      return idx.getMultiplicities(permutation());
    }
  }();
  multiplicity_.resize(multiplicity_.size() + additionalColumns_.size(), 1.0f);

  if (varsToKeep_.has_value()) {
    std::vector<float> actualMultiplicites;
    for (size_t column : getSubsetForStrippedColumns()) {
      actualMultiplicites.push_back(multiplicity_.at(column));
    }
    multiplicity_ = std::move(actualMultiplicites);
  }
  AD_CONTRACT_CHECK(multiplicity_.size() == getResultWidth());
}

// _____________________________________________________________________________
std::array<const TripleComponent* const, 3> IndexScan::getPermutedTriple()
    const {
  std::array<const TripleComponent* const, 3> triple{&subject_, &predicate_,
                                                     &object_};
  // TODO<joka921> This place has to be changed once we have a permutation
  // that is primarily sorted by G (the graph id).
  return permutation().keyOrder().permuteTriple(triple);
}

// _____________________________________________________________________________
ScanSpecification IndexScan::getScanSpecification() const {
  const IndexImpl& index = getIndex().getImpl();
  return getScanSpecificationTc().toScanSpecification(index);
}

// _____________________________________________________________________________
ScanSpecificationAsTripleComponent IndexScan::getScanSpecificationTc() const {
  auto permutedTriple = getPermutedTriple();
  return {*permutedTriple[0], *permutedTriple[1], *permutedTriple[2],
          graphsToFilter_};
}

// _____________________________________________________________________________
std::optional<std::pair<Variable, ColumnIndex>>
IndexScan::getSortedVariableAndMetadataColumnIndexForPrefiltering() const {
  if (numVariables_ < 1) {
    return std::nullopt;
  }
  const auto& permutedTriple = getPermutedTriple();
  size_t colIdx = 3 - numVariables_;
  const auto& tripleComp = permutedTriple.at(colIdx);
  AD_CORRECTNESS_CHECK(tripleComp->isVariable());
  return std::make_pair(tripleComp->getVariable(), colIdx);
}

// ___________________________________________________________________________
Permutation::ScanSpecAndBlocks IndexScan::getScanSpecAndBlocks() const {
  return permutation().getScanSpecAndBlocks(getScanSpecification(),
                                            locatedTriplesState());
}

// _____________________________________________________________________________
CompressedRelationReader::IdTableGeneratorInputRange IndexScan::getLazyScan(
    std::optional<std::vector<CompressedBlockMetadata>> blocks) const {
  // If there is a LIMIT or OFFSET clause that constrains the scan
  // (which can happen with an explicit subquery), we cannot use the prefiltered
  // blocks, as we currently have no mechanism to include limits and offsets
  // into the prefiltering (`std::nullopt` means `scan all blocks`).
  auto filteredBlocks =
      getLimitOffset().isUnconstrained() ? std::move(blocks) : std::nullopt;
  auto lazyScanAllCols = permutation().lazyScan(
      scanSpecAndBlocks_, filteredBlocks, additionalColumns(),
      cancellationHandle_, locatedTriplesState(), getLimitOffset());

  return CompressedRelationReader::IdTableGeneratorInputRange{
      ad_utility::CachingTransformInputRange<
          ad_utility::OwningView<
              CompressedRelationReader::IdTableGeneratorInputRange>,
          decltype(makeApplyColumnSubset()), LazyScanMetadata>{
          std::move(lazyScanAllCols), makeApplyColumnSubset()}};
};

// _____________________________________________________________________________
std::optional<Permutation::MetadataAndBlocks> IndexScan::getMetadataForScan()
    const {
  return permutation().getMetadataAndBlocks(scanSpecAndBlocks_,
                                            locatedTriplesState());
};

// _____________________________________________________________________________
std::array<CompressedRelationReader::IdTableGeneratorInputRange, 2>
IndexScan::lazyScanForJoinOfTwoScans(const IndexScan& s1, const IndexScan& s2) {
  AD_CONTRACT_CHECK(s1.numVariables_ <= 3 && s2.numVariables_ <= 3);
  AD_CONTRACT_CHECK(s1.numVariables_ >= 1 && s2.numVariables_ >= 1);
  // This function only works for single column joins. This means that the first
  // variable of both scans must be equal, but all other variables of the scans
  // (if present) must be different.
  const auto& getFirstVariable = [](const IndexScan& scan) {
    auto numVars = scan.numVariables();
    AD_CORRECTNESS_CHECK(numVars <= 3);
    size_t indexOfFirstVar = 3 - numVars;
    ad_utility::HashSet<Variable> otherVars;
    for (size_t i = indexOfFirstVar + 1; i < 3; ++i) {
      const auto& el = *scan.getPermutedTriple()[i];
      if (el.isVariable()) {
        otherVars.insert(el.getVariable());
      }
    }
    return std::pair{*scan.getPermutedTriple()[3 - numVars],
                     std::move(otherVars)};
  };

  auto [first1, other1] = getFirstVariable(s1);
  auto [first2, other2] = getFirstVariable(s2);
  AD_CONTRACT_CHECK(first1 == first2);

  size_t numTotal = other1.size() + other2.size();
  for (auto& var : other1) {
    other2.insert(var);
  }
  AD_CONTRACT_CHECK(other2.size() == numTotal);

  auto metaBlocks1 = s1.getMetadataForScan();
  auto metaBlocks2 = s2.getMetadataForScan();

  if (!metaBlocks1.has_value() || !metaBlocks2.has_value()) {
    return {{}};
  }
  auto [blocks1, blocks2] = CompressedRelationReader::getBlocksForJoin(
      metaBlocks1.value(), metaBlocks2.value());

  std::array result{s1.getLazyScan(blocks1), s2.getLazyScan(blocks2)};
  result[0].details().numBlocksAll_ = metaBlocks1.value().sizeBlockMetadata_;
  result[1].details().numBlocksAll_ = metaBlocks2.value().sizeBlockMetadata_;
  return result;
}

// _____________________________________________________________________________
CompressedRelationReader::IdTableGeneratorInputRange
IndexScan::lazyScanForJoinOfColumnWithScan(
    ql::span<const Id> joinColumn) const {
  AD_EXPENSIVE_CHECK(ql::ranges::is_sorted(joinColumn));
  AD_CORRECTNESS_CHECK(numVariables_ <= 3 && numVariables_ > 0);

  auto metaBlocks = getMetadataForScan();
  if (!metaBlocks.has_value() || joinColumn.empty()) {
    return {};
  }
  auto matchingBlocks =
      [&]() -> std::optional<std::vector<CompressedBlockMetadata>> {
    bool hasUndef = joinColumn.front().isUndefined();
    if (hasUndef) {
      return std::nullopt;
    }
    auto blocks = CompressedRelationReader::getBlocksForJoin(
        joinColumn, metaBlocks.value());
    return std::move(blocks.matchingBlocks_);
  }();
  auto result = getLazyScan(std::move(matchingBlocks));
  result.details().numBlocksAll_ = metaBlocks.value().sizeBlockMetadata_;
  return result;
}

// _____________________________________________________________________________
void IndexScan::updateRuntimeInfoForLazyScan(
    const LazyScanMetadata& metadata,
    RuntimeInformation::SendPriority sendPriority) {
  auto& rti = runtimeInfo();
  rti.status_ = RuntimeInformation::Status::lazilyMaterializedInProgress;
  rti.numRows_ = metadata.numElementsYielded_;
  rti.totalTime_ = metadata.blockingTime_;
  rti.addDetail("num-blocks-read", metadata.numBlocksRead_);
  rti.addDetail("num-blocks-all", metadata.numBlocksAll_);
  rti.addDetail("num-elements-read", metadata.numElementsRead_);

  // Add more details, but only if the respective value is non-zero.
  auto updateIfPositive = [&rti](const auto& value, const std::string& key) {
    if (value > 0) {
      rti.addDetail(key, value);
    }
  };
  updateIfPositive(metadata.numBlocksSkippedBecauseOfGraph_,
                   "num-blocks-skipped-graph");
  updateIfPositive(metadata.numBlocksPostprocessed_,
                   "num-blocks-postprocessed");
  updateIfPositive(metadata.numBlocksWithUpdate_, "num-blocks-with-update");
  signalQueryUpdate(sendPriority);
}

// Store a Generator and its corresponding iterator as well as unconsumed values
// resulting from the generator.
struct IndexScan::SharedGeneratorState {
  // The generator that yields the tables to be joined with the index scan.
  Result::LazyResult generator_;
  // The column index of the join column in the tables yielded by the generator.
  ColumnIndex joinColumn_;
  // Metadata and blocks of this index scan.
  Permutation::MetadataAndBlocks metaBlocks_;
  // If true, filter the join side (skip non-matching inputs). If false, pass
  // through all inputs even if they don't match any blocks.
  bool filterJoinSide_ = true;
  // The iterator of the generator that is currently being consumed.
  std::optional<Result::LazyResult::iterator> iterator_ = std::nullopt;
  // Values returned by the generator that have not been re-yielded yet.
  // Typically we expect only 3 or less values to be prefetched (this is an
  // implementation detail of `BlockZipperJoinImpl`).
  using PrefetchStorage = absl::InlinedVector<Result::IdTableVocabPair, 3>;
  PrefetchStorage prefetchedValues_{};
  // Metadata of blocks that still need to be read.
  std::vector<CompressedBlockMetadata> pendingBlocks_{};
  // The join column entry in the last block that has already been fetched.
  std::optional<Id> lastEntryInBlocks_ = std::nullopt;
  // Indicates if the generator has yielded any undefined values.
  bool hasUndef_ = false;
  // Indicates if the generator has been fully consumed.

  // Note: If the scan side is completely exhausted, we still might see
  // matching blocks from the join side that match a previously yielded block
  // from the scan side, hence the necessity of the `ScanExhausted` status.
  enum struct Status { Running, ScanSideExhausted, BothSidesExhausted };
  Status status_ = Status::Running;

  // Advance the `iterator` to the next non-empty table. Set `hasUndef_` to true
  // if the first table is undefined. Also set `status_` to `Exhausted` if the
  // generator has been fully consumed.
  void advanceInputToNextNonEmptyTable() {
    bool firstStep = !iterator_.has_value();
    if (iterator_.has_value()) {
      ++iterator_.value();
    } else {
      iterator_ = generator_.begin();
    }
    auto& iterator = iterator_.value();
    iterator = ql::ranges::find_if(
        iterator, generator_.end(),
        [](const auto& pair) { return !pair.idTable_.empty(); });
    bool foundNonEmptyTable = iterator_ != generator_.end();
    if (!foundNonEmptyTable) {
      status_ = Status::BothSidesExhausted;
    }
    // Set the undef flag if the first table is undefined.
    if (firstStep) {
      hasUndef_ = foundNonEmptyTable &&
                  iterator->idTable_.at(0, joinColumn_).isUndefined();
    }
  }

  // If (in the case of the join side not being filtered), the buffer for tables
  // from the join side grows too large, find a dummy block (a block from the
  // index scan, that might match nothing on the join side) to the result. This
  // is used in `fetch()` to enforce the invariant, that it always returns with
  // nonempty next values for both the join and scan side.
  void pushDummyBlockIfBufferTooLarge() {
    constexpr int maxBufferSize = 5;
    if (prefetchedValues_.size() <= maxBufferSize) {
      return;
    }
    // Find the last value in the join column of the last prefetched table.
    const auto& lastPrefetched = prefetchedValues_.back();
    auto lastJoinColumn = lastPrefetched.idTable_.getColumn(joinColumn_);
    AD_CORRECTNESS_CHECK(!lastJoinColumn.empty());
    Id lastValue = lastJoinColumn.back();
    const auto& metaBlockView = metaBlocks_.getBlockMetadataView();

    // If there are no more blocks left in the index scan, we can't push a
    // dummy block. This should never happen, as `pushDummyBlock...` is never
    // called under these conditions.
    AD_CORRECTNESS_CHECK(!ql::ranges::empty(metaBlockView));

    const auto& block = *metaBlockView.begin();
    AD_CORRECTNESS_CHECK(CompressedRelationReader::getRelevantIdFromTriple(
                             block.firstTriple_, metaBlocks_) > lastValue);
    // Found a suitable block, add it to pendingBlocks.
    pendingBlocks_.push_back(block);
    lastEntryInBlocks_ = CompressedRelationReader::getRelevantIdFromTriple(
        block.lastTriple_, metaBlocks_);
    metaBlocks_.removePrefix(1);
  }

  // Consume the next non-empty table from the generator and calculate the next
  // matching blocks from the index scan. This function guarantees that after
  // it returns, both `prefetchedValues` and `pendingBlocks` contain at least
  // one element.
  void fetch() {
    while (prefetchedValues_.empty() ||
           (pendingBlocks_.empty() && status_ == Status::Running)) {
      advanceInputToNextNonEmptyTable();
      if (status_ == Status::BothSidesExhausted) {
        return;
      }
      auto& idTable = iterator_.value()->idTable_;
      auto joinColumn = idTable.getColumn(joinColumn_);
      AD_EXPENSIVE_CHECK(ql::ranges::is_sorted(joinColumn));
      AD_CORRECTNESS_CHECK(!joinColumn.empty());
      // Skip processing for undef case, it will be handled differently
      if (hasUndef_) {
        return;
      }
      AD_CORRECTNESS_CHECK(!joinColumn[0].isUndefined());
      auto [newBlocks, numBlocksCompletelyHandled] =
          CompressedRelationReader::getBlocksForJoin(joinColumn, metaBlocks_);
      // The first `numBlocksCompletelyHandled` are either contained in
      // `newBlocks` or can never match any entry that is larger than the
      // entries in `joinColumn` and thus can be ignored from now on.
      metaBlocks_.removePrefix(numBlocksCompletelyHandled);

      const bool newBlockWasFound = !newBlocks.empty();
      const bool joinColumnMatchesPreviouslyYieldedBlock =
          joinColumn[0] <= lastEntryInBlocks_.value_or(Id::makeUndefined());

      bool joinSideMatches =
          newBlockWasFound || joinColumnMatchesPreviouslyYieldedBlock;
      bool joinSideBlockWillBeYielded = !filterJoinSide_ || joinSideMatches;
      bool scanSideExhausted = metaBlocks_.blockMetadata_.empty();

      if (newBlockWasFound) {
        lastEntryInBlocks_ = CompressedRelationReader::getRelevantIdFromTriple(
            newBlocks.back().lastTriple_, metaBlocks_);
        ql::ranges::move(newBlocks, std::back_inserter(pendingBlocks_));
      }
      if (joinSideBlockWillBeYielded) {
        prefetchedValues_.push_back(std::move(*iterator_.value()));
      }

      if (scanSideExhausted) {
        // The `!joinSideMatches` condition is important, because following
        // `joinColumns` from the join side might still match the last block
        // that we previously have yielded. Note: If we don't filter the join
        // side, then its remaining elements still have to be yielded, but this
        // is done by the generator for the join side.
        if (!joinSideMatches) {
          status_ = Status::BothSidesExhausted;
          return;
        }
        status_ = Status::ScanSideExhausted;
      }

      if (!newBlockWasFound && joinSideBlockWillBeYielded &&
          !scanSideExhausted) {
        pushDummyBlockIfBufferTooLarge();
      }
    }
  }

  // Check if there are any undefined values yielded by the original generator.
  // If the generator hasn't been started to get consumed, this will start it.
  bool hasUndef() {
    if (!iterator_.has_value()) {
      fetch();
    }
    return hasUndef_;
  }
};

// _____________________________________________________________________________
Result::LazyResult IndexScan::createPrefilteredJoinSide(
    std::shared_ptr<SharedGeneratorState> innerState) {
  using LoopControl = ad_utility::LoopControl<Result::IdTableVocabPair>;

  auto range = ad_utility::InputRangeFromLoopControlGet{
      [state = std::move(innerState)]() mutable {
        // Handle UNDEF case: pass through remaining input
        if (state->hasUndef()) {
          if (!state->iterator_.has_value()) {
            state->iterator_ = state->generator_.begin();
          }
          return LoopControl::breakWithYieldAll(ql::ranges::subrange(
              state->iterator_.value(), state->generator_.end()));
        }

        auto& prefetched = state->prefetchedValues_;

        if (prefetched.empty() &&
            state->status_ !=
                SharedGeneratorState::Status::BothSidesExhausted) {
          state->fetch();
        }

        if (prefetched.empty()) {
          AD_CORRECTNESS_CHECK(
              state->status_ ==
              SharedGeneratorState::Status::BothSidesExhausted);
          // If not filtering join side, yield all remaining elements.
          AD_CORRECTNESS_CHECK(state->iterator_.has_value());
          auto it = state->iterator_.value();
          if (!state->filterJoinSide_ && it != state->generator_.end()) {
            // Advance the iterator past the last value we already yielded.
            ++it;
            return LoopControl::breakWithYieldAll(
                ql::ranges::subrange(it, state->generator_.end()) |
                ql::views::filter(
                    [](const auto& block) { return !block.idTable_.empty(); }));
          } else {
            return LoopControl::makeBreak();
          }
        }

        // Make a defensive copy of the values to avoid modification during
        // iteration when yielding.
        auto copy = std::move(prefetched);
        prefetched.clear();

        // Yield all the newly found values
        return LoopControl::yieldAll(std::move(copy));
      }};
  return Result::LazyResult{std::move(range)};
}

// _____________________________________________________________________________
Result::LazyResult IndexScan::createPrefilteredIndexScanSide(
    std::shared_ptr<SharedGeneratorState> innerState) {
  using LoopControl = ad_utility::LoopControl<Result::IdTableVocabPair>;
  using namespace std::chrono_literals;
  using enum RuntimeInformation::SendPriority;

  auto range = ad_utility::InputRangeFromLoopControlGet{
      [this, state = std::move(innerState),
       metadata = LazyScanMetadata{}]() mutable {
        // Handle UNDEF case using LoopControl pattern
        if (state->hasUndef()) {
          auto scan = std::make_shared<
              CompressedRelationReader::IdTableGeneratorInputRange>(
              getLazyScan());
          scan->details().numBlocksAll_ =
              getMetadataForScan().value().sizeBlockMetadata_;
          updateRuntimeInfoForLazyScan(scan->details(), Always);
          return LoopControl::breakWithYieldAll(
              ad_utility::CachingTransformInputRange(
                  *scan, [this, scan](auto& table) mutable {
                    updateRuntimeInfoForLazyScan(scan->details(), IfDue);
                    return Result::IdTableVocabPair{std::move(table),
                                                    LocalVocab{}};
                  }));
        }

        auto& pendingBlocks = state->pendingBlocks_;

        while (pendingBlocks.empty()) {
          // This generator also breaks when only the scan side is exhausted,
          // as it represents the scan side!
          if (state->status_ != SharedGeneratorState::Status::Running) {
            metadata.numBlocksAll_ = state->metaBlocks_.sizeBlockMetadata_;
            updateRuntimeInfoForLazyScan(metadata, Always);
            return LoopControl::makeBreak();
          }
          state->fetch();
        }
        metadata.numBlocksAll_ = state->metaBlocks_.sizeBlockMetadata_;
        updateRuntimeInfoForLazyScan(metadata, IfDue);

        // We now have non-empty pending blocks
        auto scan = getLazyScan(std::move(pendingBlocks));
        AD_CORRECTNESS_CHECK(pendingBlocks.empty());

        // Capture scan details by reference so we get the updated values
        const auto& scanDetails = scan.details();

        // Transform the scan to Result::IdTableVocabPair and yield all
        auto transformedScan = ad_utility::CachingTransformInputRange(
            std::move(scan),
            [&metadata, &scanDetails,
             originalMetadata = metadata](auto& table) mutable {
              // Make sure we don't add everything more than once.
              metadata = originalMetadata;
              metadata.aggregate(scanDetails);
              return Result::IdTableVocabPair{std::move(table), LocalVocab{}};
            });

        return LoopControl::yieldAll(std::move(transformedScan));
      }};
  return Result::LazyResult{std::move(range)};
}

// _____________________________________________________________________________
std::pair<Result::LazyResult, Result::LazyResult> IndexScan::prefilterTables(
    Result::LazyResult input, ColumnIndex joinColumn, bool filterJoinSide) {
  AD_CORRECTNESS_CHECK(numVariables_ <= 3 && numVariables_ > 0);
  auto metaBlocks = getMetadataForScan();

  if (!metaBlocks.has_value()) {
    // Return empty results
    return {filterJoinSide ? Result::LazyResult{} : std::move(input),
            Result::LazyResult{}};
  }

  auto state = std::make_shared<SharedGeneratorState>(
      SharedGeneratorState{std::move(input), joinColumn,
                           std::move(metaBlocks.value()), filterJoinSide});
  return {createPrefilteredJoinSide(state),
          createPrefilteredIndexScanSide(state)};
}

// _____________________________________________________________________________
std::unique_ptr<Operation> IndexScan::cloneImpl() const {
  return std::make_unique<IndexScan>(
      _executionContext, permutation_, locatedTriplesSharedState_, subject_,
      predicate_, object_, additionalColumns_, additionalVariables_,
      graphsToFilter_, scanSpecAndBlocks_, scanSpecAndBlocksIsPrefiltered_,
      varsToKeep_, sizeEstimateIsExact_, sizeEstimate_);
}

// _____________________________________________________________________________
bool IndexScan::columnOriginatesFromGraphOrUndef(
    const Variable& variable) const {
  AD_CONTRACT_CHECK(getExternallyVisibleVariableColumns().contains(variable));
  return variable == subject_ || variable == predicate_ || variable == object_;
}

// _____________________________________________________________________________
std::optional<std::shared_ptr<QueryExecutionTree>>
IndexScan::makeTreeWithStrippedColumns(
    const std::set<Variable>& variables) const {
  ad_utility::HashSet<Variable> newVariables;
  for (const auto& [var, _] : getExternallyVisibleVariableColumns()) {
    if (ad_utility::contains(variables, var)) {
      newVariables.insert(var);
    }
  }

  return ad_utility::makeExecutionTree<IndexScan>(
      _executionContext, permutation_, locatedTriplesSharedState_, subject_,
      predicate_, object_, additionalColumns_, additionalVariables_,
      graphsToFilter_, scanSpecAndBlocks_, scanSpecAndBlocksIsPrefiltered_,
      VarsToKeep{std::move(newVariables)}, sizeEstimateIsExact_, sizeEstimate_);
}

// _____________________________________________________________________________
std::vector<ColumnIndex> IndexScan::getSubsetForStrippedColumns() const {
  AD_CORRECTNESS_CHECK(varsToKeep_.has_value());
  const auto& v = varsToKeep_.value();
  std::vector<ColumnIndex> result;
  size_t idx = 0;
  for (const auto& el : getPermutedTriple()) {
    if (el->isVariable()) {
      if (v.contains(el->getVariable())) {
        result.push_back(idx);
      }
      ++idx;
    }
  }
  for (const auto& var : additionalVariables_) {
    if (v.contains(var)) {
      result.push_back(idx);
    }
    ++idx;
  }
  return result;
}
