// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "engine/IndexScan.h"

#include <absl/strings/str_join.h>

#include <boost/optional.hpp>
#include <sstream>
#include <string>

#include "index/IndexImpl.h"
#include "parser/ParsedQuery.h"

using std::string;

// _____________________________________________________________________________
// Return the number of `Variables` given the `TripleComponent` values for
// `subject_`, `prediate` and `object`.
static size_t getNumberOfVariables(const TripleComponent& subject,
                                   const TripleComponent& predicate,
                                   const TripleComponent& object) {
  return static_cast<size_t>(subject.isVariable()) +
         static_cast<size_t>(predicate.isVariable()) +
         static_cast<size_t>(object.isVariable());
}

// _____________________________________________________________________________
IndexScan::IndexScan(QueryExecutionContext* qec, Permutation::Enum permutation,
                     const SparqlTripleSimple& triple, Graphs graphsToFilter,
                     PrefilterIndexPair prefilter)
    : Operation(qec),
      permutation_(permutation),
      subject_(triple.s_),
      predicate_(triple.p_),
      object_(triple.o_),
      graphsToFilter_{std::move(graphsToFilter)},
      prefilter_{std::move(prefilter)},
      numVariables_(getNumberOfVariables(subject_, predicate_, object_)) {
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
IndexScan::IndexScan(QueryExecutionContext* qec, Permutation::Enum permutation,
                     const SparqlTriple& triple, Graphs graphsToFilter,
                     PrefilterIndexPair prefilter)
    : IndexScan(qec, permutation, triple.getSimple(), std::move(graphsToFilter),
                std::move(prefilter)) {}

// _____________________________________________________________________________
IndexScan::IndexScan(QueryExecutionContext* qec, Permutation::Enum permutation,
                     const TripleComponent& s, const TripleComponent& p,
                     const TripleComponent& o,
                     std::vector<ColumnIndex> additionalColumns,
                     std::vector<Variable> additionalVariables,
                     Graphs graphsToFilter, PrefilterIndexPair prefilter)
    : Operation(qec),
      permutation_(permutation),
      subject_(s),
      predicate_(p),
      object_(o),
      graphsToFilter_(std::move(graphsToFilter)),
      prefilter_(std::move(prefilter)),
      numVariables_(getNumberOfVariables(subject_, predicate_, object_)),
      additionalColumns_(std::move(additionalColumns)),
      additionalVariables_(std::move(additionalVariables)) {
  std::tie(sizeEstimateIsExact_, sizeEstimate_) = computeSizeEstimate();
  determineMultiplicities();
}

// _____________________________________________________________________________
string IndexScan::getCacheKeyImpl() const {
  std::ostringstream os;
  auto permutationString = Permutation::toString(permutation_);

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
  if (!additionalColumns_.empty()) {
    os << " Additional Columns: ";
    os << absl::StrJoin(additionalColumns(), " ");
  }
  if (graphsToFilter_.has_value()) {
    // The graphs are stored as a hash set, but we need a deterministic order.
    std::vector<std::string> graphIdVec;
    std::ranges::transform(graphsToFilter_.value(),
                           std::back_inserter(graphIdVec),
                           &TripleComponent::toRdfLiteral);
    std::ranges::sort(graphIdVec);
    os << "\nFiltered by Graphs:";
    os << absl::StrJoin(graphIdVec, " ");
  }
  if (prefilter_.has_value()) {
    auto& [prefilterExpr, columnIdx] = prefilter_.value();
    os << "Added PrefiterExpression: \n";
    os << *prefilterExpr;
    os << "\nApplied on column: " << columnIdx << ".";
  }
  return std::move(os).str();
}

// _____________________________________________________________________________
string IndexScan::getDescriptor() const {
  return "IndexScan " + subject_.toString() + " " + predicate_.toString() +
         " " + object_.toString();
}

// _____________________________________________________________________________
size_t IndexScan::getResultWidth() const {
  return numVariables_ + additionalVariables_.size();
}

// _____________________________________________________________________________
vector<ColumnIndex> IndexScan::resultSortedOn() const {
  auto resAsView = ad_utility::integerRange(ColumnIndex{numVariables_});
  std::vector<ColumnIndex> result{resAsView.begin(), resAsView.end()};
  for (size_t i = 0; i < additionalColumns_.size(); ++i) {
    if (additionalColumns_.at(i) == ADDITIONAL_COLUMN_GRAPH_ID) {
      result.push_back(numVariables_ + i);
    }
  }
  return result;
}

// _____________________________________________________________________________
std::optional<std::shared_ptr<QueryExecutionTree>>
IndexScan::setPrefilterGetUpdatedQueryExecutionTree(
    const std::vector<PrefilterVariablePair>& prefilterVariablePairs) const {
  auto optSortedVarColIdxPair =
      getSortedVariableAndMetadataColumnIndexForPrefiltering();
  if (!optSortedVarColIdxPair.has_value()) {
    return std::nullopt;
  }
  const auto& [sortedVar, colIdx] = optSortedVarColIdxPair.value();
  auto it =
      std::ranges::find(prefilterVariablePairs, sortedVar, ad_utility::second);
  if (it != prefilterVariablePairs.end()) {
    return makeCopyWithAddedPrefilters(
        std::make_pair(it->first->clone(), colIdx));
  }
  return std::nullopt;
}

// _____________________________________________________________________________
VariableToColumnMap IndexScan::computeVariableToColumnMap() const {
  VariableToColumnMap variableToColumnMap;
  auto addCol = [&variableToColumnMap,
                 nextColIdx = ColumnIndex{0}](const Variable& var) mutable {
    // All the columns of an index scan only contain defined values.
    variableToColumnMap[var] = makeAlwaysDefinedColumn(nextColIdx);
    ++nextColIdx;
  };

  for (const TripleComponent* const ptr : getPermutedTriple()) {
    if (ptr->isVariable()) {
      addCol(ptr->getVariable());
    }
  }
  std::ranges::for_each(additionalVariables_, addCol);
  return variableToColumnMap;
}

//______________________________________________________________________________
std::shared_ptr<QueryExecutionTree> IndexScan::makeCopyWithAddedPrefilters(
    PrefilterIndexPair prefilter) const {
  return ad_utility::makeExecutionTree<IndexScan>(
      getExecutionContext(), permutation_, subject_, predicate_, object_,
      additionalColumns_, additionalVariables_, graphsToFilter_,
      std::move(prefilter));
}

// _____________________________________________________________________________
Result::Generator IndexScan::chunkedIndexScan() const {
  auto optBlockSpan = getBlockMetadata();
  if (!optBlockSpan.has_value()) {
    co_return;
  }
  const auto& blockSpan = optBlockSpan.value();
  // Note: Given a `PrefilterIndexPair` is available, the corresponding
  // prefiltering will be applied in `getLazyScan`.
  for (IdTable& idTable : getLazyScan({blockSpan.begin(), blockSpan.end()})) {
    co_yield {std::move(idTable), LocalVocab{}};
  }
}

// _____________________________________________________________________________
IdTable IndexScan::materializedIndexScan() const {
  IdTable idTable = getScanPermutation().scan(
      getScanSpecification(), additionalColumns(), cancellationHandle_,
      locatedTriplesSnapshot(), getLimit(),
      getBlockMetadataOptionallyPrefiltered());
  AD_CORRECTNESS_CHECK(idTable.numColumns() == getResultWidth());
  LOG(DEBUG) << "IndexScan result computation done.\n";
  checkCancellation();
  return idTable;
}

// _____________________________________________________________________________
ProtoResult IndexScan::computeResult(bool requestLaziness) {
  LOG(DEBUG) << "IndexScan result computation...\n";
  if (requestLaziness) {
    return {chunkedIndexScan(), resultSortedOn()};
  }
  return {materializedIndexScan(), getResultSortedOn(), LocalVocab{}};
}

// _____________________________________________________________________________
const Permutation& IndexScan::getScanPermutation() const {
  return getIndex().getImpl().getPermutation(permutation_);
}

// _____________________________________________________________________________
std::pair<bool, size_t> IndexScan::computeSizeEstimate() const {
  AD_CORRECTNESS_CHECK(_executionContext);
  auto [lower, upper] = getScanPermutation().getSizeEstimateForScan(
      getScanSpecification(), locatedTriplesSnapshot(),
      getBlockMetadataOptionallyPrefiltered());
  return {lower == upper, std::midpoint(lower, upper)};
}

// _____________________________________________________________________________
size_t IndexScan::getExactSize() const {
  AD_CORRECTNESS_CHECK(_executionContext);
  return getScanPermutation().getResultSizeOfScan(
      getScanSpecification(), locatedTriplesSnapshot(),
      getBlockMetadataOptionallyPrefiltered());
}

// _____________________________________________________________________________
size_t IndexScan::getCostEstimate() {
  // If we have a limit present, we only have to read the first
  // `limit + offset` elements.
  return getLimit().upperBound(getSizeEstimateBeforeLimit());
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
      return idx.getMultiplicities(*getPermutedTriple()[0], permutation_,
                                   locatedTriplesSnapshot());
    } else {
      AD_CORRECTNESS_CHECK(numVariables_ == 3);
      return idx.getMultiplicities(permutation_);
    }
  }();
  for ([[maybe_unused]] size_t i :
       std::views::iota(multiplicity_.size(), getResultWidth())) {
    multiplicity_.emplace_back(1);
  }
  AD_CONTRACT_CHECK(multiplicity_.size() == getResultWidth());
}

// _____________________________________________________________________________
std::array<const TripleComponent* const, 3> IndexScan::getPermutedTriple()
    const {
  std::array triple{&subject_, &predicate_, &object_};
  auto permutation = Permutation::toKeyOrder(permutation_);
  return {triple[permutation[0]], triple[permutation[1]],
          triple[permutation[2]]};
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

// _____________________________________________________________________________
std::optional<std::span<const CompressedBlockMetadata>>
IndexScan::getBlockMetadata() const {
  auto metadata = getMetadataForScan();
  if (metadata.has_value()) {
    return CompressedRelationReader::getBlocksFromMetadata(metadata.value());
  }
  return std::nullopt;
}

// _____________________________________________________________________________
std::optional<std::vector<CompressedBlockMetadata>>
IndexScan::getBlockMetadataOptionallyPrefiltered() const {
  auto optBlockSpan = getBlockMetadata();
  std::optional<std::vector<CompressedBlockMetadata>> optBlocks = std::nullopt;
  if (optBlockSpan.has_value()) {
    const auto& blockSpan = optBlockSpan.value();
    optBlocks = {blockSpan.begin(), blockSpan.end()};
    applyPrefilterIfPossible(optBlocks.value());
  }
  return optBlocks;
}

// _____________________________________________________________________________
void IndexScan::applyPrefilterIfPossible(
    std::vector<CompressedBlockMetadata>& blocks) const {
  if (prefilter_.has_value()) {
    // Apply the prefilter on given blocks.
    auto& [prefilterExpr, columnIndex] = prefilter_.value();
    blocks = prefilterExpr->evaluate(blocks, columnIndex);
  }
}

// _____________________________________________________________________________
Permutation::IdTableGenerator IndexScan::getLazyScan(
    std::vector<CompressedBlockMetadata> blocks) const {
  // If there is a LIMIT or OFFSET clause that constrains the scan
  // (which can happen with an explicit subquery), we cannot use the prefiltered
  // blocks, as we currently have no mechanism to include limits and offsets
  // into the prefiltering (`std::nullopt` means `scan all blocks`).
  auto filteredBlocks = getLimit().isUnconstrained()
                            ? std::optional(std::move(blocks))
                            : std::nullopt;
  if (filteredBlocks.has_value()) {
    // Note: The prefilter expression applied with applyPrefilterIfPossible()
    // is not related to the prefilter procedure mentioned in the comment above.
    // If this IndexScan owns a <PrefilterExpression, ColumnIdx> pair, it can
    // be applied.
    applyPrefilterIfPossible(filteredBlocks.value());
  }
  return getScanPermutation().lazyScan(getScanSpecification(), filteredBlocks,
                                       additionalColumns(), cancellationHandle_,
                                       locatedTriplesSnapshot(), getLimit());
};

// _____________________________________________________________________________
std::optional<Permutation::MetadataAndBlocks> IndexScan::getMetadataForScan()
    const {
  return getScanPermutation().getMetadataAndBlocks(getScanSpecification(),
                                                   locatedTriplesSnapshot());
};

// _____________________________________________________________________________
std::array<Permutation::IdTableGenerator, 2>
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
  result[0].details().numBlocksAll_ = metaBlocks1.value().blockMetadata_.size();
  result[1].details().numBlocksAll_ = metaBlocks2.value().blockMetadata_.size();
  return result;
}

// _____________________________________________________________________________
Permutation::IdTableGenerator IndexScan::lazyScanForJoinOfColumnWithScan(
    std::span<const Id> joinColumn) const {
  AD_EXPENSIVE_CHECK(std::ranges::is_sorted(joinColumn));
  AD_CORRECTNESS_CHECK(numVariables_ <= 3 && numVariables_ > 0);
  AD_CONTRACT_CHECK(joinColumn.empty() || !joinColumn[0].isUndefined());

  auto metaBlocks1 = getMetadataForScan();

  if (!metaBlocks1.has_value()) {
    return {};
  }
  auto blocks = CompressedRelationReader::getBlocksForJoin(joinColumn,
                                                           metaBlocks1.value());

  auto result = getLazyScan(blocks);
  result.details().numBlocksAll_ = metaBlocks1.value().blockMetadata_.size();
  return result;
}
