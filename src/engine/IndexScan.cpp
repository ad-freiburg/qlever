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
IndexScan::IndexScan(QueryExecutionContext* qec, Permutation::Enum permutation,
                     const SparqlTripleSimple& triple, Graphs graphsToFilter)
    : Operation(qec),
      permutation_(permutation),
      subject_(triple.s_),
      predicate_(triple.p_),
      object_(triple.o_),
      graphsToFilter_{std::move(graphsToFilter)},
      numVariables_(static_cast<size_t>(subject_.isVariable()) +
                    static_cast<size_t>(predicate_.isVariable()) +
                    static_cast<size_t>(object_.isVariable())) {
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
                     const SparqlTriple& triple, Graphs graphsToFilter)
    : IndexScan(qec, permutation, triple.getSimple(),
                std::move(graphsToFilter)) {}

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

// _____________________________________________________________________________
Result::Generator IndexScan::scanInChunks() const {
  auto metadata = getMetadataForScan();
  if (!metadata.has_value()) {
    co_return;
  }
  auto blocksSpan =
      CompressedRelationReader::getBlocksFromMetadata(metadata.value());
  std::vector<CompressedBlockMetadata> blocks{blocksSpan.begin(),
                                              blocksSpan.end()};
  for (IdTable& idTable : getLazyScan(std::move(blocks))) {
    co_yield {std::move(idTable), LocalVocab{}};
  }
}

// _____________________________________________________________________________
ProtoResult IndexScan::computeResult(bool requestLaziness) {
  LOG(DEBUG) << "IndexScan result computation...\n";
  if (requestLaziness) {
    return {scanInChunks(), resultSortedOn()};
  }
  IdTable idTable{getExecutionContext()->getAllocator()};

  using enum Permutation::Enum;
  idTable.setNumColumns(numVariables_);
  const auto& index = _executionContext->getIndex();
  idTable =
      index.scan(getScanSpecification(), permutation_, additionalColumns(),
                 cancellationHandle_, locatedTriplesSnapshot(), getLimit());
  AD_CORRECTNESS_CHECK(idTable.numColumns() == getResultWidth());
  LOG(DEBUG) << "IndexScan result computation done.\n";
  checkCancellation();

  return {std::move(idTable), resultSortedOn(), LocalVocab{}};
}

// _____________________________________________________________________________
std::pair<bool, size_t> IndexScan::computeSizeEstimate() const {
  AD_CORRECTNESS_CHECK(_executionContext);
  auto [lower, upper] = getIndex()
                            .getImpl()
                            .getPermutation(permutation())
                            .getSizeEstimateForScan(getScanSpecification(),
                                                    locatedTriplesSnapshot());
  return {lower == upper, std::midpoint(lower, upper)};
}

// _____________________________________________________________________________
size_t IndexScan::getExactSize() const {
  AD_CORRECTNESS_CHECK(_executionContext);
  return getIndex().getResultSizeOfScan(getScanSpecification(), permutation_,
                                        locatedTriplesSnapshot());
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

// ___________________________________________________________________________
std::array<const TripleComponent* const, 3> IndexScan::getPermutedTriple()
    const {
  std::array triple{&subject_, &predicate_, &object_};
  auto permutation = Permutation::toKeyOrder(permutation_);
  return {triple[permutation[0]], triple[permutation[1]],
          triple[permutation[2]]};
}

// ___________________________________________________________________________
ScanSpecification IndexScan::getScanSpecification() const {
  const IndexImpl& index = getIndex().getImpl();
  return getScanSpecificationTc().toScanSpecification(index);
}

// ___________________________________________________________________________
ScanSpecificationAsTripleComponent IndexScan::getScanSpecificationTc() const {
  auto permutedTriple = getPermutedTriple();
  return {*permutedTriple[0], *permutedTriple[1], *permutedTriple[2],
          graphsToFilter_};
}

// ___________________________________________________________________________
Permutation::IdTableGenerator IndexScan::getLazyScan(
    std::vector<CompressedBlockMetadata> blocks) const {
  // If there is a LIMIT or OFFSET clause that constrains the scan
  // (which can happen with an explicit subquery), we cannot use the prefiltered
  // blocks, as we currently have no mechanism to include limits and offsets
  // into the prefiltering (`std::nullopt` means `scan all blocks`).
  auto actualBlocks = getLimit().isUnconstrained()
                          ? std::optional{std::move(blocks)}
                          : std::nullopt;

  return getIndex()
      .getImpl()
      .getPermutation(permutation())
      .lazyScan(getScanSpecification(), std::move(actualBlocks),
                additionalColumns(), cancellationHandle_,
                locatedTriplesSnapshot(), getLimit());
};

// ________________________________________________________________
std::optional<Permutation::MetadataAndBlocks> IndexScan::getMetadataForScan()
    const {
  const auto& index = getExecutionContext()->getIndex().getImpl();
  return index.getPermutation(permutation())
      .getMetadataAndBlocks(getScanSpecification(), locatedTriplesSnapshot());
};

// ________________________________________________________________
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

// ________________________________________________________________
Permutation::IdTableGenerator IndexScan::lazyScanForJoinOfColumnWithScan(
    std::span<const Id> joinColumn) const {
  AD_EXPENSIVE_CHECK(std::ranges::is_sorted(joinColumn));
  AD_CORRECTNESS_CHECK(numVariables_ <= 3 && numVariables_ > 0);
  AD_CONTRACT_CHECK(joinColumn.empty() || !joinColumn[0].isUndefined());

  auto metaBlocks = getMetadataForScan();

  if (!metaBlocks.has_value()) {
    return {};
  }
  auto blocks = CompressedRelationReader::getBlocksForJoin(joinColumn,
                                                           metaBlocks.value());

  auto result = getLazyScan(blocks);
  result.details().numBlocksAll_ = metaBlocks.value().blockMetadata_.size();
  return result;
}

struct SharedGeneratorState {
  Result::Generator generator_;
  ColumnIndex joinColumn_;
  Permutation::MetadataAndBlocks metaBlocks_;
  std::optional<Result::Generator::iterator> iterator_ = std::nullopt;
  std::optional<Id> lastValue_ = std::nullopt;
  std::deque<Result::IdTableVocabPair> prefetchedValues_{};
  std::vector<CompressedBlockMetadata> pendingBlocks_{};
  bool hasUndef_ = false;
  bool doneFetching_ = false;

  bool fetchFirst() {
    AD_CORRECTNESS_CHECK(!iterator_.has_value());
    iterator_ = generator_.begin();
    while (iterator_ != generator_.end()) {
      if (iterator_.value()->idTable_.empty()) {
        ++iterator_.value();
        continue;
      }
      hasUndef_ = iterator_.value()->idTable_.at(0, joinColumn_).isUndefined();
      return false;
    }
    return true;
  }

  void fetch() {
    bool exhausted;
    if (iterator_.has_value()) {
      AD_CONTRACT_CHECK(iterator_.value() != generator_.end());
      do {
        // Skip empty tables
        ++iterator_.value();
        exhausted = iterator_.value() == generator_.end();
      } while (!exhausted && iterator_.value()->idTable_.empty());
    } else {
      exhausted = fetchFirst();
    }
    if (!exhausted) {
      prefetchedValues_.push_back(std::move(*iterator_.value()));
      auto joinColumn =
          prefetchedValues_.back().idTable_.getColumn(joinColumn_);
      AD_EXPENSIVE_CHECK(std::ranges::is_sorted(joinColumn));
      // Skip processing for undef case, it will be handled differently
      if (joinColumn.empty() || !joinColumn[0].isUndefined()) {
        size_t offset = std::ranges::find_if_not(joinColumn,
                                                 [this](const auto& element) {
                                                   return element == lastValue_;
                                                 }) -
                        joinColumn.begin();
        auto newValues = joinColumn.subspan(offset);
        if (!newValues.empty()) {
          std::ranges::move(CompressedRelationReader::getBlocksForJoin(
                                newValues, metaBlocks_),
                            std::back_inserter(pendingBlocks_));
          lastValue_ = newValues.back();
        }
      }
    }
    doneFetching_ = exhausted;
  }

  bool hasUndef() {
    if (!iterator_.has_value() && !doneFetching_) {
      fetch();
    }
    return hasUndef_;
  }
};

// Set the runtime info of the `scanTree` when it was lazily executed during a
// join.
void IndexScan::updateRuntimeInfoForLazyScan(
    const CompressedRelationReader::LazyScanMetadata& metadata) {
  updateRuntimeInformationWhenOptimizedOut(
      RuntimeInformation::Status::lazilyMaterialized);
  auto& rti = runtimeInfo();
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
}

// _____________________________________________________________________________
void aggregateMetadata(
    CompressedRelationReader::LazyScanMetadata& aggregate,
    const CompressedRelationReader::LazyScanMetadata& newValue) {
  aggregate.numElementsYielded_ += newValue.numElementsYielded_;
  aggregate.blockingTime_ += newValue.blockingTime_;
  aggregate.numBlocksRead_ += newValue.numBlocksRead_;
  aggregate.numBlocksAll_ += newValue.numBlocksAll_;
  aggregate.numElementsRead_ += newValue.numElementsRead_;
  aggregate.numBlocksSkippedBecauseOfGraph_ +=
      newValue.numBlocksSkippedBecauseOfGraph_;
  aggregate.numBlocksPostprocessed_ += newValue.numBlocksPostprocessed_;
  aggregate.numBlocksWithUpdate_ += newValue.numBlocksWithUpdate_;
}

// _____________________________________________________________________________
std::pair<Result::Generator, Result::Generator> IndexScan::prefilterTables(
    Result::Generator input, ColumnIndex joinColumn) {
  AD_CORRECTNESS_CHECK(numVariables_ <= 3 && numVariables_ > 0);
  auto metaBlocks = getMetadataForScan();

  if (!metaBlocks.has_value()) {
    return {Result::Generator{}, Result::Generator{}};
  }
  std::shared_ptr<SharedGeneratorState> state =
      std::make_shared<SharedGeneratorState>(std::move(input), joinColumn,
                                             std::move(metaBlocks.value()));
  auto joinSide = [](auto innerState) -> Result::Generator {
    if (innerState->hasUndef()) {
      for (Result::IdTableVocabPair& pair : innerState->prefetchedValues_) {
        co_yield pair;
      }
      innerState->prefetchedValues_.clear();
      if (innerState->iterator_.value() != innerState->generator_.end()) {
        ++innerState->iterator_.value();
      }
      while (innerState->iterator_.value() != innerState->generator_.end()) {
        co_yield *innerState->iterator_.value();
        ++innerState->iterator_.value();
      }
    } else {
      while (true) {
        if (innerState->prefetchedValues_.empty()) {
          if (innerState->doneFetching_) {
            co_return;
          }
          innerState->fetch();
        }
        while (!innerState->prefetchedValues_.empty()) {
          co_yield innerState->prefetchedValues_.front();
          innerState->prefetchedValues_.pop_front();
        }
      }
    }
  };
  auto indexScan = [](auto* self, auto innerState) -> Result::Generator {
    if (innerState->hasUndef()) {
      for (auto& pair : self->scanInChunks()) {
        co_yield pair;
      }
    } else {
      CompressedRelationReader::LazyScanMetadata metadata;
      while (true) {
        if (innerState->pendingBlocks_.empty()) {
          if (innerState->doneFetching_) {
            metadata.numBlocksAll_ =
                innerState->metaBlocks_.blockMetadata_.size();
            self->updateRuntimeInfoForLazyScan(metadata);
            co_return;
          }
          innerState->fetch();
        }
        auto blocks = std::move(innerState->pendingBlocks_);
        auto scan = self->getLazyScan(std::move(blocks));
        for (IdTable& idTable : scan) {
          co_yield {std::move(idTable), LocalVocab{}};
        }
        aggregateMetadata(metadata, scan.details());
      }
    }
  };
  return {joinSide(state), indexScan(this, state)};
}
