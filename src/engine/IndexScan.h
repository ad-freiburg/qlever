// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <string>

#include "./Operation.h"
#include "util/HashMap.h"

class SparqlTriple;
class SparqlTripleSimple;

class IndexScan final : public Operation {
  using Graphs = ScanSpecificationAsTripleComponent::Graphs;
  // Optional pair containing a `PrefilterExpression` with `ColumnIndex` (eval.
  // index)
  using PrefilterIndexPair = std::optional<std::pair<
      std::unique_ptr<prefilterExpressions::PrefilterExpression>, ColumnIndex>>;

 private:
  Permutation::Enum permutation_;
  TripleComponent subject_;
  TripleComponent predicate_;
  TripleComponent object_;
  Graphs graphsToFilter_;
  PrefilterIndexPair prefilter_;
  size_t numVariables_;
  size_t sizeEstimate_;
  bool sizeEstimateIsExact_;
  vector<float> multiplicity_;

  // Additional columns (e.g. patterns) that are being retrieved in addition to
  // the "ordinary" subjects, predicates, or objects, as well as the variables
  // that they are bound to.
  std::vector<ColumnIndex> additionalColumns_;
  std::vector<Variable> additionalVariables_;

 public:
  IndexScan(QueryExecutionContext* qec, Permutation::Enum permutation,
            const SparqlTriple& triple, Graphs graphsToFilter = std::nullopt,
            PrefilterIndexPair prefilter = std::nullopt);
  IndexScan(QueryExecutionContext* qec, Permutation::Enum permutation,
            const SparqlTripleSimple& triple,
            Graphs graphsToFilter = std::nullopt,
            PrefilterIndexPair prefilter = std::nullopt);
  // Constructor to simplify copy creation of an `IndexScan`.
  IndexScan(QueryExecutionContext* qec, Permutation::Enum permutation,
            const TripleComponent& s, const TripleComponent& p,
            const TripleComponent& o,
            std::vector<ColumnIndex> additionalColumns,
            std::vector<Variable> additionalVariables, Graphs graphsToFilter,
            PrefilterIndexPair prefilter);

  ~IndexScan() override = default;

  // Const getters for testing.
  const TripleComponent& predicate() const { return predicate_; }
  const TripleComponent& subject() const { return subject_; }
  const TripleComponent& object() const { return object_; }
  const auto& graphsToFilter() const { return graphsToFilter_; }
  const std::vector<Variable>& additionalVariables() const {
    return additionalVariables_;
  }

  const std::vector<ColumnIndex>& additionalColumns() const {
    return additionalColumns_;
  }
  std::string getDescriptor() const override;

  size_t getResultWidth() const override;

  vector<ColumnIndex> resultSortedOn() const override;

  // Set `PrefilterExpression`s and return updated `QueryExecutionTree` pointer
  // if necessary.
  std::optional<std::shared_ptr<QueryExecutionTree>>
  setPrefilterExprGetUpdatedQetPtr(
      std::vector<PrefilterVariablePair> prefilterVariablePairs) override;

  size_t numVariables() const { return numVariables_; }

  // Return the exact result size of the index scan. This is always known as it
  // can be read from the Metadata.
  size_t getExactSize() const;

  // Return two generators that lazily yield the results of `s1` and `s2` in
  // blocks, but only the blocks that can theoretically contain matching rows
  // when performing a join on the first column of the result of `s1` with the
  // first column of the result of `s2`.
  static std::array<Permutation::IdTableGenerator, 2> lazyScanForJoinOfTwoScans(
      const IndexScan& s1, const IndexScan& s2);

  // Return a generator that lazily yields the result in blocks, but only
  // the blocks that can theoretically contain matching rows when performing a
  // join between the first column of the result with the `joinColumn`.
  // Requires that the `joinColumn` is sorted, else the behavior is undefined.
  Permutation::IdTableGenerator lazyScanForJoinOfColumnWithScan(
      std::span<const Id> joinColumn) const;

 private:
  // TODO<joka921> Make the `getSizeEstimateBeforeLimit()` function `const` for
  // ALL the `Operations`.
  uint64_t getSizeEstimateBeforeLimit() override { return sizeEstimate_; }

 public:
  size_t getCostEstimate() override;

  void determineMultiplicities();

  float getMultiplicity(size_t col) override {
    if (multiplicity_.empty()) {
      determineMultiplicities();
    }
    AD_CORRECTNESS_CHECK(col < multiplicity_.size());
    return multiplicity_[col];
  }

  bool knownEmptyResult() override {
    return sizeEstimateIsExact_ && sizeEstimate_ == 0;
  }

  bool isIndexScanWithNumVariables(size_t target) const override {
    return numVariables() == target;
  }

  // Full index scans will never be able to fit in the cache on datasets the
  // size of wikidata, so we don't even need to try and waste performance.
  bool unlikelyToFitInCache(
      ad_utility::MemorySize maxCacheableSize) const override {
    return ad_utility::MemorySize::bytes(sizeEstimate_ * getResultWidth() *
                                         sizeof(Id)) > maxCacheableSize;
  }

  // An index scan can directly and efficiently support LIMIT and OFFSET
  [[nodiscard]] bool supportsLimit() const override { return true; }

  Permutation::Enum permutation() const { return permutation_; }

  // Return the stored triple in the order that corresponds to the
  // `permutation_`. For example if `permutation_ == PSO` then the result is
  // {&predicate_, &subject_, &object_}
  std::array<const TripleComponent* const, 3> getPermutedTriple() const;
  ScanSpecification getScanSpecification() const;
  ScanSpecificationAsTripleComponent getScanSpecificationTc() const;

 private:
  ProtoResult computeResult(bool requestLaziness) override;

  vector<QueryExecutionTree*> getChildren() override { return {}; }

  // Compute the size estimate of the index scan, taking delta triples (from
  // the `queryExecutionContext_`) into account. The `bool` is true iff the
  // estimate is exact. If not, the estimate is the mean of the lower and upper
  // bound.
  std::pair<bool, size_t> computeSizeEstimate() const;

  std::string getCacheKeyImpl() const override;

  VariableToColumnMap computeVariableToColumnMap() const override;

  std::shared_ptr<QueryExecutionTree> makeCopyWithAddedPrefilters(
      PrefilterIndexPair prefilter) const;

  // Return the (lazy) `IdTable` for this `IndexScan` in chunks.
  Result::Generator chunkedIndexScan() const;
  // Get the `IdTable` for this `IndexScan` in one piece.
  IdTable materializedIndexScan() const;

  // Get the mapping for all `Variable` values mapped to sorted `ColumnIndex`
  // values.
  ad_utility::HashMap<Variable, ColumnIndex> getVariableToSortedIndexMap()
      const;

  // Helper to retrieve the `CompressedBlockMetadata` span for this scan.
  std::optional<std::span<const CompressedBlockMetadata>> getOptionalBlockSpan()
      const;

  // If `isUnconstrained()` yields true, return the blocks as given or the
  // prefiltered blocks (if `prefilter_` has value). If `isUnconstrained()` is
  // false, return `std::nullopt`.
  std::optional<std::vector<CompressedBlockMetadata>>
  getOptionalPrefilteredBlocks(
      std::vector<CompressedBlockMetadata> blocks) const;

  // Helper functions for the public `getLazyScanFor...` methods and
  // `chunkedIndexScan` (see above).
  Permutation::IdTableGenerator getLazyScan(
      std::vector<CompressedBlockMetadata> blocks) const;
  std::optional<Permutation::MetadataAndBlocks> getMetadataForScan() const;
};
