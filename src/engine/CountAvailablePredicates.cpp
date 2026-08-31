// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@neptun.uni-freiburg.de)

#include "engine/CountAvailablePredicates.h"

#include "backports/algorithm.h"
#include "engine/CallFixedSize.h"
#include "engine/IndexScan.h"
#include "global/Pattern.h"
#include "global/RuntimeParameters.h"
#include "util/ParallelExecutor.h"

// _____________________________________________________________________________
CountAvailablePredicates::CountAvailablePredicates(
    QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> subtree,
    size_t subjectColumnIndex, Variable predicateVariable,
    Variable countVariable)
    : Operation(qec),
      subtree_(QueryExecutionTree::createSortedTree(std::move(subtree),
                                                    {subjectColumnIndex})),
      subjectColumnIndex_(subjectColumnIndex),
      predicateVariable_(std::move(predicateVariable)),
      countVariable_(std::move(countVariable)) {}

// _____________________________________________________________________________
std::string CountAvailablePredicates::getCacheKeyImpl() const {
  std::ostringstream os;
  if (subtree_ == nullptr) {
    os << "COUNT_AVAILABLE_PREDICATES for all entities";
  } else {
    os << "COUNT_AVAILABLE_PREDICATES (col " << subjectColumnIndex_ << ")\n"
       << subtree_->getCacheKey();
  }
  return std::move(os).str();
}

// _____________________________________________________________________________
std::string CountAvailablePredicates::getDescriptor() const {
  if (subtree_ == nullptr) {
    return "CountAvailablePredicates for a all entities";
  }
  return "CountAvailablePredicates";
}

// _____________________________________________________________________________
size_t CountAvailablePredicates::getResultWidth() const { return 2; }

// _____________________________________________________________________________
std::vector<ColumnIndex> CountAvailablePredicates::resultSortedOn() const {
  // The result is not sorted on any column.
  return {};
}

// _____________________________________________________________________________
VariableToColumnMap CountAvailablePredicates::computeVariableToColumnMap()
    const {
  VariableToColumnMap varCols;
  auto col = makeAlwaysDefinedColumn;
  varCols[predicateVariable_] = col(0);
  varCols[countVariable_] = col(1);
  return varCols;
}

// _____________________________________________________________________________
float CountAvailablePredicates::getMultiplicity([[maybe_unused]] size_t col) {
  // Determining the multiplicity of the second column (the counts)
  // is not trivial (and potentially not possible) without computing
  // at least a part of the result first, so we always return 1.
  return 1.0f;
}

// _____________________________________________________________________________
uint64_t CountAvailablePredicates::getSizeEstimateBeforeLimit() {
  if (subtree_.get() != nullptr) {
    // Predicates are only computed for entities in the subtrees result.

    // This estimate is probably wildly innacurrate, but as it does not
    // depend on the order of operations of the subtree should be sufficient
    // for the type of optimizations the optimizer can currently do.
    size_t num_distinct = subtree_->getSizeEstimate() /
                          subtree_->getMultiplicity(subjectColumnIndex_);
    return num_distinct / getIndex().getAvgNumDistinctSubjectsPerPredicate();
  } else {
    // Predicates are counted for all entities. In this case the size estimate
    // should be accurate.
    return getIndex().getNumDistinctSubjectPredicatePairs() /
           getIndex().getAvgNumDistinctSubjectsPerPredicate();
  }
}

// _____________________________________________________________________________
size_t CountAvailablePredicates::getCostEstimate() {
  if (subtree_.get() != nullptr) {
    // Without knowing the ratio of elements that will have a pattern assuming
    // constant cost per entry should be reasonable (although non distinct
    // entries are of course actually cheaper).
    return subtree_->getCostEstimate() + subtree_->getSizeEstimate();
  } else {
    // the cost is proportional to the number of elements we need to write.
    return getSizeEstimateBeforeLimit();
  }
}

// _____________________________________________________________________________
Result CountAvailablePredicates::computeResult(
    [[maybe_unused]] bool requestLaziness) {
  AD_LOG_DEBUG << "CountAvailablePredicates result computation..." << std::endl;
  IdTable idTable{getExecutionContext()->getAllocator()};
  idTable.setNumColumns(2);

  const CompactVectorOfStrings<Id>& patterns =
      _executionContext->getIndex().getPatterns();

  AD_CORRECTNESS_CHECK(subtree_);
  // Determine whether we can perform the full scan optimization. It can be
  // applied if the `subtree_` is a single index scan of a triple
  // `?s ql:has-pattern ?p`. This relation contains exactly one triple per
  // entity. All subjects are therefore distinct, and the patterns can be
  // counted directly while the scan is consumed lazily.
  // TODO<joka921> The generic implementation below has to deduplicate the
  // subjects. It therefore requires a fully materialized `IdTableView`. Make it
  // consume its input lazily, carrying the last subject across the chunk
  // boundaries. It then also handles the `ql:has-pattern` case, and this
  // special case can be removed.
  bool isPatternTrickForAllEntities = [&]() {
    auto indexScan =
        dynamic_cast<const IndexScan*>(subtree_->getRootOperation().get());
    if (!indexScan) {
      return false;
    }
    if (!indexScan->subject().isVariable() ||
        !indexScan->object().isVariable()) {
      return false;
    }
    // Note: `HAS_PATTERN_PREDICATE` is a `std::string_view`, so it has to be
    // explicitly turned into an `Iri` before the comparison. Comparing it
    // directly would convert it into the `std::string` alternative of the
    // `TripleComponent` variant, which never compares equal to the `Iri`
    // alternative that the scan holds.
    TripleComponent hasPattern{
        TripleComponent::Iri::fromIriref(HAS_PATTERN_PREDICATE)};
    return indexScan->predicate() == hasPattern;
  }();

  if (isPatternTrickForAllEntities) {
    // Compute the predicates for all entities.
    auto subresult = subtree_->getResult(true);
    CountAvailablePredicates::computePatternTrickAllEntities(
        &idTable, patterns, *subresult,
        subtree_->getVariableColumn(predicateVariable_));
    return {std::move(idTable), resultSortedOn(), LocalVocab{}};
  } else {
    std::shared_ptr<const Result> subresult = subtree_->getResult();
    AD_LOG_DEBUG << "CountAvailablePredicates subresult computation done."
                 << std::endl;

    size_t width = subresult->idTableView().numColumns();
    size_t patternColumn = subtree_->getVariableColumn(predicateVariable_);
    ad_utility::callFixedSizeVi(width, [&](auto width) {
      return computePatternTrick<width>(subresult->idTableView(), &idTable,
                                        patterns, subjectColumnIndex_,
                                        patternColumn, runtimeInfo());
    });
    return {std::move(idTable), resultSortedOn(),
            subresult->getSharedLocalVocab()};
  }
}

// _____________________________________________________________________________
void CountAvailablePredicates::computePatternTrickAllEntities(
    IdTable* dynResult, const CompactVectorOfStrings<Id>& patterns,
    const Result& subresult, ColumnIndex patternColumn) const {
  IdTableStatic<2> result = std::move(*dynResult).toStatic<2>();
  AD_LOG_DEBUG << "For all entities." << std::endl;
  ad_utility::HashMap<Id, size_t> predicateCounts;
  // The pattern indices are dense, so the counts are kept in a vector, which
  // is much faster than a hash map for the hundreds of millions of rows of a
  // large knowledge graph. The last slot counts the entities without a
  // pattern (`Pattern::NoPattern`).
  std::vector<size_t> patternCounts(patterns.size() + 1, 0);
  // Note: In contrast to `computePatternTrick` the subjects don't have to be
  // deduplicated, because the `ql:has-pattern` relation contains exactly one
  // triple per entity.
  auto countPatterns = [&patternCounts, &patterns,
                        patternColumn](const auto& idTable) {
    for (Id patternId : idTable.getColumn(patternColumn)) {
      AD_CORRECTNESS_CHECK(patternId.getDatatype() == Datatype::Int);
      size_t patternIdx = patternId.getInt();
      if (patternIdx >= patterns.size()) {
        AD_CONTRACT_CHECK(patternIdx == Pattern::NoPattern);
        patternIdx = patterns.size();
      }
      patternCounts[patternIdx]++;
    }
  };
  // The subresult is lazy unless it was already fully materialized (for
  // example because it was read from the cache).
  if (subresult.isFullyMaterialized()) {
    countPatterns(subresult.idTableView());
  } else {
    for (const auto& pair : subresult.idTables()) {
      countPatterns(pair.idTable_);
    }
  }

  // Entities without a pattern contribute no predicates.
  for (size_t patternIdx = 0; patternIdx < patterns.size(); ++patternIdx) {
    size_t count = patternCounts[patternIdx];
    if (count == 0) {
      continue;
    }
    for (const auto& predicate : patterns[patternIdx]) {
      predicateCounts[predicate] += count;
    }
  }
  result.reserve(predicateCounts.size());
  for (const auto& [predicateId, count] : predicateCounts) {
    result.push_back({predicateId, Id::makeFromInt(count)});
  }
  *dynResult = std::move(result).toDynamic();
}

namespace {
// A HashMap from `T` to `size_t` that can be merged with another such map by
// adding the counts of corresponding keys. This is what
// `computeInParallelChunks` (see below) requires of its result type.
template <typename T>
struct CountMap : ad_utility::HashMap<T, size_t> {
  void mergeWith(const CountMap& other) {
    for (const auto& [key, count] : other) {
      (*this)[key] += count;
    }
  }
};

// The number of rows (patterns) that make up a single chunk of work in the
// first (second) loop of `computePatternTrick`. Inputs that are not larger than
// this are handled by a single thread, because then the work is dominated by
// the cost of spawning threads and of merging the partial results.
constexpr size_t CHUNK_SIZE_ROWS = 500'000;
constexpr size_t CHUNK_SIZE_PATTERNS = 100'000;
}  // namespace

// _____________________________________________________________________________
template <size_t WIDTH>
void CountAvailablePredicates::computePatternTrick(
    const IdTableView<0>& dynInput, IdTable* dynResult,
    const CompactVectorOfStrings<Id>& patterns, const size_t subjectColumnIdx,
    const size_t patternColumnIdx, RuntimeInformation& runtimeInfo) {
  const IdTableView<WIDTH> input = dynInput.asStaticView<WIDTH>();
  IdTableStatic<2> result = std::move(*dynResult).toStatic<2>();
  AD_LOG_DEBUG << "For " << input.size() << " entities in column "
               << subjectColumnIdx << std::endl;

  // The number of distinct predicates in the used patterns (for the
  // statistics below).
  size_t numPatternPredicates = 0;

  decltype(auto) subjectColumn = input.getColumn(subjectColumnIdx);
  decltype(auto) patternColumn = input.getColumn(patternColumnIdx);
  size_t numThreads =
      getRuntimeParameter<&RuntimeParameters::patternTrickNumThreads_>();
  CountMap<size_t> patternCounts = ad_utility::computeInParallelChunks(
      input.size(), CHUNK_SIZE_ROWS,
      [&subjectColumn, &patternColumn](CountMap<size_t>& counts, size_t begin,
                                       size_t end) {
        for (size_t i = begin; i < end; ++i) {
          // Skip over elements with the same subject (don't count them
          // twice). Note: The element before the first one of a chunk is
          // read, but never written, so this is safe to do in parallel.
          if (i > 0 && subjectColumn[i] == subjectColumn[i - 1]) {
            continue;
          }
          counts[patternColumn[i].getInt()]++;
        }
      },
      numThreads);
  AD_LOG_DEBUG << "Using " << patternCounts.size()
               << " patterns for computing the result." << std::endl;
  // the number of predicates counted with patterns
  size_t numPredicatesSubsumedInPatterns = 0;

  // flatten into a vector, to make iterable
  AD_LOG_DEBUG << "Converting PatternMap to vector" << std::endl;
  const std::vector<std::pair<size_t, size_t>> patternVec(patternCounts.begin(),
                                                          patternCounts.end());

  // Gather the statistics, and check that all the pattern indices are valid.
  // Both are cheap enough (they only look at the size of each pattern) to be
  // done sequentially.
  for (auto [patternIndex, patternCount] : patternVec) {
    if (patternIndex >= patterns.size()) {
      AD_CONTRACT_CHECK(patternIndex == Pattern::NoPattern);
      continue;
    }
    size_t patternSize = patterns[patternIndex].size();
    numPatternPredicates += patternSize;
    numPredicatesSubsumedInPatterns += patternCount * patternSize;
  }

  // resolve the patterns to predicate counts
  AD_LOG_DEBUG << "Start translating pattern counts to predicate counts"
               << std::endl;
  CountMap<Id> predicateCounts = ad_utility::computeInParallelChunks(
      patternVec.size(), CHUNK_SIZE_PATTERNS,
      [&patternVec, &patterns](CountMap<Id>& counts, size_t begin, size_t end) {
        for (auto [patternIndex, patternCount] : ql::ranges::subrange(
                 patternVec.begin() + begin, patternVec.begin() + end)) {
          // Entities without a pattern contribute no predicates. All other
          // pattern indices have been checked above.
          if (patternIndex >= patterns.size()) {
            continue;
          }
          for (Id predicate : patterns[patternIndex]) {
            counts[predicate] += patternCount;
          }
        }
      },
      numThreads);
  AD_LOG_DEBUG << "Finished translating pattern counts to predicate counts"
               << std::endl;
  // write the predicate counts to the result
  result.reserve(predicateCounts.size());
  for (const auto& it : predicateCounts) {
    result.push_back({it.first, Id::makeFromInt(it.second)});
  }
  AD_LOG_DEBUG << "Finished writing results" << std::endl;

  // Print interesting statistics about the pattern trick: the conceptual
  // cost with patterns (one lookup per row plus one count per distinct
  // predicate in the used patterns) vs the cost without patterns (one count
  // per predicate of every row).
  size_t costWithPatterns = input.size() + numPatternPredicates;
  size_t costWithoutPatterns = input.size() + numPredicatesSubsumedInPatterns;
  double costRatio =
      static_cast<double>(costWithPatterns) / costWithoutPatterns;
  AD_LOG_DEBUG << "The conceptual cost with patterns was " << costWithPatterns
               << " vs " << costWithoutPatterns << " without patterns"
               << std::endl;
  AD_LOG_DEBUG << "This gives a ratio with to without of " << costRatio
               << std::endl;

  // Add these values to the runtime info. NOTE: the value of
  // `numPredicatesWithRepetitions` is unchanged (it was previously computed
  // as `numPredicatesSubsumedInPatterns` plus a counter that was never
  // incremented); the two `percent...` details, whose values were also
  // computed from never-incremented counters (and hence always `0` or `NaN`),
  // have been removed.
  runtimeInfo.addDetail("numEntities", input.size());
  runtimeInfo.addDetail("numPredicatesWithRepetitions",
                        numPredicatesSubsumedInPatterns);
  runtimeInfo.addDetail("costWithoutPatterns", costWithoutPatterns);
  runtimeInfo.addDetail("costWithPatterns", costWithPatterns);
  runtimeInfo.addDetail("costRatio", costRatio * 100);
  *dynResult = std::move(result).toDynamic();
}

// _____________________________________________________________________________
std::unique_ptr<Operation> CountAvailablePredicates::cloneImpl() const {
  return std::make_unique<CountAvailablePredicates>(
      _executionContext, subtree_->clone(), subjectColumnIndex_,
      predicateVariable_, countVariable_);
}
