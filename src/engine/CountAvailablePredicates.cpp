// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@neptun.uni-freiburg.de)

#include "engine/CountAvailablePredicates.h"

#include "engine/CallFixedSize.h"
#include "engine/IndexScan.h"
#include "index/IndexImpl.h"

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
string CountAvailablePredicates::getCacheKeyImpl() const {
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
string CountAvailablePredicates::getDescriptor() const {
  if (subtree_ == nullptr) {
    return "CountAvailablePredicates for a all entities";
  }
  return "CountAvailablePredicates";
}

// _____________________________________________________________________________
size_t CountAvailablePredicates::getResultWidth() const { return 2; }

// _____________________________________________________________________________
vector<ColumnIndex> CountAvailablePredicates::resultSortedOn() const {
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
  LOG(DEBUG) << "CountAvailablePredicates result computation..." << std::endl;
  IdTable idTable{getExecutionContext()->getAllocator()};
  idTable.setNumColumns(2);

  const CompactVectorOfStrings<Id>& patterns =
      _executionContext->getIndex().getPatterns();

  AD_CORRECTNESS_CHECK(subtree_);
  // Determine whether we can perform the full scan optimization. It can be
  // applied if the `subtree_` is a single index scan of a triple
  // `?s ql:has-pattern ?p`.
  // TODO<joka921> As soon as we have a lazy implementation for all index scans
  // or even all operations Then the special case for all entities can be
  // removed.
  bool isPatternTrickForAllEntities = [&]() {
    auto indexScan =
        dynamic_cast<const IndexScan*>(subtree_->getRootOperation().get());
    if (!indexScan) {
      return false;
    }
    if (!indexScan->getSubject().isVariable() ||
        !indexScan->getObject().isVariable()) {
      return false;
    }

    return indexScan->getPredicate() == HAS_PATTERN_PREDICATE;
  }();

  if (isPatternTrickForAllEntities) {
    subtree_->getRootOperation()->updateRuntimeInformationWhenOptimizedOut(
        RuntimeInformation::Status::lazilyMaterialized);
    // Compute the predicates for all entities
    CountAvailablePredicates::computePatternTrickAllEntities(&idTable,
                                                             patterns);
    return {std::move(idTable), resultSortedOn(), LocalVocab{}};
  } else {
    std::shared_ptr<const Result> subresult = subtree_->getResult();
    LOG(DEBUG) << "CountAvailablePredicates subresult computation done."
               << std::endl;

    size_t width = subresult->idTable().numColumns();
    size_t patternColumn = subtree_->getVariableColumn(predicateVariable_);
    CALL_FIXED_SIZE(width, &computePatternTrick, subresult->idTable(), &idTable,
                    patterns, subjectColumnIndex_, patternColumn,
                    runtimeInfo());
    return {std::move(idTable), resultSortedOn(),
            subresult->getSharedLocalVocab()};
  }
}

// _____________________________________________________________________________
void CountAvailablePredicates::computePatternTrickAllEntities(
    IdTable* dynResult, const CompactVectorOfStrings<Id>& patterns) const {
  IdTableStatic<2> result = std::move(*dynResult).toStatic<2>();
  LOG(DEBUG) << "For all entities." << std::endl;
  ad_utility::HashMap<Id, size_t> predicateCounts;
  ad_utility::HashMap<size_t, size_t> patternCounts;
  auto fullHasPattern =
      getExecutionContext()
          ->getIndex()
          .getImpl()
          .getPermutation(Permutation::Enum::PSO)
          .lazyScan({qlever::specialIds.at(HAS_PATTERN_PREDICATE), std::nullopt,
                     std::nullopt},
                    std::nullopt, {}, cancellationHandle_);
  for (const auto& idTable : fullHasPattern) {
    for (const auto& patternId : idTable.getColumn(1)) {
      AD_CORRECTNESS_CHECK(patternId.getDatatype() == Datatype::Int);
      patternCounts[patternId.getInt()]++;
    }
  }

  LOG(DEBUG) << "Using " << patternCounts.size()
             << " patterns for computing the result" << std::endl;
  for (const auto& [patternIdx, count] : patternCounts) {
    AD_CORRECTNESS_CHECK(patternIdx < patterns.size());
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

/**
 * @ brief A Hashmap from T to size_t which additionally supports merging of
 * Hashmaps
 *
 * publicly inherits from ad_utility::HashMap<T, size_t> and additionally
 * provides operator%= which merges Hashmaps by adding the values for
 * corresponding keys. This is needed for the parallel pattern trick
 *
 */
template <typename T>
class MergeableHashMap : public ad_utility::HashMap<T, size_t> {
 public:
  MergeableHashMap& operator%=(const MergeableHashMap& rhs) {
    for (const auto& [key, value] : rhs) {
      (*this)[key] += value;
    }
    return *this;
  }
};

// _____________________________________________________________________________
template <size_t WIDTH>
void CountAvailablePredicates::computePatternTrick(
    const IdTable& dynInput, IdTable* dynResult,
    const CompactVectorOfStrings<Id>& patterns, const size_t subjectColumnIdx,
    const size_t patternColumnIdx, RuntimeInformation& runtimeInfo) {
  const IdTableView<WIDTH> input = dynInput.asStaticView<WIDTH>();
  IdTableStatic<2> result = std::move(*dynResult).toStatic<2>();
  LOG(DEBUG) << "For " << input.size() << " entities in column "
             << subjectColumnIdx << std::endl;

  MergeableHashMap<Id> predicateCounts;
  MergeableHashMap<size_t> patternCounts;

  // declare openmp reductions which aggregate Hashmaps by adding the values for
  // corresponding keys
#pragma omp declare reduction( \
        MergeHashmapsId : MergeableHashMap<Id> : omp_out %= omp_in)
#pragma omp declare reduction( \
        MergeHashmapsSizeT : MergeableHashMap<size_t> : omp_out %= omp_in)

  // These variables are used to gather additional statistics
  size_t numEntitiesWithPatterns = 0;
  // the number of distinct predicates in patterns
  size_t numPatternPredicates = 0;
  // the number of predicates counted without patterns
  size_t numListPredicates = 0;

  if (input.size() > 0) {  // avoid strange OpenMP segfaults on GCC
    decltype(auto) subjectColumn = input.getColumn(subjectColumnIdx);
    decltype(auto) patternColumn = input.getColumn(patternColumnIdx);
#pragma omp parallel
#pragma omp single
#pragma omp taskloop grainsize(500000) default(none)                           \
    reduction(MergeHashmapsId : predicateCounts)                               \
    reduction(MergeHashmapsSizeT : patternCounts)                              \
    reduction(+ : numEntitiesWithPatterns) reduction(+ : numPatternPredicates) \
    reduction(+ : numListPredicates)                                           \
    shared(input, subjectColumn, patternColumn)
    for (size_t i = 0; i < input.size(); ++i) {
      // Skip over elements with the same subject (don't count them twice)
      Id subjectId = subjectColumn[i];
      if (i > 0 && subjectId == subjectColumn[i - 1]) {
        continue;
      }
      patternCounts[patternColumn[i].getInt()]++;
    }
  }
  LOG(DEBUG) << "Using " << patternCounts.size()
             << " patterns for computing the result." << std::endl;
  // the number of predicates counted with patterns
  size_t numPredicatesSubsumedInPatterns = 0;
  // resolve the patterns to predicate counts

  LOG(DEBUG) << "Converting PatternMap to vector" << std::endl;
  // flatten into a vector, to make iterable
  const std::vector<std::pair<size_t, size_t>> patternVec(patternCounts.begin(),
                                                          patternCounts.end());

  LOG(DEBUG) << "Start translating pattern counts to predicate counts"
             << std::endl;
  bool illegalPatternIndexFound = false;
  if (patternVec.begin() !=
      patternVec.end()) {  // avoid segfaults with OpenMP on GCC
#pragma omp parallel
#pragma omp single
#pragma omp taskloop grainsize(100000) default(none)                           \
    reduction(MergeHashmapsId : predicateCounts)                               \
    reduction(+ : numPredicatesSubsumedInPatterns)                             \
    reduction(+ : numEntitiesWithPatterns) reduction(+ : numPatternPredicates) \
    reduction(+ : numListPredicates) shared(patternVec, patterns)              \
    reduction(|| : illegalPatternIndexFound)
    // TODO<joka921> When we use iterators (`patternVec.begin()`) for the loop,
    // there is a strange warning on clang15 when OpenMP is activated. Find out
    // whether this is a known issue and whether this will be fixed in later
    // versions of clang.
    for (size_t i = 0; i != patternVec.size(); ++i) {
      auto [patternIndex, patternCount] = patternVec[i];
      // TODO<joka921> As soon as we have a better way of handling the
      // parallelism, the following block can become a simple AD_CONTRACT_CHECK.
      if (patternIndex >= patterns.size()) {
        if (patternIndex != NO_PATTERN) {
          illegalPatternIndexFound = true;
        }
        continue;
      }
      const auto& pattern = patterns[patternIndex];
      numPatternPredicates += pattern.size();
      for (const auto& predicate : pattern) {
        predicateCounts[predicate] += patternCount;
        numPredicatesSubsumedInPatterns += patternCount;
      }
    }
  }
  AD_CONTRACT_CHECK(!illegalPatternIndexFound);
  LOG(DEBUG) << "Finished translating pattern counts to predicate counts"
             << std::endl;
  // write the predicate counts to the result
  result.reserve(predicateCounts.size());
  for (const auto& it : predicateCounts) {
    result.push_back({it.first, Id::makeFromInt(it.second)});
  }
  LOG(DEBUG) << "Finished writing results" << std::endl;

  // Print interesting statistics about the pattern trick
  double ratioHasPatterns =
      static_cast<double>(numEntitiesWithPatterns) / input.size();
  size_t numPredicatesWithRepetitions =
      numPredicatesSubsumedInPatterns + numListPredicates;
  double ratioCountedWithPatterns =
      static_cast<double>(numPredicatesSubsumedInPatterns) /
      numPredicatesWithRepetitions;

  size_t costWithPatterns =
      input.size() + numListPredicates + numPatternPredicates;
  size_t costWithoutPatterns = input.size() + numPredicatesWithRepetitions;
  double costRatio =
      static_cast<double>(costWithPatterns) / costWithoutPatterns;
  // Print the ratio of entities that used a pattern
  LOG(DEBUG) << numEntitiesWithPatterns << " of " << input.size()
             << " entities had a pattern. That equals "
             << (ratioHasPatterns * 100) << " %" << std::endl;
  // Print info about how many predicates where counted with patterns
  LOG(DEBUG) << "Of the " << numPredicatesWithRepetitions << "predicates "
             << numPredicatesSubsumedInPatterns
             << " were counted with patterns, " << numListPredicates
             << " were counted without.";
  LOG(DEBUG) << "The ratio is " << (ratioCountedWithPatterns * 100) << "%"
             << std::endl;
  // Print information about of efficient the pattern trick is
  LOG(DEBUG) << "The conceptual cost with patterns was " << costWithPatterns
             << " vs " << costWithoutPatterns << " without patterns"
             << std::endl;
  // Print the cost improvement using the pattern trick gave us
  LOG(DEBUG) << "This gives a ratio  with to without of " << costRatio
             << std::endl;

  // Add these values to the runtime info
  runtimeInfo.addDetail("numEntities", input.size());
  runtimeInfo.addDetail("numPredicatesWithRepetitions",
                        numPredicatesWithRepetitions);
  runtimeInfo.addDetail("percentEntitesWithPatterns", ratioHasPatterns * 100);
  runtimeInfo.addDetail("percentPredicatesFromPatterns",
                        ratioCountedWithPatterns * 100);
  runtimeInfo.addDetail("costWithoutPatterns", costWithoutPatterns);
  runtimeInfo.addDetail("costWithPatterns", costWithPatterns);
  runtimeInfo.addDetail("costRatio", costRatio * 100);
  *dynResult = std::move(result).toDynamic();
}
