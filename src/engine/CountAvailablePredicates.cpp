// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@neptun.uni-freiburg.de)

#include "./CountAvailablePredicates.h"

#include "./CallFixedSize.h"

// _____________________________________________________________________________
CountAvailablePredicates::CountAvailablePredicates(QueryExecutionContext* qec,
                                                   Variable predicateVariable,
                                                   Variable countVariable)
    : Operation(qec),
      _subtree(nullptr),
      _subjectColumnIndex(0),
      _predicateVarName(std::move(predicateVariable)),
      _countVarName(std::move(countVariable)) {}

// _____________________________________________________________________________
CountAvailablePredicates::CountAvailablePredicates(
    QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> subtree,
    size_t subjectColumnIndex, Variable predicateVariable,
    Variable countVariable)
    : Operation(qec),
      _subtree(QueryExecutionTree::createSortedTree(std::move(subtree),
                                                    {subjectColumnIndex})),
      _subjectColumnIndex(subjectColumnIndex),
      _predicateVarName(std::move(predicateVariable)),
      _countVarName(std::move(countVariable)) {}

// _____________________________________________________________________________
string CountAvailablePredicates::asStringImpl(size_t indent) const {
  std::ostringstream os;
  for (size_t i = 0; i < indent; ++i) {
    os << " ";
  }
  if (_subtree == nullptr) {
    os << "COUNT_AVAILABLE_PREDICATES for all entities";
  } else {
    os << "COUNT_AVAILABLE_PREDICATES (col " << _subjectColumnIndex << ")\n"
       << _subtree->asString(indent);
  }
  return std::move(os).str();
}

// _____________________________________________________________________________
string CountAvailablePredicates::getDescriptor() const {
  if (_subtree == nullptr) {
    return "CountAvailablePredicates for a all entities";
  }
  return "CountAvailablePredicates";
}

// _____________________________________________________________________________
size_t CountAvailablePredicates::getResultWidth() const { return 2; }

// _____________________________________________________________________________
vector<size_t> CountAvailablePredicates::resultSortedOn() const {
  // The result is not sorted on any column.
  return {};
}

// _____________________________________________________________________________
Operation::VariableToColumnMap
CountAvailablePredicates::computeVariableToColumnMap() const {
  ad_utility::HashMap<string, size_t> varCols;
  varCols[_predicateVarName.name()] = 0;
  varCols[_countVarName.name()] = 1;
  return varCols;
}

// _____________________________________________________________________________
float CountAvailablePredicates::getMultiplicity(size_t col) {
  if (col == 0) {
    return 1;
  } else {
    // Determining the multiplicity of the second column (the counts)
    // is non trivial (and potentially not possible) without computing
    // at least a part of the result first.
    return 1;
  }
}

// _____________________________________________________________________________
size_t CountAvailablePredicates::getSizeEstimate() {
  if (_subtree.get() != nullptr) {
    // Predicates are only computed for entities in the subtrees result.

    // This estimate is probably wildly innacurrate, but as it does not
    // depend on the order of operations of the subtree should be sufficient
    // for the type of optimizations the optimizer can currently do.
    size_t num_distinct = _subtree->getSizeEstimate() /
                          _subtree->getMultiplicity(_subjectColumnIndex);
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
  if (_subtree.get() != nullptr) {
    // Without knowing the ratio of elements that will have a pattern assuming
    // constant cost per entry should be reasonable (altough non distinct
    // entries are of course actually cheaper).
    return _subtree->getCostEstimate() + _subtree->getSizeEstimate();
  } else {
    // the cost is proportional to the number of elements we need to write.
    return getSizeEstimate();
  }
}

// _____________________________________________________________________________
void CountAvailablePredicates::computeResult(ResultTable* result) {
  LOG(DEBUG) << "CountAvailablePredicates result computation..." << std::endl;
  result->_idTable.setCols(2);
  result->_sortedBy = resultSortedOn();
  result->_resultTypes.push_back(ResultTable::ResultType::KB);
  result->_resultTypes.push_back(ResultTable::ResultType::VERBATIM);

  RuntimeInformation& runtimeInfo = getRuntimeInfo();

  const std::vector<PatternID>& hasPattern =
      _executionContext->getIndex().getHasPattern();
  const CompactVectorOfStrings<Id>& hasPredicate =
      _executionContext->getIndex().getHasPredicate();
  const CompactVectorOfStrings<Id>& patterns =
      _executionContext->getIndex().getPatterns();

  if (_subtree == nullptr) {
    // Compute the predicates for all entities
    CountAvailablePredicates::computePatternTrickAllEntities(
        &result->_idTable, hasPattern, hasPredicate, patterns);
  } else {
    std::shared_ptr<const ResultTable> subresult = _subtree->getResult();
    LOG(DEBUG) << "CountAvailablePredicates subresult computation done."
               << std::endl;

    int width = subresult->_idTable.cols();
    CALL_FIXED_SIZE_1(width, CountAvailablePredicates::computePatternTrick,
                      subresult->_idTable, &result->_idTable, hasPattern,
                      hasPredicate, patterns, _subjectColumnIndex,
                      &runtimeInfo);
  }
  LOG(DEBUG) << "CountAvailablePredicates result computation done."
             << std::endl;
}

void CountAvailablePredicates::computePatternTrickAllEntities(
    IdTable* dynResult, const vector<PatternID>& hasPattern,
    const CompactVectorOfStrings<Id>& hasPredicate,
    const CompactVectorOfStrings<Id>& patterns) {
  IdTableStatic<2> result = dynResult->moveToStatic<2>();
  LOG(DEBUG) << "For all entities." << std::endl;
  ad_utility::HashMap<Id, size_t> predicateCounts;
  ad_utility::HashMap<size_t, size_t> patternCounts;

  size_t maxId = std::max(hasPattern.size(), hasPredicate.size());
  for (size_t i = 0; i < maxId; i++) {
    if (i < hasPattern.size() && hasPattern[i] != NO_PATTERN) {
      patternCounts[hasPattern[i]]++;
    } else if (i < hasPredicate.size()) {
      auto predicates = hasPredicate[i];
      for (const auto& predicate : predicates) {
        auto it = predicateCounts.find(predicate);
        if (it == predicateCounts.end()) {
          predicateCounts[predicate] = 1;
        } else {
          it->second++;
        }
      }
    }
  }

  LOG(DEBUG) << "Using " << patternCounts.size()
             << " patterns for computing the result." << std::endl;
  for (const auto& it : patternCounts) {
    for (const auto& predicate : patterns[it.first]) {
      predicateCounts[predicate] += it.second;
    }
  }
  result.reserve(predicateCounts.size());
  for (const auto& it : predicateCounts) {
    result.push_back({it.first, Id::makeFromInt(it.second)});
  }
  *dynResult = result.moveToDynamic();
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

template <int WIDTH>
void CountAvailablePredicates::computePatternTrick(
    const IdTable& dynInput, IdTable* dynResult,
    const vector<PatternID>& hasPattern,
    const CompactVectorOfStrings<Id>& hasPredicate,
    const CompactVectorOfStrings<Id>& patterns, const size_t subjectColumn,
    RuntimeInformation* runtimeInfo) {
  const IdTableView<WIDTH> input = dynInput.asStaticView<WIDTH>();
  IdTableStatic<2> result = dynResult->moveToStatic<2>();
  LOG(DEBUG) << "For " << input.size() << " entities in column "
             << subjectColumn << std::endl;

  MergeableHashMap<Id> predicateCounts;
  MergeableHashMap<size_t> patternCounts;

  // declare openmp reductions which aggregate Hashmaps by adding the values for
  // corresponding keys
#pragma omp declare reduction(MergeHashmapsId:MergeableHashMap<Id> \
                              : omp_out %= omp_in)
#pragma omp declare reduction(MergeHashmapsSizeT:MergeableHashMap<size_t> \
                              : omp_out %= omp_in)

  // These variables are used to gather additional statistics
  size_t numEntitiesWithPatterns = 0;
  // the number of distinct predicates in patterns
  size_t numPatternPredicates = 0;
  // the number of predicates counted without patterns
  size_t numListPredicates = 0;

  if (input.size() > 0) {  // avoid strange OpenMP segfaults on GCC
#pragma omp parallel
#pragma omp single
#pragma omp taskloop grainsize(500000) default(none) reduction(MergeHashmapsId:predicateCounts) reduction(MergeHashmapsSizeT : patternCounts) \
                                       reduction(+ : numEntitiesWithPatterns) reduction(+: numPatternPredicates) reduction(+: numListPredicates) shared(input, subjectColumn, hasPattern, hasPredicate)
    for (size_t inputIdx = 0; inputIdx < input.size(); ++inputIdx) {
      // Skip over elements with the same subject (don't count them twice)
      Id subjectId = input(inputIdx, subjectColumn);
      if (inputIdx > 0 && subjectId == input(inputIdx - 1, subjectColumn)) {
        continue;
      }
      if (subjectId.getDatatype() != Datatype::VocabIndex) {
        // Ignore numeric literals and other types that are folded into
        // the value IDs. They can never be subjects and thus also have no
        // patterns.
        continue;
      }
      auto subject = subjectId.getVocabIndex().get();

      if (subject < hasPattern.size() && hasPattern[subject] != NO_PATTERN) {
        // The subject matches a pattern
        patternCounts[hasPattern[subject]]++;
        numEntitiesWithPatterns++;
      } else if (subject < hasPredicate.size()) {
        // The subject does not match a pattern
        const auto& pattern = hasPredicate[subject];
        numListPredicates += pattern.size();
        if (!pattern.empty()) {
          for (const auto& predicate : pattern) {
            predicateCounts[predicate]++;
          }
        } else {
          LOG(TRACE) << "No pattern or has-relation entry found for entity "
                     << std::to_string(subject) << std::endl;
        }
      } else {
        LOG(TRACE) << "Subject " << subject
                   << " does not appear to be an entity "
                      "(its id is to high)."
                   << std::endl;
      }
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
  if (patternVec.begin() !=
      patternVec.end()) {  // avoid segfaults with OpenMP on GCC
#pragma omp parallel
#pragma omp single
#pragma omp taskloop grainsize(100000) default(none) reduction(MergeHashmapsId:predicateCounts) reduction(+ : numPredicatesSubsumedInPatterns) \
                                       reduction(+ : numEntitiesWithPatterns) reduction(+: numPatternPredicates) reduction(+: numListPredicates) shared( patternVec, patterns)
    for (auto it = patternVec.begin(); it != patternVec.end(); ++it) {
      const auto& pattern = patterns[it->first];
      numPatternPredicates += pattern.size();
      for (const auto& predicate : pattern) {
        predicateCounts[predicate] += it->second;
        numPredicatesSubsumedInPatterns += it->second;
      }
    }
  }
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
  // Print the cost improvement using the the pattern trick gave us
  LOG(DEBUG) << "This gives a ratio  with to without of " << costRatio
             << std::endl;

  // Add these values to the runtime info
  runtimeInfo->addDetail("numEntities", input.size());
  runtimeInfo->addDetail("numPredicatesWithRepetitions",
                         numPredicatesWithRepetitions);
  runtimeInfo->addDetail("percentEntitesWithPatterns", ratioHasPatterns * 100);
  runtimeInfo->addDetail("percentPredicatesFromPatterns",
                         ratioCountedWithPatterns * 100);
  runtimeInfo->addDetail("costWithoutPatterns", costWithoutPatterns);
  runtimeInfo->addDetail("costWithPatterns", costWithPatterns);
  runtimeInfo->addDetail("costRatio", costRatio * 100);
  *dynResult = result.moveToDynamic();
}
