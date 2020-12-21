// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@neptun.uni-freiburg.de)

#include "CountAvailablePredicates.h"
#include "CallFixedSize.h"

// _____________________________________________________________________________
CountAvailablePredicates::CountAvailablePredicates(QueryExecutionContext* qec)
    : Operation(qec),
      _subtree(nullptr),
      _subjectColumnIndex(0),
      _subjectEntityName(),
      _predicateVarName("predicate"),
      _countVarName("count") {}

// _____________________________________________________________________________
CountAvailablePredicates::CountAvailablePredicates(
    QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> subtree,
    size_t subjectColumnIndex)
    : Operation(qec),
      _subtree(subtree),
      _subjectColumnIndex(subjectColumnIndex),
      _subjectEntityName(),
      _predicateVarName("predicate"),
      _countVarName("count") {}

CountAvailablePredicates::CountAvailablePredicates(QueryExecutionContext* qec,
                                                   std::string entityName)
    : Operation(qec),
      _subtree(nullptr),
      _subjectColumnIndex(0),
      _subjectEntityName(entityName),
      _predicateVarName("predicate"),
      _countVarName("count") {}

// _____________________________________________________________________________
string CountAvailablePredicates::asString(size_t indent) const {
  std::ostringstream os;

  std::string fixedPredString;
  if (_predicateEntityName) {
    fixedPredString =
        " for fixed predicate " + _predicateEntityName.value() + " ";
  }
  for (size_t i = 0; i < indent; ++i) {
    os << " ";
  }
  if (_subjectEntityName) {
    os << "COUNT_AVAILABLE_PREDICATES for " << _subjectEntityName.value()
       << fixedPredString;
  } else if (_subtree == nullptr) {
    os << "COUNT_AVAILABLE_PREDICATES for all entities" << fixedPredString;
  } else {
    os << "COUNT_AVAILABLE_PREDICATES (col " << _subjectColumnIndex << ")"
       << fixedPredString << "\n"
       << _subtree->asString(indent);
  }
  return os.str();
}

// _____________________________________________________________________________
string CountAvailablePredicates::getDescriptor() const {
  std::string p = _predicateEntityName ? " for fixed predicate" : "";
  if (_subjectEntityName) {
    return "CountAvailablePredicates for a single entity" + p;
  } else if (_subtree == nullptr) {
    return "CountAvailablePredicates for a all entities" + p;
  }
  return "CountAvailablePredicates" + p;
}

// _____________________________________________________________________________
size_t CountAvailablePredicates::getResultWidth() const {
  if (_predicateEntityName) {
    return 1;
  }
  return 2;
}

// _____________________________________________________________________________
vector<size_t> CountAvailablePredicates::resultSortedOn() const {
  // The result is not sorted on any column.
  return {};
}

// _____________________________________________________________________________
void CountAvailablePredicates::setVarNames(const std::string& predicateVarName,
                                           const std::string& countVarName) {
  _predicateVarName = predicateVarName;
  _countVarName = countVarName;
  if (!ad_utility::startsWith(predicateVarName, "?")) {
    // this makes this use the singleSpecialization version
    _predicateEntityName = predicateVarName;
  }
}

// _____________________________________________________________________________
ad_utility::HashMap<string, size_t>
CountAvailablePredicates::getVariableColumns() const {
  ad_utility::HashMap<string, size_t> varCols;
  if (!_predicateEntityName) {
    varCols[_predicateVarName] = 0;
    varCols[_countVarName] = 1;
  } else {
    varCols[_countVarName] = 0;
  }
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
    return num_distinct / getIndex().getHasPredicateMultiplicityPredicates();
  } else {
    // Predicates are counted for all entities. In this case the size estimate
    // should be accurate.
    return getIndex().getHasPredicateFullSize() /
           getIndex().getHasPredicateMultiplicityPredicates();
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

void CountAvailablePredicates::computeResultSinglePredicate(
    ResultTable* result) {
  LOG(DEBUG)
      << "CountAvailablePredicates result computation for single predicate..."
      << std::endl;
  result->_data.setCols(1);
  result->_sortedBy = resultSortedOn();
  result->_resultTypes.push_back(ResultTable::ResultType::VERBATIM);

  RuntimeInformation& runtimeInfo = getRuntimeInfo();

  const std::vector<PatternID>& hasPattern =
      _executionContext->getIndex().getHasPattern();
  const CompactStringVector<Id, Id>& hasPredicate =
      _executionContext->getIndex().getHasPredicate();

  const auto& predicateToPatternMap =
      _executionContext->getIndex().getPredicateToPatternMap();

  Id predicateId;
  if (!getIndex().getVocab().getId(_predicateEntityName.value(),
                                   &predicateId)) {
    return;
  }

  if (_subjectEntityName) {
    size_t entityId;
    // If the entity exists return the all predicates for that entitity,
    // otherwise return an empty result.
    if (getIndex().getVocab().getId(_subjectEntityName.value(), &entityId)) {
      IdTable input(1, _executionContext->getAllocator());
      input.push_back({entityId});
      int width = input.cols();
      CALL_FIXED_SIZE_1(
          width, CountAvailablePredicates::computeSinglePredicatePatternTrick,
          input, &result->_data, hasPattern, hasPredicate,
          predicateToPatternMap, 0, predicateId);
    }
  } else if (_subtree == nullptr) {
    throw std::runtime_error(
        "ql:has-predicate counting with a fixed predicate and no context "
        "triples is inot supported");
  } else {
    std::shared_ptr<const ResultTable> subresult = _subtree->getResult();
    runtimeInfo.addChild(_subtree->getRootOperation()->getRuntimeInfo());
    LOG(DEBUG) << "CountAvailablePredicates subresult computation done."
               << std::endl;

    int width = subresult->_data.cols();
    CALL_FIXED_SIZE_1(
        width, CountAvailablePredicates::computeSinglePredicatePatternTrick,
        subresult->_data, &result->_data, hasPattern, hasPredicate,
        predicateToPatternMap, _subjectColumnIndex, predicateId);
  }
  LOG(DEBUG) << "CountAvailablePredicates result computation done."
             << std::endl;
}

// _____________________________________________________________________________
void CountAvailablePredicates::computeResult(ResultTable* result) {
  if (_predicateEntityName) {
    return computeResultSinglePredicate(result);
  }

  LOG(DEBUG) << "CountAvailablePredicates result computation..." << std::endl;
  result->_data.setCols(2);
  result->_sortedBy = resultSortedOn();
  result->_resultTypes.push_back(ResultTable::ResultType::KB);
  result->_resultTypes.push_back(ResultTable::ResultType::VERBATIM);

  RuntimeInformation& runtimeInfo = getRuntimeInfo();

  const std::vector<PatternID>& hasPattern =
      _executionContext->getIndex().getHasPattern();
  const CompactStringVector<Id, Id>& hasPredicate =
      _executionContext->getIndex().getHasPredicate();
  const CompactStringVector<size_t, Id>& patterns =
      _executionContext->getIndex().getPatterns();

  if (_subjectEntityName) {
    size_t entityId;
    // If the entity exists return the all predicates for that entitity,
    // otherwise return an empty result.
    if (getIndex().getVocab().getId(_subjectEntityName.value(), &entityId)) {
      IdTable input(1, _executionContext->getAllocator());
      input.push_back({entityId});
      int width = input.cols();
      CALL_FIXED_SIZE_1(width, CountAvailablePredicates::computePatternTrick,
                        input, &result->_data, hasPattern, hasPredicate,
                        patterns, 0, &runtimeInfo);
    }
  } else if (_subtree == nullptr) {
    // Compute the predicates for all entities
    CountAvailablePredicates::computePatternTrickAllEntities(
        &result->_data, hasPattern, hasPredicate, patterns);
  } else {
    std::shared_ptr<const ResultTable> subresult = _subtree->getResult();
    runtimeInfo.addChild(_subtree->getRootOperation()->getRuntimeInfo());
    LOG(DEBUG) << "CountAvailablePredicates subresult computation done."
               << std::endl;

    int width = subresult->_data.cols();
    CALL_FIXED_SIZE_1(width, CountAvailablePredicates::computePatternTrick,
                      subresult->_data, &result->_data, hasPattern,
                      hasPredicate, patterns, _subjectColumnIndex,
                      &runtimeInfo);
  }
  LOG(DEBUG) << "CountAvailablePredicates result computation done."
             << std::endl;
}

void CountAvailablePredicates::computePatternTrickAllEntities(
    IdTable* dynResult, const vector<PatternID>& hasPattern,
    const CompactStringVector<Id, Id>& hasPredicate,
    const CompactStringVector<size_t, Id>& patterns) {
  IdTableStatic<2> result = dynResult->moveToStatic<2>();
  LOG(DEBUG) << "For all entities." << std::endl;
  ad_utility::HashMap<Id, size_t> predicateCounts;
  ad_utility::HashMap<size_t, size_t> patternCounts;

  size_t maxId = std::max(hasPattern.size(), hasPredicate.size());
  for (size_t i = 0; i < maxId; i++) {
    if (i < hasPattern.size() && hasPattern[i] != NO_PATTERN) {
      patternCounts[hasPattern[i]]++;
    } else if (i < hasPredicate.size()) {
      size_t numPredicates;
      Id* predicateData;
      std::tie(predicateData, numPredicates) = hasPredicate[i];
      if (numPredicates > 0) {
        for (size_t i = 0; i < numPredicates; i++) {
          auto it = predicateCounts.find(predicateData[i]);
          if (it == predicateCounts.end()) {
            predicateCounts[predicateData[i]] = 1;
          } else {
            it->second++;
          }
        }
      }
    }
  }

  LOG(DEBUG) << "Using " << patternCounts.size()
             << " patterns for computing the result." << std::endl;
  for (const auto& it : patternCounts) {
    std::pair<Id*, size_t> pattern = patterns[it.first];
    for (size_t i = 0; i < pattern.second; i++) {
      predicateCounts[pattern.first[i]] += it.second;
    }
  }
  result.reserve(predicateCounts.size());
  for (const auto& it : predicateCounts) {
    result.push_back({it.first, static_cast<Id>(it.second)});
  }
  *dynResult = result.moveToDynamic();
}

template <int I>
void CountAvailablePredicates::computePatternTrick(
    const IdTable& dynInput, IdTable* dynResult,
    const vector<PatternID>& hasPattern,
    const CompactStringVector<Id, Id>& hasPredicate,
    const CompactStringVector<size_t, Id>& patterns, const size_t subjectColumn,
    RuntimeInformation* runtimeInfo) {
  const IdTableStatic<I> input = dynInput.asStaticView<I>();
  IdTableStatic<2> result = dynResult->moveToStatic<2>();
  LOG(DEBUG) << "For " << input.size() << " entities in column "
             << subjectColumn << std::endl;

  class MergeableId : public ad_utility::HashMap<Id, size_t> {
   public:
    MergeableId& operator%=(const MergeableId& rhs) {
      for (const auto& [key, value] : rhs) {
        (*this)[key] += value;
      }
      return *this;
    }
  };

  class MergeableSizeT : public ad_utility::HashMap<size_t, size_t> {
   public:
    MergeableSizeT& operator%=(const MergeableSizeT& rhs) {
      for (const auto& [key, value] : rhs) {
        (*this)[key] += value;
      }
      return *this;
    }
  };
  MergeableId predicateCounts;
  MergeableSizeT patternCounts;

#pragma omp declare reduction(MergeHashmapsId:MergeableId : omp_out %= omp_in)
#pragma omp declare reduction(MergeHashmapsSizeT:MergeableSizeT \
                              : omp_out %= omp_in)

  // These variables are used to gather additional statistics
  size_t numEntitiesWithPatterns = 0;
  // the number of distinct predicates in patterns
  size_t numPatternPredicates = 0;
  // the number of predicates counted without patterns
  size_t numListPredicates = 0;

  if (input.size() > 0) {  // avoid strange OpenMP segfaults
#pragma omp parallel
#pragma omp single
#pragma omp taskloop grainsize(500000) default(none) reduction(MergeHashmapsId:predicateCounts) reduction(MergeHashmapsSizeT : patternCounts) \
                                       reduction(+ : numEntitiesWithPatterns) reduction(+: numPatternPredicates) reduction(+: numListPredicates) shared(input, subjectColumn, hasPattern, hasPredicate)
    for (size_t inputIdx = 0; inputIdx < input.size(); ++inputIdx) {
      // Skip over elements with the same subject (don't count them twice)
      Id subject = input(inputIdx, subjectColumn);
      if (inputIdx > 0 && subject == input(inputIdx - 1, subjectColumn)) {
        continue;
      }

      if (subject < hasPattern.size() && hasPattern[subject] != NO_PATTERN) {
        // The subject matches a pattern
        patternCounts[hasPattern[subject]]++;
        numEntitiesWithPatterns++;
      } else if (subject < hasPredicate.size()) {
        // The subject does not match a pattern
        size_t numPredicates;
        Id* predicateData;
        std::tie(predicateData, numPredicates) = hasPredicate[subject];
        numListPredicates += numPredicates;
        if (numPredicates > 0) {
          for (size_t i = 0; i < numPredicates; i++) {
            predicateCounts[predicateData[i]]++;
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
  std::vector<std::pair<size_t, size_t>> patternVec;
  patternVec.reserve(patternCounts.size());
  for (const auto& p : patternCounts) {
    patternVec.push_back(p);
  }

  LOG(DEBUG) << "Start convertin patterns" << std::endl;
  if (patternVec.begin() != patternVec.end()) {  // avoid segfaults with OpenMP
#pragma omp parallel
#pragma omp single
#pragma omp taskloop grainsize(100000) default(none) reduction(MergeHashmapsId:predicateCounts) reduction(+ : numPredicatesSubsumedInPatterns) \
                                       reduction(+ : numEntitiesWithPatterns) reduction(+: numPatternPredicates) reduction(+: numListPredicates) shared( patternVec, patterns)
    for (auto it = patternVec.begin(); it != patternVec.end(); ++it) {
      std::pair<Id*, size_t> pattern = patterns[it->first];
      numPatternPredicates += pattern.second;
      for (size_t i = 0; i < pattern.second; i++) {
        predicateCounts[pattern.first[i]] += it->second;
        numPredicatesSubsumedInPatterns += it->second;
      }
    }
  }
  LOG(DEBUG) << "Finished converting patterns" << std::endl;
  // write the predicate counts to the result
  result.reserve(predicateCounts.size());
  for (const auto& it : predicateCounts) {
    result.push_back({it.first, static_cast<Id>(it.second)});
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

template <int I>
void CountAvailablePredicates::computeSinglePredicatePatternTrick(
    const IdTable& dynInput, IdTable* dynResult,
    const vector<PatternID>& hasPattern,
    const CompactStringVector<Id, Id>& hasPredicate,
    const ad_utility::HashMap<Id, ad_utility::HashSet<size_t>>&
        predicateToPattern,
    const size_t subjectColumn, const Id predicateId) {
  const IdTableStatic<I> input = dynInput.asStaticView<I>();
  IdTableStatic<1> result = dynResult->moveToStatic<1>();
  LOG(DEBUG) << "For " << input.size() << " entities in column "
             << subjectColumn << std::endl;

  size_t resCount = 0;
  if (!predicateToPattern.contains(predicateId)) {
    return;
  }

  const auto& map = predicateToPattern.at(predicateId);

  checkTimeout();

  LOG(INFO) << "Start loop for certifying CountPredicate" << std::endl;

  if (input.size() > 0) {  // avoid strange OpenMP segfaults
                           //#pragma omp parallel
                           //#pragma omp taskgroup
                           //#pragma omp single
    //#pragma omp taskloop grainsize(500000) default(none) reduction(+:resCount)
    // private(localElementCount, localFlag)  shared(sharedFlag, map,
    // predicateId, input, subjectColumn, hasPattern, hasPredicate)
    LOG(INFO) << "Before Loop" << std::endl;
    for (size_t inputIdx = 0; inputIdx < input.size(); ++inputIdx) {
      if (inputIdx % 1000 == 0) {
        LOG(INFO) << "Handled another 1000 els" << std::endl;
      }

      // Skip over elements with the same subject (don't count them twice)
      Id subject = input(inputIdx, subjectColumn);
      if (inputIdx > 0 && subject == input(inputIdx - 1, subjectColumn)) {
        LOG(INFO) << "Skipping because of repetitions" << std::endl;
        continue;
      }

      if (subject < hasPattern.size() && hasPattern[subject] != NO_PATTERN) {
        // The subject matches a pattern
        if (map.count(hasPattern[subject])) {
          resCount++;
          LOG(INFO) << "Found an element" << std::endl;
        }
      } else if (subject < hasPredicate.size()) {
        LOG(INFO) << "Subject has no pattern" << std::endl;
        // The subject does not match a pattern
        size_t numPredicates;
        Id* predicateData;
        std::tie(predicateData, numPredicates) = hasPredicate[subject];
        if (numPredicates > 0) {
          for (size_t i = 0; i < numPredicates; i++) {
            if (predicateData[i] == predicateId) {
              LOG(INFO) << "Found an element" << std::endl;
              resCount++;
              break;
            }
          }
        } else {
          LOG(INFO) << "No pattern or has-relation entry found for entity "
                    << std::to_string(subject) << std::endl;
        }
      } else {
        LOG(INFO) << "Subject " << subject
                  << " does not appear to be an entity "
                     "(its id is to high)."
                  << std::endl;
      }
    }
  }
  LOG(INFO) << "Finished loop" << std::endl;
  checkTimeout();

  // result.push_back({resCount});
  LOG(INFO) << " Found" << resCount << "elements" << std::endl;
  result.push_back({resCount});
  *dynResult = result.moveToDynamic();
}
