// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@neptun.uni-freiburg.de)

#include "./CountAvailablePredicates.h"

#include "./CallFixedSize.h"

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
  for (size_t i = 0; i < indent; ++i) {
    os << " ";
  }
  if (_subjectEntityName) {
    os << "COUNT_AVAILABLE_PREDICATES for " << _subjectEntityName.value();
  } else if (_subtree == nullptr) {
    os << "COUNT_AVAILABLE_PREDICATES for all entities";
  } else {
    os << "COUNT_AVAILABLE_PREDICATES (col " << _subjectColumnIndex << ")\n"
       << _subtree->asString(indent);
  }
  return os.str();
}

// _____________________________________________________________________________
string CountAvailablePredicates::getDescriptor() const {
  if (_subjectEntityName) {
    return "CountAvailablePredicates for a single entity";
  } else if (_subtree == nullptr) {
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
void CountAvailablePredicates::setVarNames(const std::string& predicateVarName,
                                           const std::string& countVarName) {
  _predicateVarName = predicateVarName;
  _countVarName = countVarName;
}

// _____________________________________________________________________________
ad_utility::HashMap<string, size_t>
CountAvailablePredicates::getVariableColumns() const {
  ad_utility::HashMap<string, size_t> varCols;
  varCols[_predicateVarName] = 0;
  varCols[_countVarName] = 1;
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
    return num_distinct /
           getIndex().getPatternIndex().getHasPredicateMultiplicityPredicates();
  } else {
    // Predicates are counted for all entities. In this case the size estimate
    // should be accurate.
    return getIndex().getPatternIndex().getHasPredicateFullSize() /
           getIndex().getPatternIndex().getHasPredicateMultiplicityPredicates();
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
  result->_data.setCols(2);
  result->_sortedBy = resultSortedOn();
  result->_resultTypes.push_back(ResultTable::ResultType::KB);
  result->_resultTypes.push_back(ResultTable::ResultType::VERBATIM);

  auto visitor = [&](const auto& patternImpl) {
    computeResult(result, patternImpl);
  };

  std::visit(visitor,
             _executionContext->getIndex().getPatternIndex().getPatternData());

  LOG(DEBUG) << "CountAvailablePredicates result computation done."
             << std::endl;
}

template <typename PredicateId>
void CountAvailablePredicates::computeResult(
    ResultTable* result,
    const PatternContainerImpl<PredicateId>& pattern_data) {
  RuntimeInformation& runtimeInfo = getRuntimeInfo();

  const auto& hasPattern = pattern_data.hasPattern();
  const auto& patterns = pattern_data.patterns();
  const auto& predicateGlobalIds =
      _executionContext->getIndex().getPatternIndex().getPredicateGlobalIds();

  if (_subjectEntityName) {
    size_t entityId;
    // If the entity exists return the all predicates for that entitity,
    // otherwise return an empty result.
    if (getIndex().getVocab().getId(_subjectEntityName.value(), &entityId)) {
      IdTable input(1, _executionContext->getAllocator());
      input.push_back({entityId});
      int width = input.cols();
      CALL_FIXED_SIZE_1(width, CountAvailablePredicates::computePatternTrick,
                        input, &result->_data, hasPattern, patterns,
                        predicateGlobalIds, 0, &runtimeInfo);
    }
  } else if (_subtree == nullptr) {
    // Compute the predicates for all entities
    CountAvailablePredicates::computePatternTrickAllEntities(
        &result->_data, hasPattern, patterns, predicateGlobalIds);
  } else {
    std::shared_ptr<const ResultTable> subresult = _subtree->getResult();
    runtimeInfo.addChild(_subtree->getRootOperation()->getRuntimeInfo());
    LOG(DEBUG) << "CountAvailablePredicates subresult computation done."
               << std::endl;

    int width = subresult->_data.cols();
    CALL_FIXED_SIZE_1(width, CountAvailablePredicates::computePatternTrick,
                      subresult->_data, &result->_data, hasPattern, patterns,
                      predicateGlobalIds, _subjectColumnIndex, &runtimeInfo);
  }
  LOG(DEBUG) << "CountAvailablePredicates result computation done."
             << std::endl;
}

template <typename PredicateId>
void CountAvailablePredicates::computePatternTrickAllEntities(
    IdTable* dynResult, const vector<PatternID>& hasPattern,
    const CompactStringVector<size_t, PredicateId>& patterns,
    const std::vector<Id>& predicateGlobalIds) {
  IdTableStatic<2> result = dynResult->moveToStatic<2>();
  LOG(DEBUG) << "For all entities." << std::endl;
  ad_utility::HashMap<Id, size_t> predicateCounts;
  ad_utility::HashMap<size_t, size_t> patternCounts;

  size_t maxId = hasPattern.size();
  for (size_t i = 0; i < maxId; i++) {
    if (i < hasPattern.size() && hasPattern[i] != NO_PATTERN) {
      patternCounts[hasPattern[i]]++;
    }
  }

  LOG(DEBUG) << "Using " << patternCounts.size()
             << " patterns for computing the result." << std::endl;
  for (const auto& it : patternCounts) {
    const auto& pattern = patterns[it.first];
    for (size_t i = 0; i < pattern.second; i++) {
      predicateCounts[predicateGlobalIds[pattern.first[i]]] += it.second;
    }
  }
  result.reserve(predicateCounts.size());
  for (const auto& it : predicateCounts) {
    result.push_back({it.first, static_cast<Id>(it.second)});
  }
  *dynResult = result.moveToDynamic();
}

template <int WIDTH, typename PredicateId>
void CountAvailablePredicates::computePatternTrick(
    const IdTable& dynInput, IdTable* dynResult,
    const vector<PatternID>& hasPattern,
    const CompactStringVector<size_t, PredicateId>& patterns,
    const std::vector<Id>& predicateGlobalIds, const size_t subjectColumn,
    RuntimeInformation* runtimeInfo) {
  const IdTableView<WIDTH> input = dynInput.asStaticView<WIDTH>();
  IdTableStatic<2> result = dynResult->moveToStatic<2>();
  LOG(DEBUG) << "For " << input.size() << " entities in column "
             << subjectColumn << std::endl;

  ad_utility::HashMap<Id, size_t> predicateCounts;
  ad_utility::HashMap<size_t, size_t> patternCounts;
  size_t inputIdx = 0;
  // These variables are used to gather additional statistics
  size_t numEntitiesWithPatterns = 0;
  // the number of distinct predicates in patterns
  size_t numPatternPredicates = 0;
  // the number of predicates counted without patterns
  size_t numListPredicates = 0;
  Id lastSubject = ID_NO_VALUE;
  while (inputIdx < input.size()) {
    // Skip over elements with the same subject (don't count them twice)
    Id subject = input(inputIdx, subjectColumn);
    if (subject == lastSubject) {
      inputIdx++;
      continue;
    }
    lastSubject = subject;
    if (subject < hasPattern.size() && hasPattern[subject] != NO_PATTERN) {
      // The subject matches a pattern
      patternCounts[hasPattern[subject]]++;
      numEntitiesWithPatterns++;
    } else {
      LOG(TRACE) << "Subject " << subject
                 << " does not appear to be an entity "
                    "(its id is to high)."
                 << std::endl;
    }
    inputIdx++;
  }
  LOG(DEBUG) << "Using " << patternCounts.size()
             << " patterns for computing the result." << std::endl;
  // the number of predicates counted with patterns
  size_t numPredicatesSubsumedInPatterns = 0;
  // resolve the patterns to predicate counts
  for (const auto& it : patternCounts) {
    const auto& pattern = patterns[it.first];
    numPatternPredicates += pattern.second;
    for (size_t i = 0; i < pattern.second; i++) {
      predicateCounts[predicateGlobalIds[pattern.first[i]]] += it.second;
      numPredicatesSubsumedInPatterns += it.second;
    }
  }
  // write the predicate counts to the result
  result.reserve(predicateCounts.size());
  for (const auto& it : predicateCounts) {
    result.push_back({it.first, static_cast<Id>(it.second)});
  }

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
