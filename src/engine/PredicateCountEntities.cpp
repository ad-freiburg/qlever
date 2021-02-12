// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@neptun.uni-freiburg.de)

#include "PredicateCountEntities.h"
#include "CallFixedSize.h"

// _____________________________________________________________________________
PredicateCountEntities::PredicateCountEntities(QueryExecutionContext* qec)
    : Operation(qec),
      _subtree(nullptr),
      _subjectColumnIndex(0),
      _subjectEntityName(),
      _predicateVarName("predicate"),
      _countVarName("count"),
      _count_for(CountType::SUBJECT) {}

// _____________________________________________________________________________
PredicateCountEntities::PredicateCountEntities(
    QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> subtree,
    size_t subjectColumnIndex)
    : Operation(qec),
      _subtree(subtree),
      _subjectColumnIndex(subjectColumnIndex),
      _subjectEntityName(),
      _predicateVarName("predicate"),
      _countVarName("count"),
      _count_for(CountType::SUBJECT) {}

PredicateCountEntities::PredicateCountEntities(QueryExecutionContext* qec,
                                               std::string entityName)
    : Operation(qec),
      _subtree(nullptr),
      _subjectColumnIndex(0),
      _subjectEntityName(entityName),
      _predicateVarName("predicate"),
      _countVarName("count"),
      _count_for(CountType::SUBJECT) {}

// _____________________________________________________________________________
string PredicateCountEntities::asString(size_t indent) const {
  std::ostringstream os;
  for (size_t i = 0; i < indent; ++i) {
    os << " ";
  }
  const std::string entity_name =
      (_count_for == CountType::OBJECT) ? "OBJECTS" : "SUBJECTS";
  if (_subjectEntityName) {
    os << "PREDICATE_COUNT_" << entity_name << " for "
       << _subjectEntityName.value();
  } else if (_subtree == nullptr) {
    os << "PREDICATE_COUNT_" << entity_name << " for all entities";
  } else {
    os << "PREDICATE_COUNT_" << entity_name << " (col " << _subjectColumnIndex
       << ")\n"
       << _subtree->asString(indent);
  }
  return os.str();
}

// _____________________________________________________________________________
string PredicateCountEntities::getDescriptor() const {
  if (_count_for == CountType::OBJECT) {
    if (_subjectEntityName) {
      return "PredicateCountObjects for a single entity";
    } else if (_subtree == nullptr) {
      return "PredicateCountObjects for a all entities";
    }
    return "PredicateCountObjects";
  } else {
    if (_subjectEntityName) {
      return "PredicateCountSubjects for a single entity";
    } else if (_subtree == nullptr) {
      return "PredicateCountSubjects for a all entities";
    }
    return "PredicateCountSubjects";
  }
}

// _____________________________________________________________________________
size_t PredicateCountEntities::getResultWidth() const { return 2; }

// _____________________________________________________________________________
vector<size_t> PredicateCountEntities::resultSortedOn() const {
  // The result is not sorted on any column.
  return {};
}

// _____________________________________________________________________________
void PredicateCountEntities::setVarNames(const std::string& predicateVarName,
                                         const std::string& countVarName) {
  _predicateVarName = predicateVarName;
  _countVarName = countVarName;
}

// _____________________________________________________________________________
void PredicateCountEntities::setCountFor(CountType count_for) {
  _count_for = count_for;
}

// _____________________________________________________________________________
ad_utility::HashMap<string, size_t> PredicateCountEntities::getVariableColumns()
    const {
  ad_utility::HashMap<string, size_t> varCols;
  varCols[_predicateVarName] = 0;
  varCols[_countVarName] = 1;
  return varCols;
}

// _____________________________________________________________________________
float PredicateCountEntities::getMultiplicity(size_t col) {
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
size_t PredicateCountEntities::getSizeEstimate() {
  if (_subtree.get() != nullptr) {
    // Predicates are only computed for entities in the subtrees result.

    // This estimate is probably wildly innacurrate, but as it does not
    // depend on the order of operations of the subtree should be sufficient
    // for the type of optimizations the optimizer can currently do.
    size_t num_distinct = _subtree->getSizeEstimate() /
                          _subtree->getMultiplicity(_subjectColumnIndex);
    return num_distinct / getIndex()
                              .getPatternIndex()
                              .getSubjectMetaData()
                              .fullHasPredicateMultiplicityPredicates;
  } else {
    // Predicates are counted for all entities. In this case the size estimate
    // should be accurate.
    return getIndex()
               .getPatternIndex()
               .getSubjectMetaData()
               .fullHasPredicateSize /
           getIndex()
               .getPatternIndex()
               .getSubjectMetaData()
               .fullHasPredicateMultiplicityPredicates;
  }
}

// _____________________________________________________________________________
size_t PredicateCountEntities::getCostEstimate() {
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
void PredicateCountEntities::computeResult(ResultTable* result) {
  LOG(DEBUG) << "PredicateCountEntities result computation..." << std::endl;
  result->_data.setCols(2);
  result->_sortedBy = resultSortedOn();
  result->_resultTypes.push_back(ResultTable::ResultType::KB);
  result->_resultTypes.push_back(ResultTable::ResultType::VERBATIM);

  std::shared_ptr<const PatternContainer> pattern_data;
  switch (_count_for) {
    case CountType::SUBJECT:
      pattern_data = _executionContext->getIndex()
                         .getPatternIndex()
                         .getSubjectPatternData();
      break;
    case CountType::OBJECT:
      pattern_data = _executionContext->getIndex()
                         .getPatternIndex()
                         .getObjectPatternData();
      break;
  }

  if (pattern_data->predicateIdSize() <= 1) {
    computeResult(result,
                  std::static_pointer_cast<const PatternContainerImpl<uint8_t>>(
                      pattern_data));
  } else if (pattern_data->predicateIdSize() <= 2) {
    computeResult(
        result, std::static_pointer_cast<const PatternContainerImpl<uint16_t>>(
                    pattern_data));
  } else if (pattern_data->predicateIdSize() <= 4) {
    computeResult(
        result, std::static_pointer_cast<const PatternContainerImpl<uint32_t>>(
                    pattern_data));
  } else if (pattern_data->predicateIdSize() <= 8) {
    computeResult(
        result, std::static_pointer_cast<const PatternContainerImpl<uint64_t>>(
                    pattern_data));
  } else {
    AD_THROW(ad_semsearch::Exception::BAD_INPUT,
             "The index contains more than 2**64 predicates.");
  }

  LOG(DEBUG) << "PredicateCountEntities result computation done." << std::endl;
}

template <typename PredicateId>
void PredicateCountEntities::computeResult(
    ResultTable* result,
    std::shared_ptr<const PatternContainerImpl<PredicateId>> pattern_data) {
  RuntimeInformation& runtimeInfo = getRuntimeInfo();

  if (_subjectEntityName) {
    size_t entityId;
    // If the entity exists return the all predicates for that entitity,
    // otherwise return an empty result.
    if (getIndex().getVocab().getId(_subjectEntityName.value(), &entityId)) {
      IdTable input(1);
      input.push_back({entityId});
      int width = input.cols();
      CALL_FIXED_SIZE_1(width, PredicateCountEntities::computePatternTrick,
                        input, &result->_data, pattern_data->hasPattern(),
                        pattern_data->hasPredicate(), pattern_data->patterns(),
                        _executionContext->getIndex()
                            .getPatternIndex()
                            .getPredicateGlobalIds(),
                        0, &runtimeInfo);
    }
  } else if (_subtree == nullptr) {
    // Compute the predicates for all entities
    PredicateCountEntities::computePatternTrickAllEntities(
        &result->_data, pattern_data->hasPattern(),
        pattern_data->hasPredicate(), pattern_data->patterns(),

        _executionContext->getIndex()
            .getPatternIndex()
            .getPredicateGlobalIds());
  } else {
    std::shared_ptr<const ResultTable> subresult = _subtree->getResult();
    runtimeInfo.addChild(_subtree->getRootOperation()->getRuntimeInfo());
    LOG(DEBUG) << "PredicateCountEntities subresult computation done."
               << std::endl;

    int width = subresult->_data.cols();
    CALL_FIXED_SIZE_1(
        width, PredicateCountEntities::computePatternTrick, subresult->_data,
        &result->_data, pattern_data->hasPattern(),
        pattern_data->hasPredicate(), pattern_data->patterns(),
        _executionContext->getIndex().getPatternIndex().getPredicateGlobalIds(),
        _subjectColumnIndex, &runtimeInfo);
  }
  LOG(DEBUG) << "PredicateCountEntities result computation done." << std::endl;
}

template <typename PredicateId>
void PredicateCountEntities::computePatternTrickAllEntities(
    IdTable* dynResult, const vector<PatternID>& hasPattern,
    const CompactStringVector<Id, PredicateId>& hasPredicate,
    const CompactStringVector<size_t, PredicateId>& patterns,
    const std::vector<Id>& predicateGlobalIds) {
  IdTableStatic<2> result = dynResult->moveToStatic<2>();
  LOG(DEBUG) << "For all entities." << std::endl;
  std::vector<size_t> predicateCounts(predicateGlobalIds.size(), 0);

  // Every pattern will be counted at least once
  std::vector<size_t> patternCounts(patterns.size(), 0);

  size_t maxId = std::max(hasPattern.size(), hasPredicate.size());
  for (size_t i = 0; i < maxId; i++) {
    if (i < hasPattern.size() && hasPattern[i] != NO_PATTERN) {
      patternCounts[hasPattern[i]]++;
    } else if (i < hasPredicate.size()) {
      size_t numPredicates;
      PredicateId* predicateData;
      std::tie(predicateData, numPredicates) = hasPredicate[i];
      if (numPredicates > 0) {
        for (size_t i = 0; i < numPredicates; i++) {
          Id predicate = predicateData[i];
          predicateCounts[predicate]++;
        }
      }
    }
  }

  LOG(DEBUG) << "Using " << patternCounts.size()
             << " patterns for computing the result." << std::endl;
  for (size_t pattern_id = 0; pattern_id < patternCounts.size(); ++pattern_id) {
    std::pair<PredicateId*, size_t> pattern = patterns[pattern_id];
    for (size_t i = 0; i < pattern.second; i++) {
      predicateCounts[pattern.first[i]] += patternCounts[pattern_id];
    }
  }
  result.reserve(predicateCounts.size());
  for (size_t predicate_local_id = 0;
       predicate_local_id < predicateCounts.size(); ++predicate_local_id) {
    result.push_back({predicateGlobalIds[predicate_local_id],
                      predicateCounts[predicate_local_id]});
  }
  *dynResult = result.moveToDynamic();
}

template <int WIDTH, typename PredicateId>
void PredicateCountEntities::computePatternTrick(
    const IdTable& dynInput, IdTable* dynResult,
    const vector<PatternID>& hasPattern,
    const CompactStringVector<Id, PredicateId>& hasPredicate,
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
    } else if (subject < hasPredicate.size()) {
      // The subject does not match a pattern
      size_t numPredicates;
      PredicateId* predicateData;
      std::tie(predicateData, numPredicates) = hasPredicate[subject];
      numListPredicates += numPredicates;
      if (numPredicates > 0) {
        for (size_t i = 0; i < numPredicates; i++) {
          Id predicate = predicateGlobalIds[predicateData[i]];
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
    inputIdx++;
  }
  LOG(DEBUG) << "Using " << patternCounts.size()
             << " patterns for computing the result." << std::endl;
  // the number of predicates counted with patterns
  size_t numPredicatesSubsumedInPatterns = 0;
  // resolve the patterns to predicate counts
  for (const auto& it : patternCounts) {
    std::pair<PredicateId*, size_t> pattern = patterns[it.first];
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
