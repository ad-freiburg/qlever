// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@neptun.uni-freiburg.de)

#include "EntityCountPredicates.h"

#include "CallFixedSize.h"

// _____________________________________________________________________________
EntityCountPredicates::EntityCountPredicates(QueryExecutionContext* qec)
    : Operation(qec),
      _subtree(nullptr),
      _subjectColumnIndex(0),
      _subjectEntityName(),
      _predicateVarName("predicate"),
      _countVarName("count"),
      _count_for(CountType::SUBJECT) {}

// _____________________________________________________________________________
EntityCountPredicates::EntityCountPredicates(
    QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> subtree,
    size_t subjectColumnIndex)
    : Operation(qec),
      _subtree(subtree),
      _subjectColumnIndex(subjectColumnIndex),
      _subjectEntityName(),
      _predicateVarName("predicate"),
      _countVarName("count"),
      _count_for(CountType::SUBJECT) {}

EntityCountPredicates::EntityCountPredicates(QueryExecutionContext* qec,
                                             std::string entityName)
    : Operation(qec),
      _subtree(nullptr),
      _subjectColumnIndex(0),
      _subjectEntityName(entityName),
      _predicateVarName("predicate"),
      _countVarName("count"),
      _count_for(CountType::SUBJECT) {}

// _____________________________________________________________________________
string EntityCountPredicates::asString(size_t indent) const {
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
string EntityCountPredicates::getDescriptor() const {
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
size_t EntityCountPredicates::getResultWidth() const { return 2; }

// _____________________________________________________________________________
vector<size_t> EntityCountPredicates::resultSortedOn() const {
  // The result is not sorted on any column.
  return {};
}

// _____________________________________________________________________________
void EntityCountPredicates::setVarNames(const std::string& predicateVarName,
                                        const std::string& countVarName) {
  _predicateVarName = predicateVarName;
  _countVarName = countVarName;
}

// _____________________________________________________________________________
void EntityCountPredicates::setCountFor(CountType count_for) {
  _count_for = count_for;
}

// _____________________________________________________________________________
ad_utility::HashMap<string, size_t> EntityCountPredicates::getVariableColumns()
    const {
  ad_utility::HashMap<string, size_t> varCols;
  varCols[_predicateVarName] = 0;
  varCols[_countVarName] = 1;
  return varCols;
}

// _____________________________________________________________________________
float EntityCountPredicates::getMultiplicity(size_t col) {
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
size_t EntityCountPredicates::getSizeEstimate() {
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
size_t EntityCountPredicates::getCostEstimate() {
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
void EntityCountPredicates::computeResult(ResultTable* result) {
  LOG(DEBUG) << "EntityCountPredicates result computation..." << std::endl;
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

  LOG(DEBUG) << "EntityCountPredicates result computation done." << std::endl;
}

template <typename PredicateId>
void EntityCountPredicates::computeResult(
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
      CALL_FIXED_SIZE_1(width, EntityCountPredicates::compute, input,
                        &result->_data, pattern_data->hasPattern(),
                        pattern_data->hasPredicate(), pattern_data->patterns(),
                        0);
    }
  } else if (_subtree == nullptr) {
    // Compute the predicates for all entities
    EntityCountPredicates::computeAllEntities(
        &result->_data, pattern_data->hasPattern(),
        pattern_data->hasPredicate(), pattern_data->patterns());
  } else {
    std::shared_ptr<const ResultTable> subresult = _subtree->getResult();
    runtimeInfo.addChild(_subtree->getRootOperation()->getRuntimeInfo());
    LOG(DEBUG) << "EntityCountPredicates subresult computation done."
               << std::endl;

    int width = subresult->_data.cols();
    CALL_FIXED_SIZE_1(width, EntityCountPredicates::compute, subresult->_data,
                      &result->_data, pattern_data->hasPattern(),
                      pattern_data->hasPredicate(), pattern_data->patterns(),
                      _subjectColumnIndex);
  }
  LOG(DEBUG) << "EntityCountPredicates result computation done." << std::endl;
}

template <typename PredicateId>
void EntityCountPredicates::computeAllEntities(
    IdTable* dynResult, const vector<PatternID>& hasPattern,
    const CompactStringVector<Id, PredicateId>& hasPredicate,
    const CompactStringVector<size_t, PredicateId>& patterns) {
  IdTableStatic<2> result = dynResult->moveToStatic<2>();
  LOG(DEBUG) << "EntityCountPredicates For all entities." << std::endl;

  size_t maxId = std::max(hasPattern.size(), hasPredicate.size());
  result.reserve(maxId);
  for (size_t i = 0; i < maxId; i++) {
    if (i < hasPattern.size() && hasPattern[i] != NO_PATTERN) {
      result.push_back({i, patterns[hasPattern[i]].second});
    } else if (i < hasPredicate.size()) {
      result.push_back({i, hasPredicate[i].second});
    }
  }
  *dynResult = result.moveToDynamic();
}

template <int WIDTH, typename PredicateId>
void EntityCountPredicates::compute(
    const IdTable& dynInput, IdTable* dynResult,
    const vector<PatternID>& hasPattern,
    const CompactStringVector<Id, PredicateId>& hasPredicate,
    const CompactStringVector<size_t, PredicateId>& patterns,
    const size_t subjectColumn) {
  const IdTableView<WIDTH> input = dynInput.asStaticView<WIDTH>();
  IdTableStatic<2> result = dynResult->moveToStatic<2>();
  LOG(DEBUG) << "For " << input.size() << " entities in column "
             << subjectColumn << std::endl;

  size_t inputIdx = 0;
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
      result.push_back({subject, patterns[hasPattern[subject]].second});
    } else if (subject < hasPredicate.size()) {
      // The subject does not match a pattern
      result.push_back({subject, hasPredicate[subject].second});
    } else {
      LOG(TRACE) << "Subject " << subject
                 << " does not appear to be an entity "
                    "(its id is to high)."
                 << std::endl;
    }
    inputIdx++;
  }
  *dynResult = result.moveToDynamic();
}
