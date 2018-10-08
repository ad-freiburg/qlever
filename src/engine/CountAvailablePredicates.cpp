// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@neptun.uni-freiburg.de)

#include "CountAvailablePredicates.h"

// _____________________________________________________________________________
CountAvailablePredicates::CountAvailablePredicates(QueryExecutionContext* qec)
    : Operation(qec),
      _subtree(nullptr),
      _subjectColumnIndex(0),
      _predicateVarName("predicate"),
      _countVarName("count") {}

// _____________________________________________________________________________
CountAvailablePredicates::CountAvailablePredicates(
    QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> subtree,
    size_t subjectColumnIndex)
    : Operation(qec),
      _subtree(subtree),
      _subjectColumnIndex(subjectColumnIndex),
      _predicateVarName("predicate"),
      _countVarName("count") {}

// _____________________________________________________________________________
string CountAvailablePredicates::asString(size_t indent) const {
  std::ostringstream os;
  for (size_t i = 0; i < indent; ++i) {
    os << " ";
  }
  if (_subtree != nullptr) {
    os << "COUNT_AVAILABLE_PREDICATES (col " << _subjectColumnIndex << ")\n"
       << _subtree->asString(indent);
  } else {
    os << "COUNT_AVAILABLE_PREDICATES for all entities.";
  }
  return os.str();
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
std::unordered_map<string, size_t>
CountAvailablePredicates::getVariableColumns() const {
  std::unordered_map<string, size_t> varCols;
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

// _____________________________________________________________________________
void CountAvailablePredicates::computeResult(ResultTable* result) const {
  result->_nofColumns = 2;
  result->_sortedBy = resultSortedOn();
  result->_fixedSizeData = new vector<array<Id, 2>>();
  result->_resultTypes.push_back(ResultTable::ResultType::KB);
  result->_resultTypes.push_back(ResultTable::ResultType::VERBATIM);

  const std::vector<PatternID>& hasPattern =
      _executionContext->getIndex().getHasPattern();
  const CompactStringVector<Id, Id>& hasPredicate =
      _executionContext->getIndex().getHasPredicate();
  const CompactStringVector<size_t, Id>& patterns =
      _executionContext->getIndex().getPatterns();

  if (_subtree == nullptr) {
    // Compute the predicates for all entities
    CountAvailablePredicates::computePatternTrickAllEntities(
        static_cast<vector<array<Id, 2>>*>(result->_fixedSizeData), hasPattern,
        hasPredicate, patterns);
  } else {
    // Compute the predicates for entities in subresult's _subjectColumnIndex
    // column.
    std::shared_ptr<const ResultTable> subresult = _subtree->getResult();
    if (result->_nofColumns > 5) {
      CountAvailablePredicates::computePatternTrick<vector<Id>>(
          &subresult->_varSizeData,
          static_cast<vector<array<Id, 2>>*>(result->_fixedSizeData),
          hasPattern, hasPredicate, patterns, _subjectColumnIndex);
    } else {
      if (subresult->_nofColumns == 1) {
        CountAvailablePredicates::computePatternTrick<array<Id, 1>>(
            static_cast<vector<array<Id, 1>>*>(subresult->_fixedSizeData),
            static_cast<vector<array<Id, 2>>*>(result->_fixedSizeData),
            hasPattern, hasPredicate, patterns, _subjectColumnIndex);
      } else if (subresult->_nofColumns == 2) {
        CountAvailablePredicates::computePatternTrick<array<Id, 2>>(
            static_cast<vector<array<Id, 2>>*>(subresult->_fixedSizeData),
            static_cast<vector<array<Id, 2>>*>(result->_fixedSizeData),
            hasPattern, hasPredicate, patterns, _subjectColumnIndex);
      } else if (subresult->_nofColumns == 3) {
        CountAvailablePredicates::computePatternTrick<array<Id, 3>>(
            static_cast<vector<array<Id, 3>>*>(subresult->_fixedSizeData),
            static_cast<vector<array<Id, 2>>*>(result->_fixedSizeData),
            hasPattern, hasPredicate, patterns, _subjectColumnIndex);
      } else if (subresult->_nofColumns == 4) {
        CountAvailablePredicates::computePatternTrick<array<Id, 4>>(
            static_cast<vector<array<Id, 4>>*>(subresult->_fixedSizeData),
            static_cast<vector<array<Id, 2>>*>(result->_fixedSizeData),
            hasPattern, hasPredicate, patterns, _subjectColumnIndex);
      } else if (subresult->_nofColumns == 5) {
        CountAvailablePredicates::computePatternTrick<array<Id, 5>>(
            static_cast<vector<array<Id, 5>>*>(subresult->_fixedSizeData),
            static_cast<vector<array<Id, 2>>*>(result->_fixedSizeData),
            hasPattern, hasPredicate, patterns, _subjectColumnIndex);
      }
    }
  }
  result->finish();
}

void CountAvailablePredicates::computePatternTrickAllEntities(
    vector<array<Id, 2>>* result, const vector<PatternID>& hasPattern,
    const CompactStringVector<Id, Id>& hasPredicate,
    const CompactStringVector<size_t, Id>& patterns) {
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

  for (const auto& it : patternCounts) {
    std::pair<Id*, size_t> pattern = patterns[it.first];
    for (size_t i = 0; i < pattern.second; i++) {
      predicateCounts[pattern.first[i]] += it.second;
    }
  }
  result->reserve(predicateCounts.size());
  for (const auto& it : predicateCounts) {
    result->push_back(array<Id, 2>{it.first, static_cast<Id>(it.second)});
  }
}

template <typename A>
void CountAvailablePredicates::computePatternTrick(
    const vector<A>* input, vector<array<Id, 2>>* result,
    const vector<PatternID>& hasPattern,
    const CompactStringVector<Id, Id>& hasPredicate,
    const CompactStringVector<size_t, Id>& patterns,
    const size_t subjectColumn) {
  ad_utility::HashMap<Id, size_t> predicateCounts;
  ad_utility::HashMap<size_t, size_t> patternCounts;
  size_t posInput = 0;
  Id lastSubject = ID_NO_VALUE;
  while (posInput < input->size()) {
    while ((*input)[posInput][subjectColumn] == lastSubject &&
           posInput < input->size()) {
      posInput++;
    }
    size_t subject = (*input)[posInput][subjectColumn];
    lastSubject = subject;
    if (subject < hasPattern.size() && hasPattern[subject] != NO_PATTERN) {
      // The subject matches a pattern
      patternCounts[hasPattern[subject]]++;
    } else if (subject < hasPredicate.size()) {
      // The subject does not match a pattern
      size_t numPredicates;
      Id* predicateData;
      std::tie(predicateData, numPredicates) = hasPredicate[subject];
      if (numPredicates > 0) {
        for (size_t i = 0; i < numPredicates; i++) {
          auto it = predicateCounts.find(predicateData[i]);
          if (it == predicateCounts.end()) {
            predicateCounts[predicateData[i]] = 1;
          } else {
            it->second++;
          }
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
    posInput++;
  }
  for (const auto& it : patternCounts) {
    std::pair<Id*, size_t> pattern = patterns[it.first];
    for (size_t i = 0; i < pattern.second; i++) {
      predicateCounts[pattern.first[i]] += it.second;
    }
  }
  result->reserve(predicateCounts.size());
  for (const auto& it : predicateCounts) {
    result->push_back(array<Id, 2>{it.first, static_cast<Id>(it.second)});
  }
}
