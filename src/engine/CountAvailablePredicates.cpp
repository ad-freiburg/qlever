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
    if (subresult->_nofColumns > 5) {
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
#ifndef DISABLE_PATTERN_TRICK_STATISTICS
  // These variables are used to gather additional statistics
  size_t num_entities_with_pattern = 0;
  // the number of predicates counted with patterns
  size_t predicates_from_patterns = 0;
  // the number of predicates counted without patterns
  size_t predicates_from_lists = 0;
#endif
  Id lastSubject = ID_NO_VALUE;
  while (posInput < input->size()) {
    // Skip over elements with the same subject (don't count them twice)
    if ((*input)[posInput][subjectColumn] == lastSubject) {
      posInput++;
      continue;
    }
    size_t subject = (*input)[posInput][subjectColumn];
    lastSubject = subject;
    if (subject < hasPattern.size() && hasPattern[subject] != NO_PATTERN) {
      // The subject matches a pattern
      patternCounts[hasPattern[subject]]++;
#ifndef DISABLE_PATTERN_TRICK_STATISTICS
      num_entities_with_pattern++;
#endif
    } else if (subject < hasPredicate.size()) {
      // The subject does not match a pattern
      size_t numPredicates;
      Id* predicateData;
      std::tie(predicateData, numPredicates) = hasPredicate[subject];
#ifndef DISABLE_PATTERN_TRICK_STATISTICS
      predicates_from_lists += numPredicates;
#endif
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
  // resolve the patterns to predicate counts
  for (const auto& it : patternCounts) {
    std::pair<Id*, size_t> pattern = patterns[it.first];
    predicates_from_patterns += it.second;
    for (size_t i = 0; i < pattern.second; i++) {
      predicateCounts[pattern.first[i]] += it.second;
    }
  }
  // write the predicate counts to the result
  result->reserve(predicateCounts.size());
  for (const auto& it : predicateCounts) {
    result->push_back(array<Id, 2>{it.first, static_cast<Id>(it.second)});
  }

#ifndef DISABLE_PATTERN_TRICK_STATISTICS
  // Print interesting statistics about the pattern trick
  double ratio_has_pattern =
      static_cast<double>(num_entities_with_pattern) / input->size();
  size_t num_predicates_total =
      predicates_from_lists + predicates_from_patterns;
  double ratio_counted_with_pattern =
      static_cast<double>(predicates_from_patterns) / num_predicates_total;

  size_t cost_with_patterns =
      input->size() + predicates_from_lists + patternCounts.size();
  size_t cost_without_patterns = input->size() + num_predicates_total;
  double cost_ratio =
      static_cast<double>(cost_with_patterns) / cost_without_patterns;
  // Print the ratio of entities that used a pattern
  LOG(DEBUG) << num_entities_with_pattern << " of " << input->size()
             << " entities had a pattern. That equals "
             << (ratio_has_pattern * 100) << "%" << std::endl;
  // Print info about how many predicates where counted with patterns
  LOG(DEBUG) << "Of the " << num_predicates_total << " predicates "
             << predicates_from_patterns
             << " were counted using patterns while " << predicates_from_lists
             << " were counted without patterns. That equals "
             << (ratio_counted_with_pattern * 100) << "%" << std::endl;
  // Print information about of efficient the pattern trick is
  LOG(DEBUG) << "The conceptual cost of the operation with patterns was "
             << cost_with_patterns
             << " while without patterns it would have been "
             << cost_without_patterns << std::endl;
  // Print the cost improvement using the the pattern trick gave us
  LOG(DEBUG) << "This equals a ratio of cost with to cost without patterns of "
             << cost_ratio << std::endl;
#endif
}
