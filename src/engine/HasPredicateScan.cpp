// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)

#include "HasPredicateScan.h"

#include "CallFixedSize.h"

template <typename A, typename R>
void doComputeSubqueryS(const std::vector<A>* input,
                        const size_t inputSubjectColumn, std::vector<R>* result,
                        const std::vector<PatternID>& hasPattern,
                        const CompactStringVector<size_t, Id>& patterns);

HasPredicateScan::HasPredicateScan(QueryExecutionContext* qec, ScanType type)
    : Operation(qec),
      _type(type),
      _subtree(nullptr),
      _subtreeColIndex(-1),
      _subject(),
      _object() {}

string HasPredicateScan::asString(size_t indent) const {
  std::ostringstream os;
  for (size_t i = 0; i < indent; ++i) {
    os << " ";
  }
  switch (_type) {
    case ScanType::FREE_S:
      os << "HAS_RELATION_SCAN with O = " << _object;
      break;
    case ScanType::FREE_O:
      os << "HAS_RELATION_SCAN with S = " << _subject;
      break;
    case ScanType::FULL_SCAN:
      os << "HAS_RELATION_SCAN for the full relation";
      break;
    case ScanType::SUBQUERY_S:
      os << "HAS_RELATION_SCAN with S = " << _subtree->asString(indent);
      break;
  }
  return os.str();
}

string HasPredicateScan::getDescriptor() const {
  switch (_type) {
    case ScanType::FREE_S:
      return "HasPredicateScan free subject: " + _subject;
    case ScanType::FREE_O:
      return "HasPredicateScan free object: " + _object;
    case ScanType::FULL_SCAN:
      return "HasPredicateScan full scan";
    case ScanType::SUBQUERY_S:
      return "HasPredicateScan with a subquery on " + _subject;
    default:
      return "HasPredicateScan";
  }
}

size_t HasPredicateScan::getResultWidth() const {
  switch (_type) {
    case ScanType::FREE_S:
      return 1;
    case ScanType::FREE_O:
      return 1;
    case ScanType::FULL_SCAN:
      return 2;
    case ScanType::SUBQUERY_S:
      return _subtree->getResultWidth() + 1;
  }
  return -1;
}

vector<size_t> HasPredicateScan::resultSortedOn() const {
  switch (_type) {
    case ScanType::FREE_S:
      // is the lack of sorting here a problem?
      return {};
    case ScanType::FREE_O:
      return {0};
    case ScanType::FULL_SCAN:
      return {0};
    case ScanType::SUBQUERY_S:
      return _subtree->resultSortedOn();
  }
  return {};
}

ad_utility::HashMap<string, size_t> HasPredicateScan::getVariableColumns()
    const {
  ad_utility::HashMap<string, size_t> varCols;
  switch (_type) {
    case ScanType::FREE_S:
      varCols.insert(std::make_pair(_subject, 0));
      break;
    case ScanType::FREE_O:
      varCols.insert(std::make_pair(_object, 0));
      break;
    case ScanType::FULL_SCAN:
      varCols.insert(std::make_pair(_subject, 0));
      varCols.insert(std::make_pair(_object, 1));
      break;
    case ScanType::SUBQUERY_S:
      varCols = _subtree->getVariableColumns();
      varCols.insert(std::make_pair(_object, getResultWidth() - 1));
      break;
  }
  return varCols;
}

void HasPredicateScan::setTextLimit(size_t limit) {
  if (_type == ScanType::SUBQUERY_S) {
    _subtree->setTextLimit(limit);
  }
}

bool HasPredicateScan::knownEmptyResult() {
  if (_type == ScanType::SUBQUERY_S) {
    return _subtree->knownEmptyResult();
  } else {
    return false;
  }
}

float HasPredicateScan::getMultiplicity(size_t col) {
  const auto& patternIndex = getIndex().getPatternIndex();
  switch (_type) {
    case ScanType::FREE_S:
      if (col == 0) {
        return patternIndex.getHasPredicateMultiplicityEntities();
      }
      break;
    case ScanType::FREE_O:
      if (col == 0) {
        return patternIndex.getHasPredicateMultiplicityPredicates();
      }
      break;
    case ScanType::FULL_SCAN:
      if (col == 0) {
        return patternIndex.getHasPredicateMultiplicityEntities();
      } else if (col == 1) {
        return patternIndex.getHasPredicateMultiplicityPredicates();
      }
      break;
    case ScanType::SUBQUERY_S:
      if (col < getResultWidth() - 1) {
        return _subtree->getMultiplicity(col) *
               patternIndex.getHasPredicateMultiplicityPredicates();
      } else {
        return _subtree->getMultiplicity(_subtreeColIndex) *
               patternIndex.getHasPredicateMultiplicityPredicates();
      }
      break;
  }
  return 1;
}

size_t HasPredicateScan::getSizeEstimate() {
  const auto& patternIndex = getIndex().getPatternIndex();
  switch (_type) {
    case ScanType::FREE_S:
      return static_cast<size_t>(
          patternIndex.getHasPredicateMultiplicityEntities());
    case ScanType::FREE_O:
      return static_cast<size_t>(
          patternIndex.getHasPredicateMultiplicityPredicates());
    case ScanType::FULL_SCAN:
      return patternIndex.getHasPredicateFullSize();
    case ScanType::SUBQUERY_S:

      size_t nofDistinctLeft = std::max(
          size_t(1),
          static_cast<size_t>(_subtree->getSizeEstimate() /
                              _subtree->getMultiplicity(_subtreeColIndex)));
      size_t nofDistinctRight = std::max(
          size_t(1), static_cast<size_t>(
                         patternIndex.getHasPredicateFullSize() /
                         patternIndex.getHasPredicateMultiplicityPredicates()));
      size_t nofDistinctInResult = std::min(nofDistinctLeft, nofDistinctRight);

      double jcMultiplicityInResult =
          _subtree->getMultiplicity(_subtreeColIndex) *
          patternIndex.getHasPredicateMultiplicityPredicates();
      return std::max(size_t(1), static_cast<size_t>(jcMultiplicityInResult *
                                                     nofDistinctInResult));
  }
  return 0;
}

size_t HasPredicateScan::getCostEstimate() {
  // TODO: these size estimates only work if all predicates are functional
  switch (_type) {
    case ScanType::FREE_S:
      return getSizeEstimate();
    case ScanType::FREE_O:
      return getSizeEstimate();
    case ScanType::FULL_SCAN:
      return getSizeEstimate();
    case ScanType::SUBQUERY_S:
      return _subtree->getCostEstimate() + getSizeEstimate();
  }
  return 0;
}

void HasPredicateScan::computeResult(ResultTable* result) {
  LOG(DEBUG) << "HasPredicateScan result computation..." << std::endl;
  result->_data.setCols(getResultWidth());
  result->_sortedBy = resultSortedOn();

  auto visitor = [&](const auto& patternImpl) {
    computeResult(result, patternImpl);
  };

  std::visit(visitor,
             _executionContext->getIndex().getPatternIndex().getPatternData());

  LOG(DEBUG) << "HasPredicateScan result compuation done." << std::endl;
}

template <typename PredicateId>
void HasPredicateScan::computeResult(
    ResultTable* result,
    const PatternContainerImpl<PredicateId>& pattern_data) {
  RuntimeInformation& runtimeInfo = getRuntimeInfo();

  const auto& hasPattern = pattern_data.hasPattern();
  const auto& patterns = pattern_data.patterns();
  const auto& predicateGlobalIds =
      _executionContext->getIndex().getPatternIndex().getPredicateGlobalIds();

  switch (_type) {
    case ScanType::FREE_S: {
      runtimeInfo.setDescriptor("HasPredicateScan free subject: " + _subject);
      Id objectId;
      if (!getIndex().getVocab().getId(_object, &objectId)) {
        AD_THROW(ad_semsearch::Exception::BAD_INPUT,
                 "The predicate '" + _object + "' is not in the vocabulary.");
      }
      HasPredicateScan::computeFreeS(result, objectId, hasPattern, patterns,
                                     predicateGlobalIds);
    } break;
    case ScanType::FREE_O: {
      runtimeInfo.setDescriptor("HasPredicateScan free object: " + _object);
      Id subjectId;
      if (!getIndex().getVocab().getId(_subject, &subjectId)) {
        AD_THROW(ad_semsearch::Exception::BAD_INPUT,
                 "The subject " + _subject + " is not in the vocabulary.");
      }
      HasPredicateScan::computeFreeO(result, subjectId, hasPattern, patterns,
                                     predicateGlobalIds);
    } break;
    case ScanType::FULL_SCAN:
      runtimeInfo.setDescriptor("HasPredicateScan full scan");
      HasPredicateScan::computeFullScan(
          result, hasPattern, patterns, predicateGlobalIds,
          getIndex().getPatternIndex().getHasPredicateFullSize());
      break;
    case ScanType::SUBQUERY_S:

      std::shared_ptr<const ResultTable> subresult = _subtree->getResult();
      result->_resultTypes.insert(result->_resultTypes.begin(),
                                  subresult->_resultTypes.begin(),
                                  subresult->_resultTypes.end());
      result->_resultTypes.push_back(ResultTable::ResultType::KB);
      int inWidth = subresult->_data.cols();
      int outWidth = result->_data.cols();
      CALL_FIXED_SIZE_2(inWidth, outWidth, HasPredicateScan::computeSubqueryS,
                        &result->_data, subresult->_data, _subtreeColIndex,
                        hasPattern, patterns, predicateGlobalIds);
      runtimeInfo.setDescriptor("HasPredicateScan with a subquery on " +
                                _subject);
      runtimeInfo.addChild(_subtree->getRootOperation()->getRuntimeInfo());
      break;
  }
}

template <typename PredicateId>
void HasPredicateScan::computeFreeS(
    ResultTable* resultTable, size_t objectId,
    const std::vector<PatternID>& hasPattern,
    const CompactStringVector<size_t, PredicateId>& patterns,
    const std::vector<Id>& predicateGlobalIds) {
  IdTableStatic<1> result = resultTable->_data.moveToStatic<1>();
  resultTable->_resultTypes.push_back(ResultTable::ResultType::KB);
  Id id = 0;
  while (id < hasPattern.size()) {
    if (id < hasPattern.size() && hasPattern[id] != NO_PATTERN) {
      // add the pattern
      const auto& [patternData, numPredicates] = patterns[hasPattern[id]];
      for (size_t i = 0; i < numPredicates; i++) {
        if (predicateGlobalIds[patternData[i]] == objectId) {
          result.push_back({id});
        }
      }
      id++;
    }
  }
  resultTable->_data = result.moveToDynamic();
}

template <typename PredicateId>
void HasPredicateScan::computeFreeO(
    ResultTable* resultTable, size_t subjectId,
    const std::vector<PatternID>& hasPattern,
    const CompactStringVector<size_t, PredicateId>& patterns,
    const std::vector<Id>& predicateGlobalIds) {
  IdTableStatic<1> result = resultTable->_data.moveToStatic<1>();
  resultTable->_resultTypes.push_back(ResultTable::ResultType::KB);

  if (subjectId < hasPattern.size() && hasPattern[subjectId] != NO_PATTERN) {
    // add the pattern
    const auto& [patternData, numPredicates] = patterns[hasPattern[subjectId]];
    for (size_t i = 0; i < numPredicates; i++) {
      result.push_back({predicateGlobalIds[patternData[i]]});
    }
  }
  resultTable->_data = result.moveToDynamic();
}

template <typename PredicateId>
void HasPredicateScan::computeFullScan(
    ResultTable* resultTable, const std::vector<PatternID>& hasPattern,
    const CompactStringVector<size_t, PredicateId>& patterns,
    const std::vector<Id>& predicateGlobalIds, size_t resultSize) {
  resultTable->_resultTypes.push_back(ResultTable::ResultType::KB);
  resultTable->_resultTypes.push_back(ResultTable::ResultType::KB);
  IdTableStatic<2> result = resultTable->_data.moveToStatic<2>();
  result.reserve(resultSize);

  size_t id = 0;
  while (id < hasPattern.size()) {
    if (id < hasPattern.size() && hasPattern[id] != NO_PATTERN) {
      // add the pattern
      const auto& [patternData, numPredicates] = patterns[hasPattern[id]];
      for (size_t i = 0; i < numPredicates; i++) {
        result.push_back({id, predicateGlobalIds[patternData[i]]});
      }
    }
    id++;
  }
  resultTable->_data = result.moveToDynamic();
}

template <int IN_WIDTH, int OUT_WIDTH, typename PredicateId>
void HasPredicateScan::computeSubqueryS(
    IdTable* dynResult, const IdTable& dynInput, const size_t subtreeColIndex,
    const std::vector<PatternID>& hasPattern,
    const CompactStringVector<size_t, PredicateId>& patterns,
    const std::vector<Id>& predicateGlobalIds) {
  IdTableStatic<OUT_WIDTH> result = dynResult->moveToStatic<OUT_WIDTH>();
  const IdTableView<IN_WIDTH> input = dynInput.asStaticView<IN_WIDTH>();

  LOG(DEBUG) << "HasPredicateScan subresult size " << input.size() << std::endl;

  for (size_t i = 0; i < input.size(); i++) {
    size_t id = input(i, subtreeColIndex);
    if (id < hasPattern.size() && hasPattern[id] != NO_PATTERN) {
      // Expand the pattern and add it to the result
      const auto& [patternData, numPredicates] = patterns[hasPattern[id]];
      for (size_t j = 0; j < numPredicates; j++) {
        result.push_back();
        size_t backIdx = result.size() - 1;
        for (size_t k = 0; k < input.cols(); k++) {
          result(backIdx, k) = input(i, k);
        }
        result(backIdx, input.cols()) = predicateGlobalIds[patternData[j]];
      }
    } else {
      break;
    }
  }
  *dynResult = result.moveToDynamic();
}

void HasPredicateScan::setSubject(const std::string& subject) {
  _subject = subject;
}

void HasPredicateScan::setObject(const std::string& object) {
  _object = object;
}

const std::string& HasPredicateScan::getObject() const { return _object; }

void HasPredicateScan::setSubtree(std::shared_ptr<QueryExecutionTree> subtree) {
  _subtree = subtree;
}

void HasPredicateScan::setSubtreeSubjectColumn(size_t colIndex) {
  _subtreeColIndex = colIndex;
}

HasPredicateScan::ScanType HasPredicateScan::getType() const { return _type; }
