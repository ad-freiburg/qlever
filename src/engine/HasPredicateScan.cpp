// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)

#include "HasPredicateScan.h"

#include "CallFixedSize.h"

template <typename A, typename R>
void doComputeSubqueryS(const std::vector<A>* input,
                        const size_t inputSubjectColumn, std::vector<R>* result,
                        const std::vector<PatternID>& hasPattern,
                        const CompactVectorOfStrings<Id>& hasPredicate,
                        const CompactVectorOfStrings<Id>& patterns);

HasPredicateScan::HasPredicateScan(QueryExecutionContext* qec, ScanType type)
    : Operation(qec),
      _type(type),
      _subtree(nullptr),
      _subtreeColIndex(-1),
      _subject(),
      _object() {}

string HasPredicateScan::asStringImpl(size_t indent) const {
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
  return std::move(os).str();
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
  switch (_type) {
    case ScanType::FREE_S:
      if (col == 0) {
        return getIndex().getHasPredicateMultiplicityEntities();
      }
      break;
    case ScanType::FREE_O:
      if (col == 0) {
        return getIndex().getHasPredicateMultiplicityPredicates();
      }
      break;
    case ScanType::FULL_SCAN:
      if (col == 0) {
        return getIndex().getHasPredicateMultiplicityEntities();
      } else if (col == 1) {
        return getIndex().getHasPredicateMultiplicityPredicates();
      }
      break;
    case ScanType::SUBQUERY_S:
      if (col < getResultWidth() - 1) {
        return _subtree->getMultiplicity(col) *
               getIndex().getHasPredicateMultiplicityPredicates();
      } else {
        return _subtree->getMultiplicity(_subtreeColIndex) *
               getIndex().getHasPredicateMultiplicityPredicates();
      }
      break;
  }
  return 1;
}

size_t HasPredicateScan::getSizeEstimate() {
  switch (_type) {
    case ScanType::FREE_S:
      return static_cast<size_t>(
          getIndex().getHasPredicateMultiplicityEntities());
    case ScanType::FREE_O:
      return static_cast<size_t>(
          getIndex().getHasPredicateMultiplicityPredicates());
    case ScanType::FULL_SCAN:
      return getIndex().getHasPredicateFullSize();
    case ScanType::SUBQUERY_S:

      size_t nofDistinctLeft = std::max(
          size_t(1),
          static_cast<size_t>(_subtree->getSizeEstimate() /
                              _subtree->getMultiplicity(_subtreeColIndex)));
      size_t nofDistinctRight = std::max(
          size_t(1), static_cast<size_t>(
                         getIndex().getHasPredicateFullSize() /
                         getIndex().getHasPredicateMultiplicityPredicates()));
      size_t nofDistinctInResult = std::min(nofDistinctLeft, nofDistinctRight);

      double jcMultiplicityInResult =
          _subtree->getMultiplicity(_subtreeColIndex) *
          getIndex().getHasPredicateMultiplicityPredicates();
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
  result->_idTable.setCols(getResultWidth());
  result->_sortedBy = resultSortedOn();

  const std::vector<PatternID>& hasPattern = getIndex().getHasPattern();
  const CompactVectorOfStrings<Id>& hasPredicate = getIndex().getHasPredicate();
  const CompactVectorOfStrings<Id>& patterns = getIndex().getPatterns();

  RuntimeInformation& runtimeInfo = getRuntimeInfo();

  switch (_type) {
    case ScanType::FREE_S: {
      runtimeInfo.setDescriptor("HasPredicateScan free subject: " + _subject);
      Id objectId;
      if (!getIndex().getId(_object, &objectId)) {
        AD_THROW(ad_semsearch::Exception::BAD_INPUT,
                 "The predicate '" + _object + "' is not in the vocabulary.");
      }
      HasPredicateScan::computeFreeS(result, objectId, hasPattern, hasPredicate,
                                     patterns);
    } break;
    case ScanType::FREE_O: {
      runtimeInfo.setDescriptor("HasPredicateScan free object: " + _object);
      Id subjectId;
      if (!getIndex().getId(_subject, &subjectId)) {
        AD_THROW(ad_semsearch::Exception::BAD_INPUT,
                 "The subject " + _subject + " is not in the vocabulary.");
      }
      HasPredicateScan::computeFreeO(result, subjectId, hasPattern,
                                     hasPredicate, patterns);
    } break;
    case ScanType::FULL_SCAN:
      runtimeInfo.setDescriptor("HasPredicateScan full scan");
      HasPredicateScan::computeFullScan(result, hasPattern, hasPredicate,
                                        patterns,
                                        getIndex().getHasPredicateFullSize());
      break;
    case ScanType::SUBQUERY_S:

      std::shared_ptr<const ResultTable> subresult = _subtree->getResult();
      result->_resultTypes.insert(result->_resultTypes.begin(),
                                  subresult->_resultTypes.begin(),
                                  subresult->_resultTypes.end());
      result->_resultTypes.push_back(ResultTable::ResultType::KB);
      int inWidth = subresult->_idTable.cols();
      int outWidth = result->_idTable.cols();
      CALL_FIXED_SIZE_2(inWidth, outWidth, HasPredicateScan::computeSubqueryS,
                        &result->_idTable, subresult->_idTable,
                        _subtreeColIndex, hasPattern, hasPredicate, patterns);
      runtimeInfo.setDescriptor("HasPredicateScan with a subquery on " +
                                _subject);
      runtimeInfo.addChild(_subtree->getRootOperation()->getRuntimeInfo());
      break;
  }

  LOG(DEBUG) << "HasPredicateScan result compuation done." << std::endl;
}

void HasPredicateScan::computeFreeS(
    ResultTable* resultTable, Id objectId,
    const std::vector<PatternID>& hasPattern,
    const CompactVectorOfStrings<Id>& hasPredicate,
    const CompactVectorOfStrings<Id>& patterns) {
  IdTableStatic<1> result = resultTable->_idTable.moveToStatic<1>();
  resultTable->_resultTypes.push_back(ResultTable::ResultType::KB);
  uint64_t entityIndex = 0;
  while (entityIndex < hasPattern.size() || entityIndex < hasPredicate.size()) {
    if (entityIndex < hasPattern.size() &&
        hasPattern[entityIndex] != NO_PATTERN) {
      // add the pattern
      const auto& pattern = patterns[hasPattern[entityIndex]];
      for (const auto& predicate : pattern) {
        if (predicate == objectId) {
          result.push_back(
              {Id::makeFromVocabIndex(VocabIndex::make(entityIndex))});
        }
      }
    } else if (entityIndex < hasPredicate.size()) {
      // add the relations
      for (const auto& predicate : hasPredicate[entityIndex]) {
        if (predicate == objectId) {
          result.push_back(
              {Id::makeFromVocabIndex(VocabIndex::make(entityIndex))});
        }
      }
    }
    entityIndex++;
  }
  resultTable->_idTable = result.moveToDynamic();
}

void HasPredicateScan::computeFreeO(
    ResultTable* resultTable, Id subjectAsId,
    const std::vector<PatternID>& hasPattern,
    const CompactVectorOfStrings<Id>& hasPredicate,
    const CompactVectorOfStrings<Id>& patterns) {
  IdTableStatic<1> result = resultTable->_idTable.moveToStatic<1>();
  resultTable->_resultTypes.push_back(ResultTable::ResultType::KB);

  // subjects are always from the vocabulary
  AD_CHECK(subjectAsId.getDatatype() == Datatype::VocabIndex);
  auto subjectIndex = subjectAsId.getVocabIndex().get();
  if (subjectIndex < hasPattern.size() &&
      hasPattern[subjectIndex] != NO_PATTERN) {
    // add the pattern
    const auto& pattern = patterns[hasPattern[subjectIndex]];
    for (const auto& predicate : pattern) {
      result.push_back({predicate});
    }
  } else if (subjectIndex < hasPredicate.size()) {
    // add the relations
    for (const auto& predicate : hasPredicate[subjectIndex]) {
      result.push_back({predicate});
    }
  }
  resultTable->_idTable = result.moveToDynamic();
}

void HasPredicateScan::computeFullScan(
    ResultTable* resultTable, const std::vector<PatternID>& hasPattern,
    const CompactVectorOfStrings<Id>& hasPredicate,
    const CompactVectorOfStrings<Id>& patterns, size_t resultSize) {
  resultTable->_resultTypes.push_back(ResultTable::ResultType::KB);
  resultTable->_resultTypes.push_back(ResultTable::ResultType::KB);
  IdTableStatic<2> result = resultTable->_idTable.moveToStatic<2>();
  result.reserve(resultSize);

  uint64_t subjectIndex = 0;
  while (subjectIndex < hasPattern.size() ||
         subjectIndex < hasPredicate.size()) {
    if (subjectIndex < hasPattern.size() &&
        hasPattern[subjectIndex] != NO_PATTERN) {
      // add the pattern
      for (const auto& predicate : patterns[hasPattern[subjectIndex]]) {
        result.push_back(
            {Id::makeFromVocabIndex(VocabIndex::make(subjectIndex)),
             predicate});
      }
    } else if (subjectIndex < hasPredicate.size()) {
      // add the relations
      for (const auto& predicate : hasPredicate[subjectIndex]) {
        result.push_back(
            {Id::makeFromVocabIndex(VocabIndex::make(subjectIndex)),
             predicate});
      }
    }
    subjectIndex++;
  }
  resultTable->_idTable = result.moveToDynamic();
}

template <int IN_WIDTH, int OUT_WIDTH>
void HasPredicateScan::computeSubqueryS(
    IdTable* dynResult, const IdTable& dynInput, const size_t subtreeColIndex,
    const std::vector<PatternID>& hasPattern,
    const CompactVectorOfStrings<Id>& hasPredicate,
    const CompactVectorOfStrings<Id>& patterns) {
  IdTableStatic<OUT_WIDTH> result = dynResult->moveToStatic<OUT_WIDTH>();
  const IdTableView<IN_WIDTH> input = dynInput.asStaticView<IN_WIDTH>();

  LOG(DEBUG) << "HasPredicateScan subresult size " << input.size() << std::endl;

  for (size_t i = 0; i < input.size(); i++) {
    Id subjectAsId = input(i, subtreeColIndex);
    AD_CHECK(subjectAsId.getDatatype() == Datatype::VocabIndex);
    auto subjectIndex = subjectAsId.getVocabIndex().get();
    if (subjectIndex < hasPattern.size() &&
        hasPattern[subjectIndex] != NO_PATTERN) {
      // Expand the pattern and add it to the result
      for (const auto& predicate : patterns[hasPattern[subjectIndex]]) {
        result.push_back();
        size_t backIdx = result.size() - 1;
        for (size_t k = 0; k < input.cols(); k++) {
          result(backIdx, k) = input(i, k);
        }
        result(backIdx, input.cols()) = predicate;
      }
    } else if (subjectIndex < hasPredicate.size()) {
      // add the relations
      for (const auto& predicate : hasPredicate[subjectIndex]) {
        result.push_back();
        size_t backIdx = result.size() - 1;
        for (size_t k = 0; k < input.cols(); k++) {
          result(backIdx, k) = input(i, k);
        }
        result(backIdx, input.cols()) = predicate;
      }
    } else {
      break;
    }
  }
  *dynResult = result.moveToDynamic();
}

void HasPredicateScan::setSubject(const TripleComponent& subject) {
  if (!subject.isString()) {
    throw ParseException{absl::StrCat(
        "The object of a ql:has-predicate triple must be an IRI, but was \"",
        subject.toString(), "\"")};
  }
  _subject = subject.getString();
}

void HasPredicateScan::setObject(const TripleComponent& object) {
  if (!object.isString()) {
    throw ParseException{absl::StrCat(
        "The object of a ql:has-predicate triple must be an IRI, but was \"",
        object.toString(), "\"")};
  }
  _object = object.getString();
}

const std::string& HasPredicateScan::getObject() const { return _object; }

void HasPredicateScan::setSubtree(std::shared_ptr<QueryExecutionTree> subtree) {
  _subtree = std::move(subtree);
}

void HasPredicateScan::setSubtreeSubjectColumn(size_t colIndex) {
  _subtreeColIndex = colIndex;
}

HasPredicateScan::ScanType HasPredicateScan::getType() const { return _type; }
