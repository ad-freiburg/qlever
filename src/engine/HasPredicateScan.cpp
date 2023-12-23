// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)

#include "HasPredicateScan.h"

#include "CallFixedSize.h"

HasPredicateScan::HasPredicateScan(QueryExecutionContext* qec,
                                   std::shared_ptr<QueryExecutionTree> subtree,
                                   size_t subtreeJoinColumn,
                                   std::string objectVariable)
    : Operation{qec},
      _type{ScanType::SUBQUERY_S},
      _subtree{std::move(subtree)},
      _subtreeJoinColumn{subtreeJoinColumn},
      _object{std::move(objectVariable)} {}

HasPredicateScan::HasPredicateScan(QueryExecutionContext* qec,
                                   SparqlTriple triple)
    : Operation{qec} {
  // Just pick one direction, they should be equivalent.
  AD_CONTRACT_CHECK(triple._p._iri == HAS_PREDICATE_PREDICATE);
  // TODO(schnelle): Handle ?p ql:has-predicate ?p
  _type = [&]() {
    if (isVariable(triple._s) && (isVariable(triple._o))) {
      if (triple._s == triple._o) {
        throw std::runtime_error{
            "ql:has-predicate with same variable for subject and object not "
            "supported."};
      }
      return ScanType::FULL_SCAN;
    } else if (isVariable(triple._s)) {
      return ScanType::FREE_S;
    } else if (isVariable(triple._o)) {
      return ScanType::FREE_O;
    } else {
      AD_FAIL();
    }
  }();
  setSubject(triple._s);
  setObject(triple._o);
}

string HasPredicateScan::getCacheKeyImpl() const {
  std::ostringstream os;
  switch (_type) {
    case ScanType::FREE_S:
      os << "HAS_PREDICATE_SCAN with O = " << _object;
      break;
    case ScanType::FREE_O:
      os << "HAS_PREDICATE_SCAN with S = " << _subject;
      break;
    case ScanType::FULL_SCAN:
      os << "HAS_PREDICATE_SCAN for the full relation";
      break;
    case ScanType::SUBQUERY_S:
      os << "HAS_PREDICATE_SCAN with S = " << _subtree->getCacheKey();
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

vector<ColumnIndex> HasPredicateScan::resultSortedOn() const {
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

VariableToColumnMap HasPredicateScan::computeVariableToColumnMap() const {
  VariableToColumnMap varCols;
  using V = Variable;
  // All the columns that are newly created by this operation contain no
  // undefined values.
  auto col = makeAlwaysDefinedColumn;

  switch (_type) {
    case ScanType::FREE_S:
      // TODO<joka921> Better types for `_subject` and `_object`.
      varCols.emplace(std::make_pair(V{_subject}, col(0)));
      break;
    case ScanType::FREE_O:
      varCols.insert(std::make_pair(V{_object}, col(0)));
      break;
    case ScanType::FULL_SCAN:
      varCols.insert(std::make_pair(V{_subject}, col(0)));
      varCols.insert(std::make_pair(V{_object}, col(1)));
      break;
    case ScanType::SUBQUERY_S:
      varCols = _subtree->getVariableColumns();
      varCols.insert(std::make_pair(V{_object}, col(getResultWidth() - 1)));
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
        return getIndex().getAvgNumDistinctPredicatesPerSubject();
      }
      break;
    case ScanType::FREE_O:
      if (col == 0) {
        return getIndex().getAvgNumDistinctSubjectsPerPredicate();
      }
      break;
    case ScanType::FULL_SCAN:
      if (col == 0) {
        return getIndex().getAvgNumDistinctPredicatesPerSubject();
      } else if (col == 1) {
        return getIndex().getAvgNumDistinctSubjectsPerPredicate();
      }
      break;
    case ScanType::SUBQUERY_S:
      if (col < getResultWidth() - 1) {
        return _subtree->getMultiplicity(col) *
               getIndex().getAvgNumDistinctSubjectsPerPredicate();
      } else {
        return _subtree->getMultiplicity(_subtreeJoinColumn) *
               getIndex().getAvgNumDistinctSubjectsPerPredicate();
      }
  }
  return 1;
}

uint64_t HasPredicateScan::getSizeEstimateBeforeLimit() {
  switch (_type) {
    case ScanType::FREE_S:
      return static_cast<size_t>(
          getIndex().getAvgNumDistinctPredicatesPerSubject());
    case ScanType::FREE_O:
      return static_cast<size_t>(
          getIndex().getAvgNumDistinctSubjectsPerPredicate());
    case ScanType::FULL_SCAN:
      return getIndex().getNumDistinctSubjectPredicatePairs();
    case ScanType::SUBQUERY_S:
      return _subtree->getSizeEstimate() *
             getIndex().getAvgNumDistinctPredicatesPerSubject();
  }
  return 0;
}

size_t HasPredicateScan::getCostEstimate() {
  // TODO: these size estimates only work if all predicates are functional
  switch (_type) {
    case ScanType::FREE_S:
      return getSizeEstimateBeforeLimit();
    case ScanType::FREE_O:
      return getSizeEstimateBeforeLimit();
    case ScanType::FULL_SCAN:
      return getSizeEstimateBeforeLimit();
    case ScanType::SUBQUERY_S:
      return _subtree->getCostEstimate() + getSizeEstimateBeforeLimit();
  }
  return 0;
}

ResultTable HasPredicateScan::computeResult() {
  IdTable idTable{getExecutionContext()->getAllocator()};
  idTable.setNumColumns(getResultWidth());

  const std::vector<PatternID>& hasPattern = getIndex().getHasPattern();
  const CompactVectorOfStrings<Id>& hasPredicate = getIndex().getHasPredicate();
  const CompactVectorOfStrings<Id>& patterns = getIndex().getPatterns();

  switch (_type) {
    case ScanType::FREE_S: {
      Id objectId;
      if (!getIndex().getId(_object, &objectId)) {
        AD_THROW("The predicate '" + _object + "' is not in the vocabulary.");
      }
      HasPredicateScan::computeFreeS(&idTable, objectId, hasPattern,
                                     hasPredicate, patterns);
      return {std::move(idTable), resultSortedOn(), LocalVocab{}};
    };
    case ScanType::FREE_O: {
      Id subjectId;
      if (!getIndex().getId(_subject, &subjectId)) {
        AD_THROW("The subject " + _subject + " is not in the vocabulary.");
      }
      HasPredicateScan::computeFreeO(&idTable, subjectId, hasPattern,
                                     hasPredicate, patterns);
      return {std::move(idTable), resultSortedOn(), LocalVocab{}};
    };
    case ScanType::FULL_SCAN:
      HasPredicateScan::computeFullScan(
          &idTable, hasPattern, hasPredicate, patterns,
          getIndex().getNumDistinctSubjectPredicatePairs());
      return {std::move(idTable), resultSortedOn(), LocalVocab{}};
    case ScanType::SUBQUERY_S:

      std::shared_ptr<const ResultTable> subresult = _subtree->getResult();
      int inWidth = subresult->idTable().numColumns();
      int outWidth = idTable.numColumns();
      CALL_FIXED_SIZE((std::array{inWidth, outWidth}),
                      HasPredicateScan::computeSubqueryS, &idTable,
                      subresult->idTable(), _subtreeJoinColumn, hasPattern,
                      hasPredicate, patterns);
      return {std::move(idTable), resultSortedOn(),
              subresult->getSharedLocalVocab()};
  }
  AD_FAIL();
}

void HasPredicateScan::computeFreeS(
    IdTable* resultTable, Id objectId, const std::vector<PatternID>& hasPattern,
    const CompactVectorOfStrings<Id>& hasPredicate,
    const CompactVectorOfStrings<Id>& patterns) {
  IdTableStatic<1> result = std::move(*resultTable).toStatic<1>();
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
  *resultTable = std::move(result).toDynamic();
}

void HasPredicateScan::computeFreeO(
    IdTable* resultTable, Id subjectAsId,
    const std::vector<PatternID>& hasPattern,
    const CompactVectorOfStrings<Id>& hasPredicate,
    const CompactVectorOfStrings<Id>& patterns) {
  // Subjects always have to be from the vocabulary
  if (subjectAsId.getDatatype() != Datatype::VocabIndex) {
    return;
  }
  IdTableStatic<1> result = std::move(*resultTable).toStatic<1>();

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
  *resultTable = std::move(result).toDynamic();
}

void HasPredicateScan::computeFullScan(
    IdTable* resultTable, const std::vector<PatternID>& hasPattern,
    const CompactVectorOfStrings<Id>& hasPredicate,
    const CompactVectorOfStrings<Id>& patterns, size_t resultSize) {
  IdTableStatic<2> result = std::move(*resultTable).toStatic<2>();
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
  *resultTable = std::move(result).toDynamic();
}

template <int IN_WIDTH, int OUT_WIDTH>
void HasPredicateScan::computeSubqueryS(
    IdTable* dynResult, const IdTable& dynInput, const size_t subtreeColIndex,
    const std::vector<PatternID>& hasPattern,
    const CompactVectorOfStrings<Id>& hasPredicate,
    const CompactVectorOfStrings<Id>& patterns) {
  IdTableStatic<OUT_WIDTH> result = std::move(*dynResult).toStatic<OUT_WIDTH>();
  const IdTableView<IN_WIDTH> input = dynInput.asStaticView<IN_WIDTH>();

  LOG(DEBUG) << "HasPredicateScan subresult size " << input.size() << std::endl;

  for (size_t i = 0; i < input.size(); i++) {
    Id subjectAsId = input(i, subtreeColIndex);
    if (subjectAsId.getDatatype() != Datatype::VocabIndex) {
      continue;
    }
    auto subjectIndex = subjectAsId.getVocabIndex().get();
    if (subjectIndex < hasPattern.size() &&
        hasPattern[subjectIndex] != NO_PATTERN) {
      // Expand the pattern and add it to the result
      for (const auto& predicate : patterns[hasPattern[subjectIndex]]) {
        result.emplace_back();
        size_t backIdx = result.size() - 1;
        for (size_t k = 0; k < input.numColumns(); k++) {
          result(backIdx, k) = input(i, k);
        }
        result(backIdx, input.numColumns()) = predicate;
      }
    } else if (subjectIndex < hasPredicate.size()) {
      // add the relations
      for (const auto& predicate : hasPredicate[subjectIndex]) {
        result.emplace_back();
        size_t backIdx = result.size() - 1;
        for (size_t k = 0; k < input.numColumns(); k++) {
          result(backIdx, k) = input(i, k);
        }
        result(backIdx, input.numColumns()) = predicate;
      }
    } else {
      break;
    }
  }
  *dynResult = std::move(result).toDynamic();
}

void HasPredicateScan::setSubject(const TripleComponent& subject) {
  // TODO<joka921> Make the _subject and _object `Variant<Variable,...>`.
  if (subject.isString()) {
    _subject = subject.getString();
  } else if (subject.isVariable()) {
    _subject = subject.getVariable().name();
  } else {
    throw ParseException{
        absl::StrCat("The subject of a ql:has-predicate triple must be an IRI "
                     "or a variable, but was \"",
                     subject.toString(), "\"")};
  }
}

void HasPredicateScan::setObject(const TripleComponent& object) {
  // TODO<joka921> Make the _subject and _object `Variant<Variable,...>`.
  if (object.isString()) {
    _object = object.getString();
  } else if (object.isVariable()) {
    _object = object.getVariable().name();
  } else {
    throw ParseException{
        absl::StrCat("The object of a ql:has-predicate triple must be an IRI "
                     "or a variable, but was \"",
                     object.toString(), "\"")};
  }
}

const std::string& HasPredicateScan::getObject() const { return _object; }

HasPredicateScan::ScanType HasPredicateScan::getType() const { return _type; }
