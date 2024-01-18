// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)

#include "engine/HasPredicateScan.h"

#include "engine/AddCombinedRowToTable.h"
#include "engine/CallFixedSize.h"
#include "engine/IndexScan.h"
#include "engine/Join.h"
#include "index/IndexImpl.h"
#include "util/JoinAlgorithms/JoinColumnMapping.h"

// Assert that the `type` is a valid value for the `ScanType` enum.
static void checkType(HasPredicateScan::ScanType type) {
  using enum HasPredicateScan::ScanType;
  static constexpr std::array supportedTypes{FREE_O, FREE_S, SUBQUERY_S,
                                             FULL_SCAN};
  AD_CORRECTNESS_CHECK(ad_utility::contains(supportedTypes, type));
}

// Helper function for the constructor of the `HasPredicateScan`.
// Return a join operation between the `subtree` and the triple `?subject
// ql:has-pattern ?object` where the subject is specified by the
// `subtreeColIndex` which is an index into the `subtree`'s result columns and
// the `?object` is specified directly via the `objectVariable`.
// Also return the column index of the `objectVariable` in the final result.
static constexpr auto makeJoin =
    [](auto* qec, std::shared_ptr<QueryExecutionTree> subtree,
       ColumnIndex subtreeColIndex, const Variable& objectVariable)
    -> HasPredicateScan::SubtreeAndColumnIndex {
  const auto& subtreeVar =
      subtree->getVariableAndInfoByColumnIndex(subtreeColIndex).first;
  auto hasPatternScan = ad_utility::makeExecutionTree<IndexScan>(
      qec, Permutation::Enum::PSO,
      SparqlTriple{subtreeVar, HAS_PATTERN_PREDICATE, objectVariable});
  auto joinedSubtree = ad_utility::makeExecutionTree<Join>(
      qec, std::move(subtree), std::move(hasPatternScan), subtreeColIndex, 0);
  auto column =
      joinedSubtree->getVariableColumns().at(objectVariable).columnIndex_;
  return {std::move(joinedSubtree), column};
};

// ___________________________________________________________________________
HasPredicateScan::HasPredicateScan(QueryExecutionContext* qec,
                                   std::shared_ptr<QueryExecutionTree> subtree,
                                   size_t subtreeJoinColumn,
                                   std::string objectVariable)
    : Operation{qec},
      _type{ScanType::SUBQUERY_S},
      _subtree{makeJoin(qec, std::move(subtree), subtreeJoinColumn,
                        Variable{objectVariable})},
      _object{std::move(objectVariable)} {}

// ___________________________________________________________________________
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

// ___________________________________________________________________________
string HasPredicateScan::getCacheKeyImpl() const {
  std::ostringstream os;
  checkType(_type);
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
      os << "HAS_PREDICATE_SCAN with S = " << subtree().getCacheKey();
      break;
  }
  return std::move(os).str();
}

// ___________________________________________________________________________
string HasPredicateScan::getDescriptor() const {
  checkType(_type);
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

// ___________________________________________________________________________
size_t HasPredicateScan::getResultWidth() const {
  checkType(_type);
  switch (_type) {
    case ScanType::FREE_S:
      return 1;
    case ScanType::FREE_O:
      return 1;
    case ScanType::FULL_SCAN:
      return 2;
    case ScanType::SUBQUERY_S:
      return subtree().getResultWidth();
  }
  return -1;
}

// ___________________________________________________________________________
vector<ColumnIndex> HasPredicateScan::resultSortedOn() const {
  checkType(_type);
  switch (_type) {
    case ScanType::FREE_S:
      // is the lack of sorting here a problem?
      return {};
    case ScanType::FREE_O:
      return {0};
    case ScanType::FULL_SCAN:
      return {0};
    case ScanType::SUBQUERY_S:
      return subtree().resultSortedOn();
  }
  return {};
}

// ___________________________________________________________________________
VariableToColumnMap HasPredicateScan::computeVariableToColumnMap() const {
  using V = Variable;
  // All the columns that are newly created by this operation contain no
  // undefined values.
  auto col = makeAlwaysDefinedColumn;

  checkType(_type);
  switch (_type) {
    case ScanType::FREE_S:
      return {{V{_subject}, col(0)}};
    case ScanType::FREE_O:
      return {{V{_object}, col(0)}};
    case ScanType::FULL_SCAN:
      return {{V{_subject}, col(0)}, {V{_object}, col(1)}};
    case ScanType::SUBQUERY_S:
      return subtree().getVariableColumns();
  }
  AD_FAIL();
}

// ___________________________________________________________________________
void HasPredicateScan::setTextLimit(size_t limit) {
  if (_type == ScanType::SUBQUERY_S) {
    subtree().setTextLimit(limit);
  }
}

// ___________________________________________________________________________
bool HasPredicateScan::knownEmptyResult() {
  if (_type == ScanType::SUBQUERY_S) {
    return subtree().knownEmptyResult();
  } else {
    return false;
  }
}

// ___________________________________________________________________________
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
        return subtree().getMultiplicity(col) *
               getIndex().getAvgNumDistinctSubjectsPerPredicate();
      } else {
        return subtree().getMultiplicity(subtreeColIdx()) *
               getIndex().getAvgNumDistinctSubjectsPerPredicate();
      }
  }
  return 1;
}

// ___________________________________________________________________________
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
      return subtree().getSizeEstimate() *
             getIndex().getAvgNumDistinctPredicatesPerSubject();
  }
  return 0;
}

// ___________________________________________________________________________
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
      return subtree().getCostEstimate() + getSizeEstimateBeforeLimit();
  }
  return 0;
}

// ___________________________________________________________________________
ResultTable HasPredicateScan::computeResult() {
  IdTable idTable{getExecutionContext()->getAllocator()};
  idTable.setNumColumns(getResultWidth());

  const CompactVectorOfStrings<Id>& patterns = getIndex().getPatterns();
  auto hasPattern =
      getExecutionContext()
          ->getIndex()
          .getImpl()
          .getPermutation(Permutation::Enum::PSO)
          .lazyScan(qlever::specialIds.at(HAS_PATTERN_PREDICATE), std::nullopt,
                    std::nullopt, {}, cancellationHandle_);

  switch (_type) {
    case ScanType::FREE_S: {
      Id objectId;
      if (!getIndex().getId(_object, &objectId)) {
        AD_THROW("The predicate '" + _object + "' is not in the vocabulary.");
      }
      HasPredicateScan::computeFreeS(&idTable, objectId, hasPattern, patterns);
      return {std::move(idTable), resultSortedOn(), LocalVocab{}};
    };
    case ScanType::FREE_O: {
      Id subjectId;
      if (!getIndex().getId(_subject, &subjectId)) {
        AD_THROW("The subject " + _subject + " is not in the vocabulary.");
      }
      HasPredicateScan::computeFreeO(&idTable, subjectId, patterns);
      return {std::move(idTable), resultSortedOn(), LocalVocab{}};
    };
    case ScanType::FULL_SCAN:
      HasPredicateScan::computeFullScan(
          &idTable, hasPattern, patterns,
          getIndex().getNumDistinctSubjectPredicatePairs());
      return {std::move(idTable), resultSortedOn(), LocalVocab{}};
    case ScanType::SUBQUERY_S:

      int width = idTable.numColumns();
      auto doCompute = [this, &idTable, &patterns]<int width>() {
        return computeSubqueryS<width>(&idTable, patterns);
      };
      return ad_utility::callFixedSize(width, doCompute);
  }
  AD_FAIL();
}

// ___________________________________________________________________________
void HasPredicateScan::computeFreeS(
    IdTable* resultTable, Id objectId, auto&& hasPattern,
    const CompactVectorOfStrings<Id>& patterns) {
  IdTableStatic<1> result = std::move(*resultTable).toStatic<1>();
  // TODO<joka921> This can be a much simpler and cheaper implementation that
  // does a lazy scan on the specified predicate and then simply performs a
  // DISTINCT on the result.
  for (const auto& block : hasPattern) {
    auto patternColumn = block.getColumn(1);
    auto subjects = block.getColumn(0);
    for (size_t i : ad_utility::integerRange(block.numRows())) {
      const auto& pattern = patterns[patternColumn[i].getInt()];
      for (const auto& predicate : pattern) {
        if (predicate == objectId) {
          result.push_back({subjects[i]});
          break;
        }
      }
    }
  }
  *resultTable = std::move(result).toDynamic();
}

// ___________________________________________________________________________
void HasPredicateScan::computeFreeO(
    IdTable* resultTable, Id subjectAsId,
    const CompactVectorOfStrings<Id>& patterns) {
  auto hasPattern = getExecutionContext()
                        ->getIndex()
                        .getImpl()
                        .getPermutation(Permutation::Enum::PSO)
                        .scan(qlever::specialIds.at(HAS_PATTERN_PREDICATE),
                              subjectAsId, {}, cancellationHandle_);
  AD_CORRECTNESS_CHECK(hasPattern.numRows() <= 1);
  for (auto& patternIdx : hasPattern.getColumn(0)) {
    const auto& pattern = patterns[patternIdx.getInt()];
    resultTable->resize(pattern.size());
    std::ranges::copy(pattern, resultTable->getColumn(0).begin());
  }
}

// ___________________________________________________________________________
void HasPredicateScan::computeFullScan(
    IdTable* resultTable, auto&& hasPattern,
    const CompactVectorOfStrings<Id>& patterns, size_t resultSize) {
  IdTableStatic<2> result = std::move(*resultTable).toStatic<2>();
  result.reserve(resultSize);
  for (const auto& block : hasPattern) {
    auto patternColumn = block.getColumn(1);
    auto subjects = block.getColumn(0);
    for (size_t i : ad_utility::integerRange(block.numRows())) {
      const auto& pattern = patterns[patternColumn[i].getInt()];
      for (const auto& predicate : pattern) {
        result.push_back({subjects[i], predicate});
      }
    }
  }
  *resultTable = std::move(result).toDynamic();
}

// ___________________________________________________________________________
template <int WIDTH>
ResultTable HasPredicateScan::computeSubqueryS(
    IdTable* dynResult, const CompactVectorOfStrings<Id>& patterns) {
  auto subresult = subtree().getResult();
  auto patternCol = subtreeColIdx();
  auto result = std::move(*dynResult).toStatic<WIDTH>();
  for (const auto& row : subresult->idTable().asStaticView<WIDTH>()) {
    const auto& pattern = patterns[row[patternCol].getInt()];
    for (auto predicate : pattern) {
      result.push_back(row);
      result.back()[patternCol] = predicate;
    }
  }
  return {std::move(result).toDynamic(), resultSortedOn(),
          subresult->getSharedLocalVocab()};
}

// ___________________________________________________________________________
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

// ___________________________________________________________________________
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

// ___________________________________________________________________________
const std::string& HasPredicateScan::getObject() const { return _object; }

// ___________________________________________________________________________
HasPredicateScan::ScanType HasPredicateScan::getType() const { return _type; }
