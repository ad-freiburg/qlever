// Copyright 2018, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Authors: (2018 - 2019) Florian Kramer (florian.kramer@mail.uni-freiburg.de)
//          (2024 -     ) Johannes Kalmbach (kalmbach@cs.uni-freiburg.de)

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
      SparqlTriple{subtreeVar, std::string{HAS_PATTERN_PREDICATE},
                   objectVariable});
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
                                   Variable objectVariable)
    : Operation{qec},
      type_{ScanType::SUBQUERY_S},
      subtree_{makeJoin(qec, std::move(subtree), subtreeJoinColumn,
                        Variable{objectVariable})},
      object_{std::move(objectVariable)} {}

// A small helper function that sanitizes the `triple` which is passed to the
// constructor of `HasPredicateScan` and determines the corresponding
// `ScanType`.
static HasPredicateScan::ScanType getScanType(const SparqlTriple& triple) {
  using enum HasPredicateScan::ScanType;
  AD_CONTRACT_CHECK(triple.p_.iri_ == HAS_PREDICATE_PREDICATE);
  if (isVariable(triple.s_) && (isVariable(triple.o_))) {
    if (triple.s_ == triple.o_) {
      throw std::runtime_error{
          "ql:has-predicate with same variable for subject and object not "
          "supported."};
    }
    return FULL_SCAN;
  } else if (isVariable(triple.s_)) {
    return FREE_S;
  } else if (isVariable(triple.o_)) {
    return FREE_O;
  } else {
    AD_FAIL();
  }
}

// ___________________________________________________________________________
HasPredicateScan::HasPredicateScan(QueryExecutionContext* qec,
                                   SparqlTriple triple)
    : Operation{qec},
      type_{getScanType(triple)},
      subject_{triple.s_},
      object_{triple.o_} {}

// ___________________________________________________________________________
string HasPredicateScan::getCacheKeyImpl() const {
  std::ostringstream os;
  checkType(type_);
  switch (type_) {
    case ScanType::FREE_S:
      os << "HAS_PREDICATE_SCAN with O = " << object_;
      break;
    case ScanType::FREE_O:
      os << "HAS_PREDICATE_SCAN with S = " << subject_;
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
  checkType(type_);
  switch (type_) {
    case ScanType::FREE_S:
      return "HasPredicateScan free subject: " + subject_.toRdfLiteral();
    case ScanType::FREE_O:
      return "HasPredicateScan free object: " + object_.toRdfLiteral();
    case ScanType::FULL_SCAN:
      return "HasPredicateScan full scan";
    case ScanType::SUBQUERY_S:
      return "HasPredicateScan with subquery";
    default:
      return "HasPredicateScan";
  }
}

// ___________________________________________________________________________
size_t HasPredicateScan::getResultWidth() const {
  checkType(type_);
  switch (type_) {
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
  checkType(type_);
  switch (type_) {
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
  // All the columns that are newly created by this operation contain no
  // undefined values.
  auto col = makeAlwaysDefinedColumn;

  checkType(type_);
  switch (type_) {
    case ScanType::FREE_S:
      return {{subject_.getVariable(), col(0)}};
    case ScanType::FREE_O:
      return {{object_.getVariable(), col(0)}};
    case ScanType::FULL_SCAN:
      return {{subject_.getVariable(), col(0)},
              {object_.getVariable(), col(1)}};
    case ScanType::SUBQUERY_S:
      return subtree().getVariableColumns();
  }
  AD_FAIL();
}

// ___________________________________________________________________________
bool HasPredicateScan::knownEmptyResult() {
  if (type_ == ScanType::SUBQUERY_S) {
    return subtree().knownEmptyResult();
  } else {
    return false;
  }
}

// ___________________________________________________________________________
float HasPredicateScan::getMultiplicity(size_t col) {
  // Default value for columns about which we know nothing.
  double result = 1.0;
  switch (type_) {
    case ScanType::FREE_S:
      if (col == 0) {
        result = getIndex().getAvgNumDistinctPredicatesPerSubject();
      }
      break;
    case ScanType::FREE_O:
      if (col == 0) {
        result = getIndex().getAvgNumDistinctSubjectsPerPredicate();
      }
      break;
    case ScanType::FULL_SCAN:
      if (col == 0) {
        result = getIndex().getAvgNumDistinctPredicatesPerSubject();
      } else if (col == 1) {
        result = getIndex().getAvgNumDistinctSubjectsPerPredicate();
      }
      break;
    case ScanType::SUBQUERY_S:
      if (col < getResultWidth() - 1) {
        result = subtree().getMultiplicity(col) *
                 getIndex().getAvgNumDistinctSubjectsPerPredicate();
      } else {
        result = subtree().getMultiplicity(subtreeColIdx()) *
                 getIndex().getAvgNumDistinctSubjectsPerPredicate();
      }
  }
  return static_cast<float>(result);
}

// ___________________________________________________________________________
uint64_t HasPredicateScan::getSizeEstimateBeforeLimit() {
  switch (type_) {
    case ScanType::FREE_S:
      return static_cast<uint64_t>(
          getIndex().getAvgNumDistinctPredicatesPerSubject());
    case ScanType::FREE_O:
      return static_cast<uint64_t>(
          getIndex().getAvgNumDistinctSubjectsPerPredicate());
    case ScanType::FULL_SCAN:
      return getIndex().getNumDistinctSubjectPredicatePairs();
    case ScanType::SUBQUERY_S:
      return static_cast<uint64_t>(
          static_cast<double>(subtree().getSizeEstimate()) *
          getIndex().getAvgNumDistinctPredicatesPerSubject());
  }
  return 0;
}

// ___________________________________________________________________________
size_t HasPredicateScan::getCostEstimate() {
  // TODO: these size estimates only work if all predicates are functional
  switch (type_) {
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
Result HasPredicateScan::computeResult([[maybe_unused]] bool requestLaziness) {
  IdTable idTable{getExecutionContext()->getAllocator()};
  idTable.setNumColumns(getResultWidth());

  const CompactVectorOfStrings<Id>& patterns = getIndex().getPatterns();
  const auto& index = getExecutionContext()->getIndex().getImpl();
  auto scanSpec =
      ScanSpecificationAsTripleComponent{
          TripleComponent::Iri::fromIriref(HAS_PATTERN_PREDICATE), std::nullopt,
          std::nullopt}
          .toScanSpecification(index);
  auto hasPattern =
      index.getPermutation(Permutation::Enum::PSO)
          .lazyScan(scanSpec, std::nullopt, {}, cancellationHandle_,
                    locatedTriplesSnapshot());

  auto getId = [this](const TripleComponent tc) {
    std::optional<Id> id = tc.toValueId(getIndex().getVocab());
    if (!id.has_value()) {
      AD_THROW("The entity '" + tc.toRdfLiteral() +
               "' required by `ql:has-predicate` is not in the vocabulary.");
    }
    return id.value();
  };
  switch (type_) {
    case ScanType::FREE_S: {
      HasPredicateScan::computeFreeS(&idTable, getId(object_), hasPattern,
                                     patterns);
      return {std::move(idTable), resultSortedOn(), LocalVocab{}};
    };
    case ScanType::FREE_O: {
      HasPredicateScan::computeFreeO(&idTable, getId(subject_), patterns);
      return {std::move(idTable), resultSortedOn(), LocalVocab{}};
    };
    case ScanType::FULL_SCAN:
      HasPredicateScan::computeFullScan(
          &idTable, hasPattern, patterns,
          getIndex().getNumDistinctSubjectPredicatePairs());
      return {std::move(idTable), resultSortedOn(), LocalVocab{}};
    case ScanType::SUBQUERY_S:

      auto width = static_cast<int>(idTable.numColumns());
      auto doCompute = [this, &idTable, &patterns]<int width>() {
        return computeSubqueryS<width>(&idTable, patterns);
      };
      return ad_utility::callFixedSize(width, doCompute);
  }
  AD_FAIL();
}

// ___________________________________________________________________________
template <typename HasPattern>
void HasPredicateScan::computeFreeS(
    IdTable* resultTable, Id objectId, HasPattern& hasPattern,
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
    const CompactVectorOfStrings<Id>& patterns) const {
  const auto& index = getExecutionContext()->getIndex().getImpl();
  auto scanSpec =
      ScanSpecificationAsTripleComponent{
          TripleComponent::Iri::fromIriref(HAS_PATTERN_PREDICATE), subjectAsId,
          std::nullopt}
          .toScanSpecification(index);
  auto hasPattern = index.getPermutation(Permutation::Enum::PSO)
                        .scan(std::move(scanSpec), {}, cancellationHandle_,
                              locatedTriplesSnapshot());
  AD_CORRECTNESS_CHECK(hasPattern.numRows() <= 1);
  for (Id patternId : hasPattern.getColumn(0)) {
    const auto& pattern = patterns[patternId.getInt()];
    resultTable->resize(pattern.size());
    ql::ranges::copy(pattern, resultTable->getColumn(0).begin());
  }
}

// ___________________________________________________________________________
template <typename HasPattern>
void HasPredicateScan::computeFullScan(
    IdTable* resultTable, HasPattern& hasPattern,
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
Result HasPredicateScan::computeSubqueryS(
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
const TripleComponent& HasPredicateScan::getObject() const { return object_; }

// ___________________________________________________________________________
HasPredicateScan::ScanType HasPredicateScan::getType() const { return type_; }

// _____________________________________________________________________________
std::unique_ptr<Operation> HasPredicateScan::cloneImpl() const {
  auto copy = std::make_unique<HasPredicateScan>(*this);
  if (subtree_.has_value()) {
    copy->subtree_.value().subtree_ = subtree().clone();
  }
  return copy;
}
