// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "engine/IndexScan.h"

#include <sstream>
#include <string>

#include "index/IndexImpl.h"
#include "index/TriplesView.h"
#include "parser/ParsedQuery.h"

using std::string;

// _____________________________________________________________________________
IndexScan::IndexScan(QueryExecutionContext* qec, Permutation::Enum permutation,
                     const SparqlTriple& triple)
    : Operation(qec),
      permutation_(permutation),
      subject_(triple._s),
      predicate_(triple._p.getIri().starts_with("?")
                     ? TripleComponent(Variable{triple._p.getIri()})
                     : TripleComponent(triple._p.getIri())),
      object_(triple._o),
      numVariables_(static_cast<size_t>(subject_.isVariable()) +
                    static_cast<size_t>(predicate_.isVariable()) +
                    static_cast<size_t>(object_.isVariable())),
      sizeEstimate_(computeSizeEstimate()) {
  // Check the following invariant: The permuted input triple must contain at
  // least one variable, and all the variables must be at the end of the
  // permuted triple. For example in the PSO permutation, either only the O, or
  // the S and O, or all three of P, S, O can be variables, all other
  // combinations are not supported.
  auto permutedTriple = getPermutedTriple();
  for (size_t i = 0; i < 3 - numVariables_; ++i) {
    AD_CONTRACT_CHECK(!permutedTriple.at(i)->isVariable());
  }
  for (size_t i = 3 - numVariables_; i < permutedTriple.size(); ++i) {
    AD_CONTRACT_CHECK(permutedTriple.at(i)->isVariable());
  }
}

// _____________________________________________________________________________
string IndexScan::asStringImpl(size_t indent) const {
  std::ostringstream os;
  for (size_t i = 0; i < indent; ++i) {
    os << ' ';
  }

  auto permutationString = Permutation::toString(permutation_);

  if (getResultWidth() == 3) {
    AD_CORRECTNESS_CHECK(getResultWidth() == 3);
    os << "SCAN FOR FULL INDEX " << permutationString << " (DUMMY OPERATION)";

  } else {
    auto firstKeyString = permutationString.at(0);
    auto permutedTriple = getPermutedTriple();
    const auto& firstKey = permutedTriple.at(0)->toRdfLiteral();
    if (getResultWidth() == 1) {
      auto secondKeyString = permutationString.at(1);
      const auto& secondKey = permutedTriple.at(1)->toRdfLiteral();
      os << "SCAN " << permutationString << " with " << firstKeyString
         << " = \"" << firstKey << "\", " << secondKeyString << " = \""
         << secondKey << "\"";
    } else if (getResultWidth() == 2) {
      os << "SCAN " << permutationString << " with " << firstKeyString
         << " = \"" << firstKey << "\"";
    }
  }
  return std::move(os).str();
}

// _____________________________________________________________________________
string IndexScan::getDescriptor() const {
  return "IndexScan " + subject_.toString() + " " + predicate_.toString() +
         " " + object_.toString();
}

// _____________________________________________________________________________
size_t IndexScan::getResultWidth() const { return numVariables_; }

// _____________________________________________________________________________
vector<ColumnIndex> IndexScan::resultSortedOn() const {
  switch (getResultWidth()) {
    case 1:
      return {ColumnIndex{0}};
    case 2:
      return {ColumnIndex{0}, ColumnIndex{1}};
    case 3:
      return {ColumnIndex{0}, ColumnIndex{1}, ColumnIndex{2}};
    default:
      AD_FAIL();
  }
}

// _____________________________________________________________________________
VariableToColumnMap IndexScan::computeVariableToColumnMap() const {
  VariableToColumnMap variableToColumnMap;
  // All the columns of an index scan only contain defined values.
  auto makeCol = makeAlwaysDefinedColumn;
  auto nextColIdx = ColumnIndex{0};

  for (const TripleComponent* const ptr : getPermutedTriple()) {
    if (ptr->isVariable()) {
      variableToColumnMap[ptr->getVariable()] = makeCol(nextColIdx);
      ++nextColIdx;
    }
  }
  return variableToColumnMap;
}
// _____________________________________________________________________________
ResultTable IndexScan::computeResult() {
  LOG(DEBUG) << "IndexScan result computation...\n";
  IdTable idTable{getExecutionContext()->getAllocator()};

  using enum Permutation::Enum;
  idTable.setNumColumns(numVariables_);
  const auto& index = _executionContext->getIndex();
  const auto permutedTriple = getPermutedTriple();
  if (numVariables_ == 2) {
    index.scan(*permutedTriple[0], &idTable, permutation_, _timeoutTimer);
  } else if (numVariables_ == 1) {
    index.scan(*permutedTriple[0], *permutedTriple[1], &idTable, permutation_,
               _timeoutTimer);
  } else {
    AD_CORRECTNESS_CHECK(numVariables_ == 3);
    computeFullScan(&idTable, permutation_);
  }
  LOG(DEBUG) << "IndexScan result computation done.\n";

  return {std::move(idTable), resultSortedOn(), LocalVocab{}};
}

// _____________________________________________________________________________
size_t IndexScan::computeSizeEstimate() {
  if (_executionContext) {
    // Should always be in this branch. Else is only for test cases.

    // We have to do a simple scan anyway so might as well do it now
    if (getResultWidth() == 1) {
      // TODO<C++23> Use the monadic operation `std::optional::or_else`.
      // Note: we cannot use `optional::value_or()` here, because the else
      // case is expensive to compute, and we need it lazily evaluated.
      if (auto size = getExecutionContext()->getQueryTreeCache().getPinnedSize(
              asString());
          size.has_value()) {
        return size.value();
      } else {
        // This call explicitly has to read two blocks of triples from memory to
        // obtain an exact size estimate.
        return getIndex().getResultSizeOfScan(
            *getPermutedTriple()[0], *getPermutedTriple()[1], permutation_);
      }
    } else if (getResultWidth() == 2) {
      const TripleComponent& firstKey = *getPermutedTriple()[0];
      return getIndex().getCardinality(firstKey, permutation_);
    } else {
      // The triple consists of three variables.
      // TODO<joka921> As soon as all implementations of a full index scan
      // (Including the "dummy joins" in Join.cpp) consistently exclude the
      // internal triples, this estimate should be changed to only return
      // the number of triples in the actual knowledge graph (excluding the
      // internal triples).
      AD_CORRECTNESS_CHECK(getResultWidth() == 3);
      return getIndex().numTriples().normalAndInternal_();
    }
  } else {
    // Only for test cases. The handling of the objects is to make the
    // strange query planner tests pass.
    // TODO<joka921> Code duplication.
    std::string objectStr =
        object_.isString() ? object_.getString() : object_.toString();
    std::string subjectStr =
        subject_.isString() ? subject_.getString() : subject_.toString();
    std::string predStr =
        predicate_.isString() ? predicate_.getString() : predicate_.toString();
    return 1000 + subjectStr.size() + predStr.size() + objectStr.size();
  }
}

// _____________________________________________________________________________
size_t IndexScan::getCostEstimate() {
  if (getResultWidth() != 3) {
    return getSizeEstimateBeforeLimit();
  } else {
    // The computation of the `full scan` estimate must be consistent with the
    // full scan dummy joins in `Join.cpp` for correct query planning.
    // The following calculation is done in a way that makes materializing a
    // full index scan always more expensive than implicitly computing it in the
    // so-called "dummy joins" (see `Join.h` and `Join.cpp`). The assumption is,
    // that materializing a single triple via a full index scan is 10'000 more
    // expensive than materializing it via some other means.

    // Note that we cannot set the cost to `infinity` or `max`, because this
    // might lead to overflows in upstream operations when the cost estimate is
    // an integer (this currently is the case). When implementing them as
    // floating point numbers, a cost estimate of `infinity` would
    // remove the ability to distinguish the costs of plans that perform full
    // scans but still have different overall costs.
    // TODO<joka921> The conceptually right way to do this is to make the cost
    // estimate a tuple `(numFullIndexScans, costEstimateForRemainder)`.
    // Implement this functionality.

    return getSizeEstimateBeforeLimit() * 10'000;
  }
}

// _____________________________________________________________________________
void IndexScan::determineMultiplicities() {
  multiplicity_.clear();
  if (_executionContext) {
    const auto& idx = getIndex();
    if (getResultWidth() == 1) {
      multiplicity_.emplace_back(1);
    } else if (getResultWidth() == 2) {
      const auto permutedTriple = getPermutedTriple();
      multiplicity_ = idx.getMultiplicities(*permutedTriple[0], permutation_);
    } else {
      AD_CORRECTNESS_CHECK(getResultWidth() == 3);
      multiplicity_ = idx.getMultiplicities(permutation_);
    }
  } else {
    multiplicity_.emplace_back(1);
    multiplicity_.emplace_back(1);
    if (getResultWidth() == 3) {
      multiplicity_.emplace_back(1);
    }
  }
  assert(multiplicity_.size() >= 1 || multiplicity_.size() <= 3);
}

// ________________________________________________________________________
void IndexScan::computeFullScan(IdTable* result,
                                const Permutation::Enum permutation) const {
  auto [ignoredRanges, isTripleIgnored] =
      getIndex().getImpl().getIgnoredIdRanges(permutation);

  result->setNumColumns(3);

  // This implementation computes the complete knowledge graph, except the
  // internal triples.
  uint64_t resultSize = getIndex().numTriples().normal_;
  if (getLimit()._limit.has_value() && getLimit()._limit < resultSize) {
    resultSize = getLimit()._limit.value();
  }

  // TODO<joka921> Implement OFFSET
  if (getLimit()._offset != 0) {
    throw NotSupportedException{
        "Scanning the complete index with an OFFSET clause is currently not "
        "supported by QLever"};
  }
  result->reserve(resultSize);
  auto table = std::move(*result).toStatic<3>();
  size_t i = 0;
  const auto& permutationImpl =
      getExecutionContext()->getIndex().getImpl().getPermutation(permutation);
  auto triplesView =
      TriplesView(permutationImpl, getExecutionContext()->getAllocator(),
                  ignoredRanges, isTripleIgnored);
  for (const auto& triple : triplesView) {
    if (i >= resultSize) {
      break;
    }
    table.push_back(triple);
    ++i;
  }
  *result = std::move(table).toDynamic();
}

// ___________________________________________________________________________
std::array<const TripleComponent* const, 3> IndexScan::getPermutedTriple()
    const {
  std::array triple{&subject_, &predicate_, &object_};
  auto permutation = Permutation::toKeyOrder(permutation_);
  return {triple[permutation[0]], triple[permutation[1]],
          triple[permutation[2]]};
}
