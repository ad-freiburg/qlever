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
      _permutation(permutation),
      _subject(triple._s),
      _predicate(triple._p.getIri().starts_with("?")
                     ? TripleComponent(Variable{triple._p.getIri()})
                     : TripleComponent(triple._p.getIri())),
      _object(triple._o),
      _numVariables(_subject.isVariable() + _predicate.isVariable() +
                    _object.isVariable()),
      _sizeEstimate(std::numeric_limits<size_t>::max()) {
  precomputeSizeEstimate();

  auto permutedTriple = getPermutedTriple();
  for (size_t i = 0; i < 3 - _numVariables; ++i) {
    AD_CONTRACT_CHECK(!permutedTriple.at(i)->isVariable());
  }
  for (size_t i = 3 - _numVariables; i < permutedTriple.size(); ++i) {
    AD_CONTRACT_CHECK(permutedTriple.at(i)->isVariable());
  }
}

// _____________________________________________________________________________
string IndexScan::asStringImpl(size_t indent) const {
  std::ostringstream os;
  for (size_t i = 0; i < indent; ++i) {
    os << ' ';
  }

  auto permutationString = permutationToString(_permutation);

  if (getResultWidth() == 3) {
    AD_CORRECTNESS_CHECK(getResultWidth() == 3);
    os << "SCAN FOR FULL INDEX " << permutationToString(_permutation)
       << " (DUMMY OPERATION)";

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
  return "IndexScan " + _subject.toString() + " " + _predicate.toString() +
         " " + _object.toString();
}

// _____________________________________________________________________________
size_t IndexScan::getResultWidth() const { return _numVariables; }

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
  VariableToColumnMap res;
  // All the columns of an index scan only contain defined values.
  auto makeCol = makeAlwaysDefinedColumn;
  auto col = ColumnIndex{0};

  for (const TripleComponent* const ptr : getPermutedTriple()) {
    if (ptr->isVariable()) {
      res[ptr->getVariable()] = makeCol(col);
      ++col;
    }
  }
  return res;
}
// _____________________________________________________________________________
ResultTable IndexScan::computeResult() {
  LOG(DEBUG) << "IndexScan result computation...\n";
  IdTable idTable{getExecutionContext()->getAllocator()};

  using enum Permutation::Enum;
  idTable.setNumColumns(_numVariables);
  const auto& idx = _executionContext->getIndex();
  const auto permutedTriple = getPermutedTriple();
  if (_numVariables == 2) {
    idx.scan(*permutedTriple[0], &idTable, _permutation, _timeoutTimer);
  } else if (_numVariables == 1) {
    idx.scan(*permutedTriple[0], *permutedTriple[1], &idTable, _permutation,
             _timeoutTimer);
  } else {
    AD_CORRECTNESS_CHECK(_numVariables == 3);
    computeFullScan(&idTable, _permutation);
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
      if (auto size = getExecutionContext()->getQueryTreeCache().getPinnedSize(
              asString());
          size.has_value()) {
        return size.value();
      } else {
        // Explicitly store the result of the index scan. This make sure that
        // 1. It is not evicted from the cache before this query needs it again.
        // 2. We preserve the information whether this scan was computed during
        // the query planning.
        // TODO<joka921> We should only do this for small index scans. Even with
        // only one variable, index scans can become arbitrary large (e.g.
        // ?x rdf:type <someFixedType>. But this requires more information
        // from the scanning, so I leave it open for another PR.
        createRuntimeInfoFromEstimates();
        _precomputedResult = getResult();
        if (getRuntimeInfo().status_ == RuntimeInformation::Status::completed) {
          getRuntimeInfo().status_ =
              RuntimeInformation::Status::completedDuringQueryPlanning;
        }
        auto sizeEstimate = _precomputedResult.value()->size();
        getRuntimeInfo().sizeEstimate_ = sizeEstimate;
        getRuntimeInfo().costEstimate_ = sizeEstimate;
        return sizeEstimate;
      }
    } else if (getResultWidth() == 2) {
      const auto& firstKey = *getPermutedTriple()[0];
      return getIndex().getCardinality(firstKey, _permutation);
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
        _object.isString() ? _object.getString() : _object.toString();
    std::string subjectStr =
        _subject.isString() ? _subject.getString() : _subject.toString();
    std::string predStr =
        _predicate.isString() ? _predicate.getString() : _predicate.toString();
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
  _multiplicity.clear();
  if (_executionContext) {
    const auto& idx = getIndex();
    if (getResultWidth() == 1) {
      _multiplicity.emplace_back(1);
    } else if (getResultWidth() == 2) {
      const auto permutedTriple = getPermutedTriple();
      _multiplicity = idx.getMultiplicities(*permutedTriple[0], _permutation);
    } else {
      AD_CORRECTNESS_CHECK(getResultWidth() == 3);
      _multiplicity = idx.getMultiplicities(_permutation);
    }
  } else {
    _multiplicity.emplace_back(1);
    _multiplicity.emplace_back(1);
    if (getResultWidth() == 3) {
      _multiplicity.emplace_back(1);
    }
  }
  assert(_multiplicity.size() >= 1 || _multiplicity.size() <= 3);
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
  using Arr = std::array<const TripleComponent* const, 3>;
  Arr inp{&_subject, &_predicate, &_object};
  auto permutation = permutationToKeyOrder(_permutation);
  return {inp[permutation[0]], inp[permutation[1]], inp[permutation[2]]};
}
