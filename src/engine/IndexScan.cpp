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

IndexScan::IndexScan(QueryExecutionContext* qec, ScanType type,
                     const SparqlTriple& triple)
    : Operation(qec),
      _type(type),
      _subject(triple._s),
      _predicate(triple._p.getIri().starts_with("?")
                     ? TripleComponent(Variable{triple._p.getIri()})
                     : TripleComponent(triple._p.getIri())),
      _object(triple._o),
      _sizeEstimate(std::numeric_limits<size_t>::max()) {
  precomputeSizeEstimate();
}
// _____________________________________________________________________________
string IndexScan::asStringImpl(size_t indent) const {
  std::ostringstream os;
  for (size_t i = 0; i < indent; ++i) {
    os << ' ';
  }
  switch (_type) {
    case PSO_BOUND_S:
      os << "SCAN PSO with P = \"" << _predicate << "\", S = \"" << _subject
         << "\"";
      break;
    case POS_BOUND_O:
      os << "SCAN POS with P = \"" << _predicate << "\", O = \""
         << _object.toRdfLiteral() << "\"";
      break;
    case SOP_BOUND_O:
      os << "SCAN SOP with S = \"" << _subject << "\", O = \""
         << _object.toRdfLiteral() << "\"";
      break;
    case PSO_FREE_S:
      os << "SCAN PSO with P = \"" << _predicate << "\"";
      break;
    case POS_FREE_O:
      os << "SCAN POS with P = \"" << _predicate << "\"";
      break;
    case SPO_FREE_P:
      os << "SCAN SPO with S = \"" << _subject << "\"";
      break;
    case SOP_FREE_O:
      os << "SCAN SOP with S = \"" << _subject << "\"";
      break;
    case OPS_FREE_P:
      os << "SCAN OPS with O = \"" << _object.toRdfLiteral() << "\"";
      break;
    case OSP_FREE_S:
      os << "SCAN OSP with O = \"" << _object.toRdfLiteral() << "\"";
      break;
    case FULL_INDEX_SCAN_SPO:
      os << "SCAN FOR FULL INDEX SPO (DUMMY OPERATION)";
      break;
    case FULL_INDEX_SCAN_SOP:
      os << "SCAN FOR FULL INDEX SOP (DUMMY OPERATION)";
      break;
    case FULL_INDEX_SCAN_PSO:
      os << "SCAN FOR FULL INDEX PSO (DUMMY OPERATION)";
      break;
    case FULL_INDEX_SCAN_POS:
      os << "SCAN FOR FULL INDEX POS (DUMMY OPERATION)";
      break;
    case FULL_INDEX_SCAN_OSP:
      os << "SCAN FOR FULL INDEX OSP (DUMMY OPERATION)";
      break;
    case FULL_INDEX_SCAN_OPS:
      os << "SCAN FOR FULL INDEX OPS (DUMMY OPERATION)";
      break;
  }
  return std::move(os).str();
}

// _____________________________________________________________________________
string IndexScan::getDescriptor() const {
  return "IndexScan " + _subject.toString() + " " + _predicate.toString() +
         " " + _object.toString();
}

// _____________________________________________________________________________
size_t IndexScan::getResultWidth() const {
  switch (_type) {
    case PSO_BOUND_S:
    case POS_BOUND_O:
    case SOP_BOUND_O:
      return 1;
    case PSO_FREE_S:
    case POS_FREE_O:
    case SPO_FREE_P:
    case SOP_FREE_O:
    case OSP_FREE_S:
    case OPS_FREE_P:
      return 2;
    case FULL_INDEX_SCAN_SPO:
    case FULL_INDEX_SCAN_SOP:
    case FULL_INDEX_SCAN_PSO:
    case FULL_INDEX_SCAN_POS:
    case FULL_INDEX_SCAN_OSP:
    case FULL_INDEX_SCAN_OPS:
      return 3;
    default:
      AD_FAIL();
  }
}

// _____________________________________________________________________________
vector<size_t> IndexScan::resultSortedOn() const {
  switch (getResultWidth()) {
    case 1:
      return {0};
    case 2:
      return {0, 1};
    case 3:
      return {0, 1, 2};
    default:
      AD_FAIL();
  }
}

// _____________________________________________________________________________
VariableToColumnMap IndexScan::computeVariableToColumnMap() const {
  VariableToColumnMap res;
  // All the columns of an index scan only contain defined values.
  auto makeCol = makeAlwaysDefinedColumn;
  size_t col = 0;

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

  using enum Index::Permutation;
  size_t numVariables = scanTypeToNumVariables(_type);
  idTable.setNumColumns(numVariables);
  const auto& idx = _executionContext->getIndex();
  const auto permutedTriple = getPermutedTriple();
  const Index::Permutation permutation = scanTypeToPermutation(_type);
  if (numVariables == 2) {
    idx.scan(*permutedTriple[0], &idTable, permutation, _timeoutTimer);
  } else if (numVariables == 1) {
    idx.scan(*permutedTriple[0], *permutedTriple[1], &idTable, permutation,
             _timeoutTimer);
  } else {
    AD_CORRECTNESS_CHECK(numVariables == 3);
    computeFullScan(&idTable, permutation);
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
      return getIndex().getCardinality(firstKey, scanTypeToPermutation(_type));
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
  auto permutation = scanTypeToPermutation(_type);
  if (_executionContext) {
    const auto& idx = getIndex();
    if (getResultWidth() == 1) {
      _multiplicity.emplace_back(1);
    } else if (getResultWidth() == 2) {
      const auto permutedTriple = getPermutedTriple();
      _multiplicity = idx.getMultiplicities(*permutedTriple[0], permutation);
    } else {
      AD_CORRECTNESS_CHECK(getResultWidth() == 3);
      _multiplicity = idx.getMultiplicities(permutation);
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
                                const Index::Permutation permutation) const {
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
  auto triplesView =
      getExecutionContext()->getIndex().getImpl().applyToPermutation(
          permutation, [&, ignoredRanges = ignoredRanges,
                        isTripleIgnored = isTripleIgnored](const auto& p) {
            return TriplesView(p, getExecutionContext()->getAllocator(),
                               ignoredRanges, isTripleIgnored);
          });
  for (const auto& triple : triplesView) {
    if (i >= resultSize) {
      break;
    }
    table.push_back(triple);
    ++i;
  }
  *result = std::move(table).toDynamic();
}
