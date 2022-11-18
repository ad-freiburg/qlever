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
      _predicate(triple._p.getIri()),
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
  return "IndexScan " + _subject.toString() + " " + _predicate + " " +
         _object.toString();
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
      AD_THROW(ad_semsearch::Exception::CHECK_FAILED, "Should be unreachable.");
  }
}

// _____________________________________________________________________________
vector<size_t> IndexScan::resultSortedOn() const {
  switch (_type) {
    case PSO_BOUND_S:
    case POS_BOUND_O:
    case SOP_BOUND_O:
      return {0};
    case PSO_FREE_S:
    case POS_FREE_O:
    case SPO_FREE_P:
    case SOP_FREE_O:
    case OSP_FREE_S:
    case OPS_FREE_P:
      return {0, 1};
    case FULL_INDEX_SCAN_SPO:
    case FULL_INDEX_SCAN_SOP:
    case FULL_INDEX_SCAN_PSO:
    case FULL_INDEX_SCAN_POS:
    case FULL_INDEX_SCAN_OSP:
    case FULL_INDEX_SCAN_OPS:
      return {0, 1, 2};
    default:
      AD_THROW(ad_semsearch::Exception::CHECK_FAILED, "Should be unreachable.");
  }
}

// _____________________________________________________________________________
VariableToColumnMap IndexScan::computeVariableToColumnMap() const {
  VariableToColumnMap res;
  size_t col = 0;

  // Helper lambdas that add the respective triple component as the next column.
  auto addSubject = [&]() {
    if (_subject.isVariable()) {
      res[_subject.getVariable()] = col++;
    }
  };
  // TODO<joka921> Refactor the `PropertyPath` class s.t. it also has
  //`isVariable` and `getVariable`, then those three lambdas can become one.
  auto addPredicate = [&]() {
    if (_predicate[0] == '?') {
      res[Variable{_predicate}] = col++;
    }
  };
  auto addObject = [&]() {
    if (_object.isVariable()) {
      res[_object.getVariable()] = col++;
    }
  };

  switch (_type) {
    case SPO_FREE_P:
    case FULL_INDEX_SCAN_SPO:
      addSubject();
      addPredicate();
      addObject();
      return res;
    case SOP_FREE_O:
    case SOP_BOUND_O:
    case FULL_INDEX_SCAN_SOP:
      addSubject();
      addObject();
      addPredicate();
      return res;
    case PSO_BOUND_S:
    case PSO_FREE_S:
    case FULL_INDEX_SCAN_PSO:
      addPredicate();
      addSubject();
      addObject();
      return res;
    case POS_BOUND_O:
    case POS_FREE_O:
    case FULL_INDEX_SCAN_POS:
      addPredicate();
      addObject();
      addSubject();
      return res;
    case OPS_FREE_P:
    case FULL_INDEX_SCAN_OPS:
      addObject();
      addPredicate();
      addSubject();
      return res;
    case OSP_FREE_S:
    case FULL_INDEX_SCAN_OSP:
      addObject();
      addSubject();
      addPredicate();
      return res;
    default:
      AD_THROW(ad_semsearch::Exception::CHECK_FAILED, "Should be unreachable.");
  }
}
// _____________________________________________________________________________
void IndexScan::computeResult(ResultTable* result) {
  LOG(DEBUG) << "IndexScan result computation...\n";

  using enum Index::Permutation;
  switch (_type) {
    case PSO_BOUND_S:
      computePSOboundS(result);
      break;
    case POS_BOUND_O:
      computePOSboundO(result);
      break;
    case PSO_FREE_S:
      computePSOfreeS(result);
      break;
    case POS_FREE_O:
      computePOSfreeO(result);
      break;
    case SOP_BOUND_O:
      computeSOPboundO(result);
      break;
    case SPO_FREE_P:
      computeSPOfreeP(result);
      break;
    case SOP_FREE_O:
      computeSOPfreeO(result);
      break;
    case OSP_FREE_S:
      computeOSPfreeS(result);
      break;
    case OPS_FREE_P:
      computeOPSfreeP(result);
      break;
    case FULL_INDEX_SCAN_SPO:
      computeFullScan(result, SPO);
      break;
    case FULL_INDEX_SCAN_SOP:
      computeFullScan(result, SOP);
      break;
    case FULL_INDEX_SCAN_PSO:
      computeFullScan(result, PSO);
      break;
    case FULL_INDEX_SCAN_POS:
      computeFullScan(result, POS);
      break;
    case FULL_INDEX_SCAN_OSP:
      computeFullScan(result, OSP);
      break;
    case FULL_INDEX_SCAN_OPS:
      computeFullScan(result, OPS);
      break;
  }
  LOG(DEBUG) << "IndexScan result computation done.\n";
}

// _____________________________________________________________________________
void IndexScan::computePSOboundS(ResultTable* result) const {
  result->_idTable.setCols(1);
  result->_resultTypes.push_back(ResultTable::ResultType::KB);
  result->_sortedBy = {0};
  const auto& idx = _executionContext->getIndex();
  idx.scan(_predicate, _subject, &result->_idTable, Index::Permutation::PSO,
           _timeoutTimer);
}

// _____________________________________________________________________________
void IndexScan::computePSOfreeS(ResultTable* result) const {
  result->_idTable.setCols(2);
  result->_resultTypes.push_back(ResultTable::ResultType::KB);
  result->_resultTypes.push_back(ResultTable::ResultType::KB);
  result->_sortedBy = {0, 1};
  const auto& idx = _executionContext->getIndex();
  idx.scan(_predicate, &result->_idTable, Index::Permutation::PSO,
           _timeoutTimer);
}

// _____________________________________________________________________________
void IndexScan::computePOSboundO(ResultTable* result) const {
  result->_idTable.setCols(1);
  result->_resultTypes.push_back(ResultTable::ResultType::KB);
  result->_sortedBy = {0};
  const auto& idx = _executionContext->getIndex();
  idx.scan(_predicate, _object, &result->_idTable, Index::Permutation::POS,
           _timeoutTimer);
}

// _____________________________________________________________________________
void IndexScan::computePOSfreeO(ResultTable* result) const {
  result->_idTable.setCols(2);
  result->_resultTypes.push_back(ResultTable::ResultType::KB);
  result->_resultTypes.push_back(ResultTable::ResultType::KB);
  result->_sortedBy = {0, 1};
  const auto& idx = _executionContext->getIndex();
  idx.scan(_predicate, &result->_idTable, Index::Permutation::POS,
           _timeoutTimer);
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
        return getResult()->size();
      }
    }
    // TODO<joka921> Should be a oneliner
    // getIndex().cardinality(getPermutation(), getFirstKey());
    if (_type == SPO_FREE_P || _type == SOP_FREE_O) {
      return getIndex().getCardinality(_subject, Index::Permutation::SPO);
    } else if (_type == POS_FREE_O || _type == PSO_FREE_S) {
      return getIndex().getCardinality(_predicate, Index::Permutation::PSO);
    } else if (_type == OPS_FREE_P || _type == OSP_FREE_S) {
      return getIndex().getCardinality(_object, Index::Permutation::OSP);
    }
    // The triple consists of three variables.
    return getIndex().getNofTriples();
  } else {
    // Only for test cases. The handling of the objects is to make the
    // strange query planner tests pass.
    // TODO<joka921> Code duplication.
    std::string objectStr =
        _object.isString() ? _object.getString() : _object.toString();
    std::string subjectStr =
        _subject.isString() ? _subject.getString() : _subject.toString();
    return 1000 + subjectStr.size() + _predicate.size() + objectStr.size();
  }
}

// _____________________________________________________________________________
size_t IndexScan::getCostEstimate() {
  if (getResultWidth() != 3) {
    return getSizeEstimate();
  } else {
    // This is to make unit tests pass that have no explicit execution context.
    // TODO<joka921> get rid of these unit tests!
    if (!_executionContext) {
      return getSizeEstimate();
    }
    // The computation of the `full scan` estimate must be consistent with the
    // full scan dummy joins in `Join.cpp` for correct query planning.
    // TODO<joka921> Factor out the common code to keep it in sync.

    // The following calculation is done in a way that makes materializing a
    // full index scan always more expensive than implicitly computing it in the
    // so-called "dummy joins" (see `Join.h` and `Join.cpp`). To achieve this,
    // the estimate here is computed analogously to the estimates of the dummy
    // joins, but with an additional penalty factor. The overall goal is to make
    // the query planner only materialize a full index scan if there is no other
    // way to compute the query.

    // Note that we cannot set the cost to `infinity` or `max`, because this
    // might lead to overflows in upstream operations when the cost estimate is
    // an integer (this currently is the case). When implementing them as
    // floating point numbers, a cost estimate of `infinity` would
    // remove the ability to distinguish the costs of plans that perform full
    // scans but still have different overall costs.
    // TODO<joka921> The conceptually right way to do this is to make the cost
    // estimate a tuple `(numFullIndexScans, costEstimateForRemainder)`.
    // Implement this functionality.
    size_t diskRandomAccessCost =
        _executionContext->getCostFactor("DISK_RANDOM_ACCESS_COST");
    size_t numScans = getSizeEstimate() / getMultiplicity(0);
    size_t averageScanSize = getMultiplicity(0);
    static constexpr size_t fullScanPenalty = 1'000;
    auto totalCost =
        fullScanPenalty * numScans * (diskRandomAccessCost + averageScanSize);
    return totalCost;
  }
}

// _____________________________________________________________________________
void IndexScan::computeSPOfreeP(ResultTable* result) const {
  result->_idTable.setCols(2);
  result->_resultTypes.push_back(ResultTable::ResultType::KB);
  result->_resultTypes.push_back(ResultTable::ResultType::KB);
  result->_sortedBy = {0, 1};
  const auto& idx = _executionContext->getIndex();
  idx.scan(_subject, &result->_idTable, Index::Permutation::SPO, _timeoutTimer);
}

// _____________________________________________________________________________
void IndexScan::computeSOPboundO(ResultTable* result) const {
  result->_idTable.setCols(1);
  result->_resultTypes.push_back(ResultTable::ResultType::KB);
  result->_sortedBy = {0};
  const auto& idx = _executionContext->getIndex();
  idx.scan(_subject, _object, &result->_idTable, Index::Permutation::SOP,
           _timeoutTimer);
}

// _____________________________________________________________________________
void IndexScan::computeSOPfreeO(ResultTable* result) const {
  result->_idTable.setCols(2);
  result->_resultTypes.push_back(ResultTable::ResultType::KB);
  result->_resultTypes.push_back(ResultTable::ResultType::KB);
  result->_sortedBy = {0, 1};
  const auto& idx = _executionContext->getIndex();
  idx.scan(_subject, &result->_idTable, Index::Permutation::SOP, _timeoutTimer);
}

// _____________________________________________________________________________
void IndexScan::computeOPSfreeP(ResultTable* result) const {
  result->_idTable.setCols(2);
  result->_resultTypes.push_back(ResultTable::ResultType::KB);
  result->_resultTypes.push_back(ResultTable::ResultType::KB);
  result->_sortedBy = {0, 1};
  const auto& idx = _executionContext->getIndex();
  idx.scan(_object, &result->_idTable, Index::Permutation::OPS, _timeoutTimer);
}

// _____________________________________________________________________________
void IndexScan::computeOSPfreeS(ResultTable* result) const {
  result->_idTable.setCols(2);
  result->_resultTypes.push_back(ResultTable::ResultType::KB);
  result->_resultTypes.push_back(ResultTable::ResultType::KB);
  result->_sortedBy = {0, 1};
  const auto& idx = _executionContext->getIndex();
  idx.scan(_object, &result->_idTable, Index::Permutation::OSP, _timeoutTimer);
}

// _____________________________________________________________________________
void IndexScan::determineMultiplicities() {
  _multiplicity.clear();
  if (_executionContext) {
    if (getResultWidth() == 1) {
      _multiplicity.emplace_back(1);
    } else {
      const auto& idx = getIndex();
      switch (_type) {
        case PSO_FREE_S:
          _multiplicity =
              idx.getMultiplicities(_predicate, Index::Permutation::PSO);
          break;
        case POS_FREE_O:
          _multiplicity =
              idx.getMultiplicities(_predicate, Index::Permutation::POS);
          break;
        case SPO_FREE_P:
          _multiplicity =
              idx.getMultiplicities(_subject, Index::Permutation::SPO);
          break;
        case SOP_FREE_O:
          _multiplicity =
              idx.getMultiplicities(_subject, Index::Permutation::SOP);
          break;
        case OSP_FREE_S:
          _multiplicity =
              idx.getMultiplicities(_object, Index::Permutation::OSP);
          break;
        case OPS_FREE_P:
          _multiplicity =
              idx.getMultiplicities(_object, Index::Permutation::OPS);
          break;
        case FULL_INDEX_SCAN_SPO:
          _multiplicity = idx.getMultiplicities(Index::Permutation::SPO);
          break;
        case FULL_INDEX_SCAN_SOP:
          _multiplicity = idx.getMultiplicities(Index::Permutation::SOP);
          break;
        case FULL_INDEX_SCAN_PSO:
          _multiplicity = idx.getMultiplicities(Index::Permutation::PSO);
          break;
        case FULL_INDEX_SCAN_POS:
          _multiplicity = idx.getMultiplicities(Index::Permutation::POS);
          break;
        case FULL_INDEX_SCAN_OSP:
          _multiplicity = idx.getMultiplicities(Index::Permutation::OSP);
          break;
        case FULL_INDEX_SCAN_OPS:
          _multiplicity = idx.getMultiplicities(Index::Permutation::OPS);
          break;
        default:
          AD_THROW(ad_semsearch::Exception::ASSERT_FAILED,
                   "Switch reached default block unexpectedly!");
      }
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

void IndexScan::computeFullScan(ResultTable* result,
                                const Index::Permutation permutation) const {
  std::vector<std::pair<Id, Id>> ignoredRanges;

  auto literalRange = getIndex().getVocab().prefix_range("\"");
  auto taggedPredicatesRange = getIndex().getVocab().prefix_range("@");
  VocabIndex languagePredicateIndex;
  bool success =
      getIndex().getVocab().getId(LANGUAGE_PREDICATE, &languagePredicateIndex);
  AD_CHECK(success);

  using enum Index::Permutation;
  // TODO<joka921> lift `prefixRange` to Index and ID
  if (permutation == SPO || permutation == SOP) {
    ignoredRanges.push_back({Id::makeFromVocabIndex(literalRange.first),
                             Id::makeFromVocabIndex(literalRange.second)});
  } else if (permutation == PSO || permutation == POS) {
    ignoredRanges.push_back(
        {Id::makeFromVocabIndex(taggedPredicatesRange.first),
         Id::makeFromVocabIndex(taggedPredicatesRange.second)});
    ignoredRanges.emplace_back(
        Id::makeFromVocabIndex(languagePredicateIndex),
        Id::makeFromVocabIndex(languagePredicateIndex.incremented()));
  }

  auto isTripleIgnored = [&](const auto& triple) {
    if (permutation == SPO || permutation == OPS) {
      // Predicates are always entities from the vocabulary.
      auto id = triple[1].getVocabIndex();
      return id == languagePredicateIndex ||
             (id >= taggedPredicatesRange.first &&
              id < taggedPredicatesRange.second);
    } else if (permutation == SOP || permutation == OSP) {
      // Predicates are always entities from the vocabulary.
      auto id = triple[2].getVocabIndex();
      return id == languagePredicateIndex ||
             (id >= taggedPredicatesRange.first &&
              id < taggedPredicatesRange.second);
    }
    return false;
  };

  result->_idTable.setCols(3);
  result->_resultTypes.push_back(ResultTable::ResultType::KB);
  result->_resultTypes.push_back(ResultTable::ResultType::KB);
  result->_resultTypes.push_back(ResultTable::ResultType::KB);
  result->_sortedBy = {0, 1, 2};

  uint64_t resultSize = getIndex().getNofTriples();
  if (getLimit().has_value() && getLimit() < resultSize) {
    resultSize = getLimit().value();
  }
  result->_idTable.reserve(resultSize);
  auto table = result->_idTable.moveToStatic<3>();
  size_t i = 0;
  auto triplesView =
      getExecutionContext()->getIndex().getImpl().applyToPermutation(
          permutation, [&](const auto& p) {
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
  result->_idTable = table.moveToDynamic();
}
