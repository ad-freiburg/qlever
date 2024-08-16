// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "engine/IndexScan.h"

#include <absl/strings/str_join.h>

#include <boost/optional.hpp>
#include <sstream>
#include <string>

#include "index/IndexImpl.h"
#include "index/TriplesView.h"
#include "parser/ParsedQuery.h"

using std::string;

// _____________________________________________________________________________
IndexScan::IndexScan(QueryExecutionContext* qec, Permutation::Enum permutation,
                     const SparqlTripleSimple& triple)
    : Operation(qec),
      permutation_(permutation),
      subject_(triple.s_),
      predicate_(triple.p_),
      object_(triple.o_),
      numVariables_(static_cast<size_t>(subject_.isVariable()) +
                    static_cast<size_t>(predicate_.isVariable()) +
                    static_cast<size_t>(object_.isVariable())) {
  // We previously had `nullptr`s here in unit tests. This is no longer
  // necessary nor allowed.
  AD_CONTRACT_CHECK(qec != nullptr);
  for (auto& [idx, variable] : triple.additionalScanColumns_) {
    additionalColumns_.push_back(idx);
    additionalVariables_.push_back(variable);
  }
  sizeEstimate_ = computeSizeEstimate();

  // Check the following invariant: All the variables must be at the end of the
  // permuted triple. For example in the PSO permutation, either only the O, or
  // the S and O, or all three of P, S, O, or none of them can be variables, all
  // other combinations are not supported.
  auto permutedTriple = getPermutedTriple();
  for (size_t i = 0; i < 3 - numVariables_; ++i) {
    AD_CONTRACT_CHECK(!permutedTriple.at(i)->isVariable());
  }
  for (size_t i = 3 - numVariables_; i < permutedTriple.size(); ++i) {
    AD_CONTRACT_CHECK(permutedTriple.at(i)->isVariable());
  }
}

// _____________________________________________________________________________
IndexScan::IndexScan(QueryExecutionContext* qec, Permutation::Enum permutation,
                     const SparqlTriple& triple)
    : IndexScan(qec, permutation, triple.getSimple()) {}

// _____________________________________________________________________________
string IndexScan::getCacheKeyImpl() const {
  std::ostringstream os;
  auto permutationString = Permutation::toString(permutation_);

  if (numVariables_ == 3) {
    os << "SCAN FOR FULL INDEX " << permutationString;

  } else {
    os << "SCAN " << permutationString << " with ";
    auto addKey = [&os, &permutationString, this](size_t idx) {
      auto keyString = permutationString.at(idx);
      const auto& key = getPermutedTriple().at(idx)->toRdfLiteral();
      os << keyString << " = \"" << key << "\"";
    };
    for (size_t i = 0; i < 3 - numVariables_; ++i) {
      addKey(i);
      os << ", ";
    }
  }
  if (!additionalColumns_.empty()) {
    os << " Additional Columns: ";
    os << absl::StrJoin(additionalColumns(), " ");
  }
  return std::move(os).str();
}

// _____________________________________________________________________________
string IndexScan::getDescriptor() const {
  return "IndexScan " + subject_.toString() + " " + predicate_.toString() +
         " " + object_.toString();
}

// _____________________________________________________________________________
size_t IndexScan::getResultWidth() const {
  return numVariables_ + additionalVariables_.size();
}

// _____________________________________________________________________________
vector<ColumnIndex> IndexScan::resultSortedOn() const {
  auto resAsView = ad_utility::integerRange(ColumnIndex{numVariables_});
  std::vector<ColumnIndex> result{resAsView.begin(), resAsView.end()};
  for (size_t i = 0; i < additionalColumns_.size(); ++i) {
    if (additionalColumns_.at(i) == ADDITIONAL_COLUMN_GRAPH_ID) {
      result.push_back(numVariables_ + i);
    }
  }
  return result;
}

// _____________________________________________________________________________
VariableToColumnMap IndexScan::computeVariableToColumnMap() const {
  VariableToColumnMap variableToColumnMap;
  auto addCol = [&variableToColumnMap,
                 nextColIdx = ColumnIndex{0}](const Variable& var) mutable {
    // All the columns of an index scan only contain defined values.
    variableToColumnMap[var] = makeAlwaysDefinedColumn(nextColIdx);
    ++nextColIdx;
  };

  for (const TripleComponent* const ptr : getPermutedTriple()) {
    if (ptr->isVariable()) {
      addCol(ptr->getVariable());
    }
  }
  std::ranges::for_each(additionalVariables_, addCol);
  return variableToColumnMap;
}
// _____________________________________________________________________________
ProtoResult IndexScan::computeResult([[maybe_unused]] bool requestLaziness) {
  LOG(DEBUG) << "IndexScan result computation...\n";
  IdTable idTable{getExecutionContext()->getAllocator()};

  using enum Permutation::Enum;
  idTable.setNumColumns(numVariables_);
  const auto& index = _executionContext->getIndex();
  if (numVariables_ < 3) {
    idTable = index.scan(getScanSpecification(), permutation_,
                         additionalColumns(), cancellationHandle_, getLimit());
  } else {
    AD_CORRECTNESS_CHECK(numVariables_ == 3);
    computeFullScan(&idTable, permutation_);
  }
  AD_CORRECTNESS_CHECK(idTable.numColumns() == getResultWidth());
  LOG(DEBUG) << "IndexScan result computation done.\n";
  checkCancellation();

  return {std::move(idTable), resultSortedOn(), LocalVocab{}};
}

// _____________________________________________________________________________
size_t IndexScan::computeSizeEstimate() const {
  AD_CORRECTNESS_CHECK(_executionContext);
  // We have to do a simple scan anyway so might as well do it now
  if (numVariables_ < 3) {
    return getIndex().getResultSizeOfScan(getScanSpecification(), permutation_);
  } else {
    // The triple consists of three variables.
    // TODO<joka921> As soon as all implementations of a full index scan
    // (Including the "dummy joins" in Join.cpp) consistently exclude the
    // internal triples, this estimate should be changed to only return
    // the number of triples in the actual knowledge graph (excluding the
    // internal triples).
    AD_CORRECTNESS_CHECK(numVariables_ == 3);
    return getIndex().numTriples().normalAndInternal_();
  }
}

// _____________________________________________________________________________
size_t IndexScan::getCostEstimate() {
  // If we have a limit present, we only have to read the first
  // `limit + offset` elements.
  return getLimit().upperBound(getSizeEstimateBeforeLimit());
}

// _____________________________________________________________________________
void IndexScan::determineMultiplicities() {
  multiplicity_ = [this]() -> std::vector<float> {
    const auto& idx = getIndex();
    if (numVariables_ == 0) {
      return {};
    } else if (numVariables_ == 1) {
      // There are no duplicate triples in RDF and two elements are fixed.
      return {1.0f};
    } else if (numVariables_ == 2) {
      return idx.getMultiplicities(*getPermutedTriple()[0], permutation_);
    } else {
      AD_CORRECTNESS_CHECK(numVariables_ == 3);
      return idx.getMultiplicities(permutation_);
    }
  }();
  for ([[maybe_unused]] size_t i :
       std::views::iota(multiplicity_.size(), getResultWidth())) {
    multiplicity_.emplace_back(1);
  }
  AD_CONTRACT_CHECK(multiplicity_.size() == getResultWidth());
}

// ________________________________________________________________________
void IndexScan::computeFullScan(IdTable* result,
                                const Permutation::Enum permutation) const {
  auto [ignoredRanges, isTripleIgnored] =
      getIndex().getImpl().getIgnoredIdRanges(permutation);

  result->setNumColumns(3);

  // This implementation computes the complete knowledge graph, except the
  // internal triples.
  uint64_t resultSize = getIndex().numTriples().normal;
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
  auto triplesView = TriplesView(permutationImpl, cancellationHandle_,
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

// ___________________________________________________________________________
ScanSpecificationAsTripleComponent IndexScan::getScanSpecification() const {
  auto permutedTriple = getPermutedTriple();
  return {*permutedTriple[0], *permutedTriple[1], *permutedTriple[2]};
}

// ___________________________________________________________________________
Permutation::IdTableGenerator IndexScan::getLazyScan(
    const IndexScan& s, std::vector<CompressedBlockMetadata> blocks) {
  const IndexImpl& index = s.getIndex().getImpl();
  std::optional<Id> col0Id;
  if (s.numVariables_ < 3) {
    col0Id = s.getPermutedTriple()[0]->toValueId(index.getVocab()).value();
  }
  std::optional<Id> col1Id;
  if (s.numVariables_ < 2) {
    col1Id = s.getPermutedTriple()[1]->toValueId(index.getVocab()).value();
  }

  // This function is currently only called by the `getLazyScanForJoin...`
  // functions. In these cases we always have at least one variable in each of
  // the scans, because otherwise there would be no join column.
  AD_CORRECTNESS_CHECK(s.numVariables_ >= 1);
  // If there is a LIMIT or OFFSET clause that constrains the scan
  // (which can happen with an explicit subquery), we cannot use the prefiltered
  // blocks, as we currently have no mechanism to include limits and offsets
  // into the prefiltering (`std::nullopt` means `scan all blocks`).
  auto actualBlocks = s.getLimit().isUnconstrained()
                          ? std::optional{std::move(blocks)}
                          : std::nullopt;

  return index.getPermutation(s.permutation())
      .lazyScan({col0Id, col1Id, std::nullopt}, std::move(actualBlocks),
                s.additionalColumns(), s.cancellationHandle_, s.getLimit());
};

// ________________________________________________________________
std::optional<Permutation::MetadataAndBlocks> IndexScan::getMetadataForScan(
    const IndexScan& s) {
  const auto& index = s.getExecutionContext()->getIndex().getImpl();
  auto scanSpec = s.getScanSpecification().toScanSpecification(index);
  if (!scanSpec.has_value()) {
    return std::nullopt;
  }
  return index.getPermutation(s.permutation())
      .getMetadataAndBlocks(scanSpec.value());
};

// ________________________________________________________________
std::array<Permutation::IdTableGenerator, 2>
IndexScan::lazyScanForJoinOfTwoScans(const IndexScan& s1, const IndexScan& s2) {
  AD_CONTRACT_CHECK(s1.numVariables_ <= 3 && s2.numVariables_ <= 3);
  AD_CONTRACT_CHECK(s1.numVariables_ >= 1 && s2.numVariables_ >= 1);

  // This function only works for single column joins. This means that the first
  // variable of both scans must be equal, but all other variables of the scans
  // (if present) must be different.
  const auto& getFirstVariable = [](const IndexScan& scan) {
    auto numVars = scan.numVariables();
    AD_CORRECTNESS_CHECK(numVars <= 3);
    size_t indexOfFirstVar = 3 - numVars;
    ad_utility::HashSet<Variable> otherVars;
    for (size_t i = indexOfFirstVar + 1; i < 3; ++i) {
      const auto& el = *scan.getPermutedTriple()[i];
      if (el.isVariable()) {
        otherVars.insert(el.getVariable());
      }
    }
    return std::pair{*scan.getPermutedTriple()[3 - numVars],
                     std::move(otherVars)};
  };

  auto [first1, other1] = getFirstVariable(s1);
  auto [first2, other2] = getFirstVariable(s2);
  AD_CONTRACT_CHECK(first1 == first2);

  size_t numTotal = other1.size() + other2.size();
  for (auto& var : other1) {
    other2.insert(var);
  }
  AD_CONTRACT_CHECK(other2.size() == numTotal);

  auto metaBlocks1 = getMetadataForScan(s1);
  auto metaBlocks2 = getMetadataForScan(s2);

  if (!metaBlocks1.has_value() || !metaBlocks2.has_value()) {
    return {{}};
  }
  auto [blocks1, blocks2] = CompressedRelationReader::getBlocksForJoin(
      metaBlocks1.value(), metaBlocks2.value());

  std::array result{getLazyScan(s1, blocks1), getLazyScan(s2, blocks2)};
  result[0].details().numBlocksAll_ = metaBlocks1.value().blockMetadata_.size();
  result[1].details().numBlocksAll_ = metaBlocks2.value().blockMetadata_.size();
  return result;
}

// ________________________________________________________________
Permutation::IdTableGenerator IndexScan::lazyScanForJoinOfColumnWithScan(
    std::span<const Id> joinColumn, const IndexScan& s) {
  AD_EXPENSIVE_CHECK(std::ranges::is_sorted(joinColumn));
  AD_CORRECTNESS_CHECK(s.numVariables_ <= 3 && s.numVariables_ > 0);

  auto metaBlocks1 = getMetadataForScan(s);

  if (!metaBlocks1.has_value()) {
    return {};
  }
  auto blocks = CompressedRelationReader::getBlocksForJoin(joinColumn,
                                                           metaBlocks1.value());

  auto result = getLazyScan(s, blocks);
  result.details().numBlocksAll_ = metaBlocks1.value().blockMetadata_.size();
  return result;
}
