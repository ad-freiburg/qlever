// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "engine/Describe.h"

#include "../../test/engine/ValuesForTesting.h"
#include "engine/IndexScan.h"
#include "engine/Join.h"

// _____________________________________________________________________________
Describe::Describe(QueryExecutionContext* qec,
                   std::shared_ptr<QueryExecutionTree> subtree,
                   parsedQuery::Describe describe)
    : Operation{qec},
      subtree_{std::move(subtree)},
      describe_{std::move(describe)} {
  AD_CORRECTNESS_CHECK(subtree_ != nullptr);
}

// _____________________________________________________________________________
std::vector<QueryExecutionTree*> Describe::getChildren() {
  return {subtree_.get()};
}

// _____________________________________________________________________________
string Describe::getCacheKeyImpl() const {
  // The cache key must represent the `resources_` as well as the `subtree_`.
  std::string resourceKey;
  for (const auto& resource : describe_.resources_) {
    if (std::holds_alternative<TripleComponent::Iri>(resource)) {
      resourceKey.append(
          std::get<TripleComponent::Iri>(resource).toStringRepresentation());
    } else {
      resourceKey.append(absl::StrCat(
          "column #",
          subtree_->getVariableColumnOrNullopt(std::get<Variable>(resource))
              .value_or(static_cast<size_t>(-1)),
          " "));
    }
  }
  // TODO: Add the graphs from `describe_.datasetClauses_` to the cache key.
  return absl::StrCat("DESCRIBE ", subtree_->getCacheKey(), resourceKey);
}

// _____________________________________________________________________________
string Describe::getDescriptor() const { return "DESCRIBE"; }

// _____________________________________________________________________________
size_t Describe::getResultWidth() const { return 3; }

// As DESCRIBE is never part of the query planning (it is always the root
// operation), we can return dummy values for the following functions.
size_t Describe::getCostEstimate() { return 2 * subtree_->getCostEstimate(); }

uint64_t Describe::getSizeEstimateBeforeLimit() {
  return subtree_->getSizeEstimate() * 2;
}

float Describe::getMultiplicity([[maybe_unused]] size_t col) { return 1.0f; }

bool Describe::knownEmptyResult() { return false; }

// The result cannot easily be sorted, as it involves recursive expanding of
// graphs.
vector<ColumnIndex> Describe::resultSortedOn() const { return {}; }

// The result always consists of three hardcoded variables `?subject`,
// `?predicate`, `?object`. Note: The variable names must be in sync with the
// implicit CONSTRUCT query created by the parser (see
// `SparqlQleverVisitor::visitDescribe`) for details.
VariableToColumnMap Describe::computeVariableToColumnMap() const {
  using V = Variable;
  auto col = makeAlwaysDefinedColumn;
  return {{V("?subject"), col(0)},
          {V("?predicate"), col(1)},
          {V("?object"), col(2)}};
}

// A helper function for the recursive BFS. Return those `Id`s from `input` (an
// `IdTable` with one column) that are blank nodes and not in `alreadySeen`,
// with duplicates removed. The retuned `Id`s are added to `alreadySeen`.
static IdTable getNewBlankNodes(
    const auto& allocator, ad_utility::HashSetWithMemoryLimit<Id>& alreadySeen,
    std::span<Id> input) {
  IdTable result{1, allocator};
  result.resize(input.size());
  decltype(auto) col = result.getColumn(0);
  size_t i = 0;
  for (Id id : input) {
    if (id.getDatatype() != Datatype::BlankNodeIndex) {
      continue;
    }
    auto [it, isNew] = alreadySeen.emplace(id);
    if (!isNew) {
      continue;
    }
    col[i] = id;
    ++i;
  }
  result.resize(i);
  return result;
}

// _____________________________________________________________________________
void Describe::recursivelyAddBlankNodes(
    IdTable& finalResult, LocalVocab& localVocab,
    ad_utility::HashSetWithMemoryLimit<Id>& alreadySeen, IdTable blankNodes) {
  AD_CORRECTNESS_CHECK(blankNodes.numColumns() == 1);

  // Stop condition for the recursion, no new start nodes found.
  if (blankNodes.empty()) {
    return;
  }
  auto table =
      makeAndExecuteJoinWithFullIndex(std::move(blankNodes), localVocab);

  // TODO<joka921> Make the result of DESCRIBE lazy, then we avoid the
  // additional Copying here.
  finalResult.insertAtEnd(table);

  // Compute the set of newly found blank nodes and recurse.
  // Note: The stop condition is at the beginning of the recursive call.
  auto newBlankNodes =
      getNewBlankNodes(allocator(), alreadySeen, table.getColumn(2));
  recursivelyAddBlankNodes(finalResult, localVocab, alreadySeen,
                           std::move(newBlankNodes));
}

// _____________________________________________________________________________
IdTable Describe::makeAndExecuteJoinWithFullIndex(
    IdTable input, LocalVocab& localVocab) const {
  AD_CORRECTNESS_CHECK(input.numColumns() == 1);
  using V = Variable;
  auto subjectVar = V{"?subject"};
  auto valuesOp = ad_utility::makeExecutionTree<ValuesForTesting>(
      getExecutionContext(), std::move(input),
      std::vector<std::optional<Variable>>{subjectVar});
  SparqlTripleSimple triple{subjectVar, V{"?predicate"}, V{"?object"}};
  auto indexScan = ad_utility::makeExecutionTree<IndexScan>(
      getExecutionContext(), Permutation::SPO, triple,
      describe_.datasetClauses_.defaultGraphs_);
  auto joinColValues = valuesOp->getVariableColumn(subjectVar);
  auto joinColScan = indexScan->getVariableColumn(subjectVar);

  auto join = ad_utility::makeExecutionTree<Join>(
      getExecutionContext(), std::move(valuesOp), std::move(indexScan),
      joinColValues, joinColScan);

  auto result = join->getResult();
  IdTable resultTable = result->idTable().clone();

  using CI = ColumnIndex;
  CI s = join->getVariableColumn(V{"?subject"});
  CI p = join->getVariableColumn(V{"?predicate"});
  CI o = join->getVariableColumn(V{"?object"});

  resultTable.setColumnSubset(std::vector{s, p, o});
  localVocab.mergeWith(std::span{&result->localVocab(), 1});
  return resultTable;
}

// _____________________________________________________________________________
IdTable Describe::getIdsToDescribe(const Result& result,
                                   LocalVocab& localVocab) const {
  ad_utility::HashSetWithMemoryLimit<Id> idsToDescribe{allocator()};
  const auto& vocab = getIndex().getVocab();
  for (const auto& resource : describe_.resources_) {
    if (std::holds_alternative<TripleComponent::Iri>(resource)) {
      idsToDescribe.insert(
          TripleComponent{std::get<TripleComponent::Iri>(resource)}.toValueId(
              vocab, localVocab));
    } else {
      auto var = std::get<Variable>(resource);
      auto column = subtree_->getVariableColumnOrNullopt(var);
      if (!column.has_value()) {
        continue;
      }
      for (Id id : result.idTable().getColumn(column.value())) {
        idsToDescribe.insert(id);
      }
    }
  }
  IdTable idsAsTable{1, allocator()};
  idsAsTable.resize(idsToDescribe.size());
  std::ranges::copy(idsToDescribe, idsAsTable.getColumn(0).begin());
  return idsAsTable;
}

// _____________________________________________________________________________
ProtoResult Describe::computeResult([[maybe_unused]] bool requestLaziness) {
  LocalVocab localVocab;
  // TODO<joka921> Do we benefit from laziness here? Probably not, because we
  // have to deduplicate the whole input anyway.
  auto subresult = subtree_->getResult();
  auto idsAsTable = getIdsToDescribe(*subresult, localVocab);

  auto resultTable =
      makeAndExecuteJoinWithFullIndex(std::move(idsAsTable), localVocab);

  // Recursively follow all blank nodes.
  ad_utility::HashSetWithMemoryLimit<Id> alreadySeen{allocator()};
  auto blankNodes =
      getNewBlankNodes(allocator(), alreadySeen, resultTable.getColumn(2));
  recursivelyAddBlankNodes(resultTable, localVocab, alreadySeen,
                           std::move(blankNodes));

  return {std::move(resultTable), resultSortedOn(), std::move(localVocab)};
}
