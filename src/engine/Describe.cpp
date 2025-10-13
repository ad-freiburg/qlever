// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "engine/Describe.h"

#include <absl/strings/str_join.h>

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
  // If the DESCRIBE query has no WHERE clause, `subtree_` is the neutral
  // element, but never `nullptr`.
  AD_CORRECTNESS_CHECK(subtree_ != nullptr);
}

// _____________________________________________________________________________
std::vector<QueryExecutionTree*> Describe::getChildren() {
  return {subtree_.get()};
}

// _____________________________________________________________________________
std::string Describe::getCacheKeyImpl() const {
  // The cache key must represent the `resources_` (the variables and IRIs of
  // the DESCRIBE clause) and the `subtree_` (the WHERE clause).
  std::string result = absl::StrCat("DESCRIBE ", subtree_->getCacheKey(), " ");
  for (const auto& resource : describe_.resources_) {
    if (std::holds_alternative<TripleComponent::Iri>(resource)) {
      result.append(
          std::get<TripleComponent::Iri>(resource).toStringRepresentation());
    } else {
      result.append(absl::StrCat(
          "column #",
          subtree_->getVariableColumnOrNullopt(std::get<Variable>(resource))
              .value_or(static_cast<size_t>(-1)),
          " "));
    }
  }

  // Add the names of the default graphs (from the FROM clauses) to the cache
  // key, in a deterministic order.
  //
  // NOTE: The default and named graphs are also part of the cache key of the
  // `subtree_`. However, the named graphs only determine the result for
  // `subtree_` (the resources to be described), whereas the default graphs
  // also determine which triples for these resources become part of the result.
  const auto& defaultGraphs = describe_.datasetClauses_.activeDefaultGraphs();
  if (defaultGraphs.has_value()) {
    std::vector<std::string> graphIdVec;
    ql::ranges::transform(defaultGraphs.value(), std::back_inserter(graphIdVec),
                          &TripleComponent::toRdfLiteral);
    ql::ranges::sort(graphIdVec);
    absl::StrAppend(&result,
                    "\nFiltered by Graphs:", absl::StrJoin(graphIdVec, " "));
  }
  return result;
}

// _____________________________________________________________________________
std::string Describe::getDescriptor() const { return "DESCRIBE"; }

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
std::vector<ColumnIndex> Describe::resultSortedOn() const { return {}; }

// The result always has three variables `?subject`, `?predicate`, `?object`.
//
// NOTE: These variable names are hardcoded in the implicit CONSTRUCT query
// created in `SparqlQleverVisitor::visitDescribe`.
VariableToColumnMap Describe::computeVariableToColumnMap() const {
  using V = Variable;
  auto col = makeAlwaysDefinedColumn;
  return {{V("?subject"), col(0)},
          {V("?predicate"), col(1)},
          {V("?object"), col(2)}};
}

// A helper function for the recursive BFS. Return those `Id`s from `input` (an
// `IdTable` with one column) that are blank nodes and not in `alreadySeen`,
// with duplicates removed. The returned `Id`s are added to `alreadySeen`.
template <typename Allocator>
static IdTable getNewBlankNodes(
    const Allocator& allocator,
    ad_utility::HashSetWithMemoryLimit<Id>& alreadySeen, ql::span<Id> input) {
  IdTable result{1, allocator};
  result.resize(input.size());
  decltype(auto) resultColumn = result.getColumn(0);
  size_t i = 0;
  for (Id id : input) {
    if (id.getDatatype() != Datatype::BlankNodeIndex) {
      continue;
    }
    auto [it, isNew] = alreadySeen.emplace(id);
    if (!isNew) {
      continue;
    }
    resultColumn[i] = id;
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

  // If there are no more `blankNodes` to explore, we are done.
  if (blankNodes.empty()) {
    return;
  }

  // Expand the `blankNodes` by joining them with the full index and add the
  // resulting triples to the `finalResult`.
  //
  // TODO<joka921> Make the result of DESCRIBE lazy, then we can avoid the
  // additional copy here.
  auto table =
      makeAndExecuteJoinWithFullIndex(std::move(blankNodes), localVocab);
  finalResult.insertAtEnd(table);

  // Compute the set of newly found blank nodes and recurse.
  auto newBlankNodes =
      getNewBlankNodes(allocator(), alreadySeen, table.getColumn(2));
  recursivelyAddBlankNodes(finalResult, localVocab, alreadySeen,
                           std::move(newBlankNodes));
}

// _____________________________________________________________________________
IdTable Describe::makeAndExecuteJoinWithFullIndex(
    IdTable input, LocalVocab& localVocab) const {
  AD_CORRECTNESS_CHECK(input.numColumns() == 1);

  // Create a `Join` operation that joins `input` (with column `?subject`) with
  // the full index (with columns `?subject`, `?predicate`, `?object`) on the
  // `?subject` column.
  using V = Variable;
  auto subjectVar = V{"?subject"};
  auto valuesOp = ad_utility::makeExecutionTree<ValuesForTesting>(
      getExecutionContext(), std::move(input),
      std::vector<std::optional<Variable>>{subjectVar});
  SparqlTripleSimple triple{subjectVar, V{"?predicate"}, V{"?object"}};
  auto activeGraphs = describe_.datasetClauses_.activeDefaultGraphs();
  auto indexScan = ad_utility::makeExecutionTree<IndexScan>(
      getExecutionContext(), Permutation::SPO, triple,
      activeGraphs.has_value()
          ? IndexScan::Graphs::Whitelist(std::move(activeGraphs).value())
          : IndexScan::Graphs::All());
  auto joinColValues = valuesOp->getVariableColumn(subjectVar);
  auto joinColScan = indexScan->getVariableColumn(subjectVar);
  auto join = ad_utility::makeExecutionTree<Join>(
      getExecutionContext(), std::move(valuesOp), std::move(indexScan),
      joinColValues, joinColScan);

  // Compute the result of the `join` and select the columns `?subject`,
  // `?predicate`, `?object`.
  //
  // NOTE: Typically, the join result has already those exact columns, in which
  // case the `selectColumns` operation is a no-op. Note sure when this is not
  // the case, but better safe than sorry.
  auto result = join->getResult();
  IdTable resultTable = result->idTable().clone();
  ColumnIndex s = join->getVariableColumn(V{"?subject"});
  ColumnIndex p = join->getVariableColumn(V{"?predicate"});
  ColumnIndex o = join->getVariableColumn(V{"?object"});
  resultTable.setColumnSubset(std::vector{s, p, o});

  // The `indexScan` might have added some delta triples with local vocab IDs,
  // so make sure to merge them into the `localVocab`.
  localVocab.mergeWith(result->localVocab());

  return resultTable;
}

// _____________________________________________________________________________
IdTable Describe::getIdsToDescribe(const Result& result,
                                   LocalVocab& localVocab) const {
  // First collect the `Id`s in a hash set, in order to remove duplicates.
  ad_utility::HashSetWithMemoryLimit<Id> idsToDescribe{allocator()};
  const auto& vocab = getIndex().getVocab();
  for (const auto& resource : describe_.resources_) {
    if (std::holds_alternative<TripleComponent::Iri>(resource)) {
      // For an IRI, add the corresponding ID to `idsToDescribe`.
      idsToDescribe.insert(
          TripleComponent{std::get<TripleComponent::Iri>(resource)}.toValueId(
              vocab, localVocab, getIndex().encodedIriManager()));
    } else {
      // For a variable, add all IDs that match the variable in the `result` of
      // the WHERE clause to `idsToDescribe`.
      const auto& var = std::get<Variable>(resource);
      auto column = subtree_->getVariableColumnOrNullopt(var);
      if (!column.has_value()) {
        continue;
      }
      for (Id id : result.idTable().getColumn(column.value())) {
        idsToDescribe.insert(id);
      }
    }
  }

  // Copy the `Id`s from the hash set to an `IdTable`.
  IdTable idsAsTable{1, allocator()};
  idsAsTable.resize(idsToDescribe.size());
  ql::ranges::copy(idsToDescribe, idsAsTable.getColumn(0).begin());
  return idsAsTable;
}

// _____________________________________________________________________________
Result Describe::computeResult([[maybe_unused]] bool requestLaziness) {
  LocalVocab localVocab;
  // Compute the results of the WHERE clause and extract the `Id`s to describe.
  //
  // TODO<joka921> Would we benefit from computing `resultOfWhereClause` lazily?
  // Probably not, because we have to deduplicate the whole input anyway.
  auto resultOfWhereClause = subtree_->getResult();
  auto idsAsTable = getIdsToDescribe(*resultOfWhereClause, localVocab);

  // Get all triples with the `Id`s as subject.
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

// _____________________________________________________________________________
std::unique_ptr<Operation> Describe::cloneImpl() const {
  return std::make_unique<Describe>(_executionContext, subtree_->clone(),
                                    describe_);
}
