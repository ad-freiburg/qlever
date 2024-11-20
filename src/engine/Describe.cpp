// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "engine/Describe.h"

#include "../../test/engine/ValuesForTesting.h"
#include "engine/IndexScan.h"
#include "engine/Join.h"
#include "engine/Values.h"

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
  // The cache key must repesent the `resources_` as well as the `subtree_`.
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

// A helper function for the recursive BFS. Return the subset of `input` (as an
// `IdTable` with one column) that fulfills  the following properties:
// 1. The ID is a blank node
// 2. The ID is not part of `alreadySeen`.
// The returned IDs are then also added to `alreadySeen`. The result contains no
// duplicates.
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
    IdTable& finalResult, ad_utility::HashSetWithMemoryLimit<Id>& alreadySeen,
    IdTable blankNodes) {
  AD_CORRECTNESS_CHECK(blankNodes.numColumns() == 1);

  // Stop condition for the recursion, no new start nodes found.
  if (blankNodes.empty()) {
    return;
  }

  // Set up a join between the `blankNodes` and the full index.
  using V = Variable;
  auto subjectVar = V{"?subject"};
  SparqlTripleSimple triple{subjectVar, V{"?predicate"}, V{"?object"}};
  auto indexScan = ad_utility::makeExecutionTree<IndexScan>(
      getExecutionContext(), Permutation::SPO, triple);
  auto valuesOp = ad_utility::makeExecutionTree<ValuesForTesting>(
      getExecutionContext(), std::move(blankNodes),
      std::vector<std::optional<Variable>>{subjectVar});

  auto joinColValues = valuesOp->getVariableColumn(subjectVar);
  auto joinColScan = indexScan->getVariableColumn(subjectVar);

  auto join = ad_utility::makeExecutionTree<Join>(
      getExecutionContext(), std::move(valuesOp), std::move(indexScan),
      joinColValues, joinColScan);

  auto result = join->getResult();
  // TODO<joka921, RobinTF> As soon as the join is lazy, we can compute the
  // result lazy, and therefore avoid the copy via `clone` of the IdTable.
  auto table = result->idTable().clone();
  using CI = ColumnIndex;
  CI s = join->getVariableColumn(V{"?subject"});
  CI p = join->getVariableColumn(V{"?predicate"});
  CI o = join->getVariableColumn(V{"?object"});
  table.setColumnSubset(std::vector{s, p, o});
  // TODO<joka921> Make the result of DESCRIBE lazy, then we avoid the
  // additional Copying here.
  finalResult.insertAtEnd(table);

  // Compute the set of newly found blank nodes and recurse.
  // Note: The stop condition is at the beginning of the recursive call.
  auto newBlankNodes =
      getNewBlankNodes(allocator(), alreadySeen, table.getColumn(2));
  recursivelyAddBlankNodes(finalResult, alreadySeen, std::move(newBlankNodes));
}

// _____________________________________________________________________________
ProtoResult Describe::computeResult([[maybe_unused]] bool requestLaziness) {
  std::vector<TripleComponent> valuesToDescribe;
  for (const auto& resource : describe_.resources_) {
    if (std::holds_alternative<TripleComponent::Iri>(resource)) {
      valuesToDescribe.push_back(std::get<TripleComponent::Iri>(resource));
    } else {
      // TODO<joka921> Implement this, it should be fairly simple.
      throw std::runtime_error("DESCRIBE with a variable is not yet supported");
    }
  }
  parsedQuery::SparqlValues values;
  using V = Variable;
  Variable subjectVar = V{"?subject"};
  values._variables.push_back(subjectVar);

  // Set up and execute a JOIN between the described IRIs and the full index.
  // TODO<joka921> There is a lot of code duplication in the following block
  // with the recursive BFS on the blank nodes, factor that out.
  for (const auto& v : valuesToDescribe) {
    values._values.push_back(std::vector{v});
  }
  SparqlTripleSimple triple{subjectVar, V{"?predicate"}, V{"?object"}};
  auto indexScan = ad_utility::makeExecutionTree<IndexScan>(
      getExecutionContext(), Permutation::SPO, triple);
  auto valuesOp =
      ad_utility::makeExecutionTree<Values>(getExecutionContext(), values);
  auto joinColValues = valuesOp->getVariableColumn(subjectVar);
  auto joinColScan = indexScan->getVariableColumn(subjectVar);

  // TODO<joka921> We have to (here, as well as in the recursion) also respect
  // the GRAPHS by which the result is filtered ( + add unit tests for that
  // case).
  auto join = ad_utility::makeExecutionTree<Join>(
      getExecutionContext(), std::move(valuesOp), std::move(indexScan),
      joinColValues, joinColScan);

  // TODO<joka921> The following code (which extracts the required columns and
  // writes the initial triples) is also duplicated, factor it out.
  auto result = join->getResult();
  IdTable resultTable = result->idTable().clone();

  using CI = ColumnIndex;
  CI s = join->getVariableColumn(V{"?subject"});
  CI p = join->getVariableColumn(V{"?predicate"});
  CI o = join->getVariableColumn(V{"?object"});

  resultTable.setColumnSubset(std::vector{s, p, o});

  // Recursively follow all blank nodes.
  ad_utility::HashSetWithMemoryLimit<Id> alreadySeen{allocator()};
  auto blankNodes =
      getNewBlankNodes(allocator(), alreadySeen, resultTable.getColumn(2));
  recursivelyAddBlankNodes(resultTable, alreadySeen, std::move(blankNodes));
  return {std::move(resultTable), resultSortedOn(),
          result->getSharedLocalVocab()};
}
