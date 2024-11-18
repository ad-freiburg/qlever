// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "engine/Describe.h"

#include "../../test/engine/ValuesForTesting.h"
#include "engine/IndexScan.h"
#include "engine/Join.h"
#include "engine/Values.h"

Describe::Describe(QueryExecutionContext* qec,
                   std::shared_ptr<QueryExecutionTree> subtree,
                   parsedQuery::Describe describe)
    : Operation{qec},
      subtree_{std::move(subtree)},
      describe_{std::move(describe)} {
  AD_CORRECTNESS_CHECK(subtree_ != nullptr);
}

std::vector<QueryExecutionTree*> Describe::getChildren() {
  return {subtree_.get()};
}

string Describe::getCacheKeyImpl() const {
  std::string resourceKey;
  for (const auto& resource : describe_.resources_) {
    if (std::holds_alternative<TripleComponent::Iri>(resource)) {
      resourceKey.append(
          std::get<TripleComponent::Iri>(resource).toStringRepresentation());
    } else {
      // TODO<joka921> Don't throw on unknown variables
      resourceKey.append(absl::StrCat(
          "column #", subtree_->getVariableColumn(std::get<Variable>(resource)),
          " "));
    }
  }
  return absl::StrCat("DESCRIBE ", subtree_->getCacheKey(), resourceKey);
}

string Describe::getDescriptor() const { return "DESCRIBE"; }

size_t Describe::getResultWidth() const { return 3; }

// As DESCRIBE is never part of the query planning, we can return dummy values
// for the following functions.

size_t Describe::getCostEstimate() { return 2 * subtree_->getCostEstimate(); }

uint64_t Describe::getSizeEstimateBeforeLimit() {
  return subtree_->getSizeEstimate() * 2;
}

float Describe::getMultiplicity([[maybe_unused]] size_t col) { return 1.0f; }

bool Describe::knownEmptyResult() { return subtree_->knownEmptyResult(); }

vector<ColumnIndex> Describe::resultSortedOn() const { return {}; }

VariableToColumnMap Describe::computeVariableToColumnMap() const {
  using V = Variable;
  auto col = makeAlwaysDefinedColumn;
  return {{V("?subject"), col(0)},
          {V("?predicate"), col(1)},
          {V("?object"), col(2)}};
}

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

// TODO<joka921> Recursively follow the blank nodes etc.
void Describe::recursivelyAddBlankNodes(
    IdTable& finalResult, ad_utility::HashSetWithMemoryLimit<Id>& alreadySeen,
    IdTable blankNodes) {
  AD_CORRECTNESS_CHECK(blankNodes.numColumns() == 1);
  if (blankNodes.empty()) {
    return;
  }
  using V = Variable;
  SparqlTripleSimple triple{V{"?subject"}, V{"?predicate"}, V{"?object"}};
  auto indexScan = ad_utility::makeExecutionTree<IndexScan>(
      getExecutionContext(), Permutation::SPO, triple);
  auto valuesOp = ad_utility::makeExecutionTree<ValuesForTesting>(
      getExecutionContext(), std::move(blankNodes),
      std::vector<std::optional<Variable>>{V{"?subject"}});

  // TODO<joka921> It might be, that the Column index is definitely not 0, we
  // have to store this separately.
  auto join = ad_utility::makeExecutionTree<Join>(
      getExecutionContext(), valuesOp, std::move(indexScan), 0, 0);

  auto result = join->getResult();
  // TODO<joka921> A lot of those things are inefficient, lets get it working
  // first.
  auto table = result->idTable().clone();
  finalResult.reserve(finalResult.size() + table.size());
  auto s = join->getVariableColumn(V{"?subject"});
  auto p = join->getVariableColumn(V{"?predicate"});
  auto o = join->getVariableColumn(V{"?object"});
  table.setColumnSubset(std::vector{s, p, o});
  finalResult.insertAtEnd(table);

  auto newBlankNodes =
      getNewBlankNodes(allocator(), alreadySeen, table.getColumn(2));
  // recurse
  recursivelyAddBlankNodes(finalResult, alreadySeen, std::move(newBlankNodes));
}

// _____________________________________________________________________________
ProtoResult Describe::computeResult([[maybe_unused]] bool requestLaziness) {
  std::vector<TripleComponent> valuesToDescribe;
  for (const auto& resource : describe_.resources_) {
    if (std::holds_alternative<TripleComponent::Iri>(resource)) {
      valuesToDescribe.push_back(std::get<TripleComponent::Iri>(resource));
    } else {
      throw std::runtime_error("DESCRIBE with a variable is not yet supported");
    }
  }
  parsedQuery::SparqlValues values;
  using V = Variable;
  values._variables.push_back(V{"?subject"});
  for (const auto& v : valuesToDescribe) {
    values._values.push_back(std::vector{v});
  }
  SparqlTripleSimple triple{V{"?subject"}, V{"?predicate"}, V{"?object"}};
  auto indexScan = ad_utility::makeExecutionTree<IndexScan>(
      getExecutionContext(), Permutation::SPO, triple);
  auto valuesOp =
      ad_utility::makeExecutionTree<Values>(getExecutionContext(), values);

  // TODO<joka921> It might be, that the Column index is definitely not 0, we
  // have to store this separately.
  auto join = ad_utility::makeExecutionTree<Join>(
      getExecutionContext(), valuesOp, std::move(indexScan), 0, 0);

  auto result = join->getResult();
  IdTable resultTable = result->idTable().clone();

  auto s = join->getVariableColumn(V{"?subject"});
  auto p = join->getVariableColumn(V{"?predicate"});
  auto o = join->getVariableColumn(V{"?object"});

  resultTable.setColumnSubset(std::vector{s, p, o});

  // Recursively follow blank nodes
  ad_utility::HashSetWithMemoryLimit<Id> alreadySeen{allocator()};
  auto blankNodes =
      getNewBlankNodes(allocator(), alreadySeen, resultTable.getColumn(2));
  recursivelyAddBlankNodes(resultTable, alreadySeen, std::move(blankNodes));
  return {std::move(resultTable), resultSortedOn(),
          result->getSharedLocalVocab()};
}
