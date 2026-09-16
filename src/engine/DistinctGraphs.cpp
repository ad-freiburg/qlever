// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Mete Tolga Gonultas <mg885@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/DistinctGraphs.h"

#include <absl/strings/str_cat.h>

#include <algorithm>
#include <optional>

#include "engine/Result.h"
#include "global/Constants.h"
#include "index/CompressedRelation.h"
#include "index/IndexImpl.h"
#include "index/LocatedTriples.h"
#include "index/Permutation.h"
#include "index/ScanSpecification.h"
#include "index/TripleComponentConversions.h"
#include "util/HashSet.h"

// ____________________________________________________________________________
DistinctGraphs::DistinctGraphs(QueryExecutionContext* qec,
                               Variable graphVariable, bool includeDefaultGraph)
    : Operation{qec},
      graphVariable_{std::move(graphVariable)},
      includeDefaultGraph_{includeDefaultGraph} {}

// ____________________________________________________________________________
std::unique_ptr<Operation> DistinctGraphs::cloneImpl() const {
  return std::make_unique<DistinctGraphs>(_executionContext, graphVariable_,
                                          includeDefaultGraph_);
}

// ____________________________________________________________________________
std::string DistinctGraphs::getCacheKeyImpl() const {
  return absl::StrCat("DistinctGraphs includeDefaultGraph=",
                      includeDefaultGraph_ ? "true" : "false");
}

// ____________________________________________________________________________
VariableToColumnMap DistinctGraphs::computeVariableToColumnMap() const {
  return {{graphVariable_, makeAlwaysDefinedColumn(0)}};
}

// ____________________________________________________________________________
size_t DistinctGraphs::getCostEstimate() {
  return getIndex()
      .getImpl()
      .getPermutation(Permutation::Enum::SPO)
      .metaData()
      .blockData()
      .size();
}

// Compute the distinct graph IDs of all blocks of the SPO permutation (see
// `CompressedRelationReader::computeUniqueGraphIds` for the details of the
// block metadata shortcut) and remove the default graph if requested.
// ____________________________________________________________________________
Result DistinctGraphs::computeResult([[maybe_unused]] bool requestLaziness) {
  const auto& permutation =
      getIndex().getImpl().getPermutation(Permutation::Enum::SPO);
  const LocatedTriplesPerBlock& ltpb =
      permutation.getLocatedTriplesForPermutation(locatedTriplesState());
  auto scanSpecAndBlocks = permutation.getScanSpecAndBlocks(
      ScanSpecification{std::nullopt, std::nullopt, std::nullopt},
      locatedTriplesState());
  ad_utility::HashSetWithMemoryLimit<Id::T> graphIds =
      permutation.reader().computeUniqueGraphIds(
          scanSpecAndBlocks, ltpb, cancellationHandle_, allocator());

  if (!includeDefaultGraph_) {
    auto defaultGraph = toValueId(
        TripleComponent{
            ad_utility::triple_component::Iri::fromIriref(DEFAULT_GRAPH_IRI)},
        getIndex().getImpl());
    graphIds.erase(defaultGraph->getBits());
  }

  IdTable idTable{1, getExecutionContext()->getAllocator()};
  idTable.resize(graphIds.size());
  ql::ranges::transform(graphIds, idTable.getColumn(0).begin(), Id::fromBits);
  numOfDistinctGraphs_ = graphIds.size();
  return {std::move(idTable), resultSortedOn(), LocalVocab{}};
}
