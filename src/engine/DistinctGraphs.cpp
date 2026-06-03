// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Mete tolga gonultas mg885@email.uni-freiburg.de, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/DistinctGraphs.h"

#include <algorithm>
#include <optional>
#include "engine/Result.h"
#include "global/Constants.h"
#include "global/RuntimeParameters.h"
#include "index/CompressedRelation.h"
#include "index/IndexImpl.h"
#include "index/Permutation.h"
#include "index/ScanSpecification.h"
#include "util/HashSet.h"

// Constructor needs qec and graph variable which isn't should be bound on the inner queries 
DistinctGraphs::DistinctGraphs(QueryExecutionContext* qec,
                               Variable graphVariable)
    : Operation{qec}, graphVariable_{std::move(graphVariable)} {}

std::unique_ptr<Operation> DistinctGraphs::cloneImpl() const {
  return std::make_unique<DistinctGraphs>(_executionContext, graphVariable_);
}

VariableToColumnMap DistinctGraphs::computeVariableToColumnMap() const {
  return {{graphVariable_, makeAlwaysDefinedColumn(0)}};
}

/**
 * @brief Implements support for SPARQL queries of the form GRAPH ?g { ... } where ?g is an unbound variable.
 * * DistinctGraphs operation efficiently reads all distinct named graph IDs directly from 
 * the block metadata of the index, without a full table scan where possible.
 * * If distinct graph count <= MAX_NUM_GRAPHS_STORED_IN_BLOCK_METADATA:
 * - Traverse through the graphInfo_;
 * - If a new graph Id is seen, decompress and read it to prove its existence.
 * Else:
 * - Decompress, read, and add all graph Ids to the result.
 *
 * Filters out the default graph IRI from the result when the treatDefaultGraphAsNamedGraph 
 * runtime parameter is off.
 *
 * @param requestLaziness If true, hints to the query engine that results can be evaluated lazily.
 * Currently marked as [[maybe_unused]].
 *
 * @return A Result object containing a unique set of all matching named graph Ids.
 */
Result DistinctGraphs::computeResult([[maybe_unused]] bool requestLaziness) {
  const auto& permutation =
      getIndex().getImpl().getPermutation(Permutation::Enum::SPO);
  ad_utility::HashSet<Id::T> graphIds;

  using ScanSpecAndBlocks = CompressedRelationReader::ScanSpecAndBlocks;
  ScanSpecAndBlocks scanSpecAndBlocks = permutation.getScanSpecAndBlocks(ScanSpecification{std::nullopt, std::nullopt, std::nullopt}, locatedTriplesState());
  
  for (const auto& metadata : scanSpecAndBlocks.getBlockMetadataView()){
    bool hasNewGraphId = ql::ranges::any_of(metadata.graphInfo_.value(), 
      [&graphIds](Id id){return !graphIds.contains(id.getBits()); }
    );
    bool shouldScan = !metadata.graphInfo_ || hasNewGraphId;
    if (shouldScan) {
      auto lazyScanResult = permutation.lazyScan(
          scanSpecAndBlocks, std::vector<CompressedBlockMetadata>{metadata},
          std::vector<ColumnIndex>{ADDITIONAL_COLUMN_GRAPH_ID},
          cancellationHandle_, locatedTriplesState());
      for (auto& idTable : lazyScanResult) {
        for (Id id : idTable.getColumn(3)) {
          graphIds.insert(id.getBits());
        }
      }
    }   
  }

  auto treatDefaultGraphAsNamedGraph =
      getRuntimeParameter<&RuntimeParameters::treatDefaultGraphAsNamedGraph_>();
  if (!treatDefaultGraphAsNamedGraph) {
    auto defaultGraph =
        TripleComponent{
            ad_utility::triple_component::Iri::fromIriref(DEFAULT_GRAPH_IRI)}
            .toValueId(getIndex());
    graphIds.erase(defaultGraph->getBits());
  }

  IdTable idTable{1, getExecutionContext()->getAllocator()};
  idTable.resize(graphIds.size());
  ql::ranges::transform(graphIds, idTable.getColumn(0).begin(),
                        Id::fromBits);

  return {std::move(idTable), resultSortedOn(), LocalVocab{}};
}
