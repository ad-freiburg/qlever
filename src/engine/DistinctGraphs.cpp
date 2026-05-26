#include "DistinctGraphs.h"

#include <optional>

#include "engine/IndexScan.h"
#include "engine/Result.h"
#include "global/Constants.h"
#include "global/RuntimeParameters.h"
#include "index/CompressedRelation.h"
#include "index/IndexImpl.h"
#include "index/Permutation.h"
#include "index/ScanSpecification.h"
#include "util/HashSet.h"
#include "util/Log.h"

DistinctGraphs::DistinctGraphs(QueryExecutionContext* qec,
                               Variable graphVariable)
    : Operation{qec}, graphVariable_{std::move(graphVariable)} {}

std::vector<QueryExecutionTree*> DistinctGraphs::getChildren() { return {}; }

std::string DistinctGraphs::getDescriptor() const { return "Distinct Graphs"; }

size_t DistinctGraphs::getResultWidth() const { return 1; }

size_t DistinctGraphs::getCostEstimate() { return 0; }

float DistinctGraphs::getMultiplicity(size_t) { return 1.0f; }

bool DistinctGraphs::knownEmptyResult() { return false; }

std::unique_ptr<Operation> DistinctGraphs::cloneImpl() const {
  return std::make_unique<DistinctGraphs>(_executionContext, graphVariable_);
}

std::vector<ColumnIndex> DistinctGraphs::resultSortedOn() const { return {}; }

std::string DistinctGraphs::getCacheKeyImpl() const {
  return "DistinctGraphs " + graphVariable_.name();
}

uint64_t DistinctGraphs::getSizeEstimateBeforeLimit() { return 1; }

Result DistinctGraphs::computeResult([[maybe_unused]] bool requestLaziness) {
  const auto& permutation =
      getIndex().getImpl().getPermutation(Permutation::Enum::SPO);
  auto blockRanges =
      permutation.getAugmentedMetadataForPermutation(locatedTriplesState());
  ad_utility::HashSet<Id> graphIds;

  using ScanSpecAndBlocks = CompressedRelationReader::ScanSpecAndBlocks;
  ScanSpecAndBlocks scanSpec{
      ScanSpecification{std::nullopt, std::nullopt, std::nullopt}, blockRanges};

  for (const auto& blockRange : blockRanges) {  // vector<range of Blocks>
    // metadatas of "every block in range"
    for (const auto& metadata : blockRange) {
      if (metadata.graphInfo_) {
        bool hasNewGraphId = false;
        for (const auto& id : *metadata.graphInfo_) {
          if (!graphIds.contains(id)) {
            hasNewGraphId = true;  // if there is at least one non-existed id in
                                   // the graphIds
            break;
          }
        }
        if (hasNewGraphId) {  // there is at least one non-proved element in the
                              // graph infos
          auto lazyScanResult = permutation.lazyScan(
              scanSpec, std::vector<CompressedBlockMetadata>{metadata},
              std::vector<ColumnIndex>{ADDITIONAL_COLUMN_GRAPH_ID},
              cancellationHandle_, locatedTriplesState());
          for (auto& idTable : lazyScanResult) {
            for (Id id : idTable.getColumn(3)) {
              graphIds.insert(id);
            }
          }
        }
      } else {  // if there is more than MAX_GRAPH graph
        auto lazyScanResult = permutation.lazyScan(
            scanSpec, std::vector<CompressedBlockMetadata>{metadata},
            std::vector<ColumnIndex>{ADDITIONAL_COLUMN_GRAPH_ID},
            cancellationHandle_, locatedTriplesState());
        for (auto& idTable : lazyScanResult) {
          for (Id id : idTable.getColumn(3)) {
            graphIds.insert(id);
          }
        }
      }
    }
  }

  auto treatDefaultGraphAsNamedGraph_ =
      getRuntimeParameter<&RuntimeParameters::treatDefaultGraphAsNamedGraph_>();
  if (!treatDefaultGraphAsNamedGraph_) {
    auto defaultGraph =
        TripleComponent{
            ad_utility::triple_component::Iri::fromIriref(DEFAULT_GRAPH_IRI)}
            .toValueId(getIndex());
    graphIds.erase(*defaultGraph);
  }

  IdTable idTable{1, getExecutionContext()->getAllocator()};
  idTable.resize(graphIds.size());
  size_t rowIndex = 0;
  for (Id id : graphIds) {
    idTable[rowIndex][0] = id;
    rowIndex++;
  }
  return {std::move(idTable), resultSortedOn(), LocalVocab{}};
}

VariableToColumnMap DistinctGraphs::computeVariableToColumnMap() const {
  return {{graphVariable_, makeAlwaysDefinedColumn(0)}};
}
