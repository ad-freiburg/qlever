// Copyright 2026, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)

#ifndef QLEVER_SRC_ENGINE_JOINWITHINDEXSCANHELPERS_H
#define QLEVER_SRC_ENGINE_JOINWITHINDEXSCANHELPERS_H

#include "engine/AddCombinedRowToTable.h"
#include "engine/IndexScan.h"
#include "engine/Result.h"
#include "index/CompressedRelation.h"
#include "util/Iterators.h"
#include "util/JoinAlgorithms/JoinAlgorithms.h"
#include "util/JoinAlgorithms/JoinColumnMapping.h"

namespace qlever::joinWithIndexScanHelpers {

// Tag types to indicate the join semantics
struct InnerJoinTag {};
struct OptionalJoinTag {};
struct MinusTag {};

// Helper to convert generators to the format expected by join algorithms
using IteratorWithSingleCol = ad_utility::InputRangeTypeErased<
    ad_utility::IdTableAndFirstCols<1, IdTable>>;

inline IteratorWithSingleCol convertGenerator(
    CompressedRelationReader::IdTableGeneratorInputRange&& gen,
    IndexScan& scan) {
  // Store the generator in a wrapper so we can access its details after moving
  auto generatorStorage =
      std::make_shared<CompressedRelationReader::IdTableGeneratorInputRange>(
          std::move(gen));

  using SendPriority = RuntimeInformation::SendPriority;

  auto range = ad_utility::CachingTransformInputRange(
      *generatorStorage,
      [generatorStorage, &scan,
       sendPriority = SendPriority::Always](auto& table) mutable {
        scan.updateRuntimeInfoForLazyScan(generatorStorage->details(),
                                          sendPriority);
        sendPriority = SendPriority::IfDue;
        // IndexScans don't have a local vocabulary, so we can just use an empty
        // one.
        return ad_utility::IdTableAndFirstCols<1, IdTable>{std::move(table),
                                                           LocalVocab{}};
      });

  return IteratorWithSingleCol{std::move(range)};
}

// Helper to set scan status to lazily completed (variadic, accepts 1+ scans)
template <typename... Scans>
inline void setScanStatusToLazilyCompleted(Scans&... scans) {
  (void(scans.runtimeInfo().status_ =
            RuntimeInformation::Status::lazilyMaterializedCompleted),
   ...);
}

}  // namespace qlever::joinWithIndexScanHelpers

#endif  // QLEVER_SRC_ENGINE_JOINWITHINDEXSCANHELPERS_H
