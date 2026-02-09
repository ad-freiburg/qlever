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

// Helper to set scan status to lazily completed (variadic, accepts 1+ scans)
template <typename... Scans>
inline void setScanStatusToLazilyCompleted(Scans&... scans) {
  (void(scans.runtimeInfo().status_ =
            RuntimeInformation::Status::lazilyMaterializedCompleted),
   ...);
}

}  // namespace qlever::joinWithIndexScanHelpers

#endif  // QLEVER_SRC_ENGINE_JOINWITHINDEXSCANHELPERS_H
