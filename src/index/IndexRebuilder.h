// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_SRC_INDEX_INDEXREBUILDER_H
#define QLEVER_SRC_INDEX_INDEXREBUILDER_H

#include <string>
#include <vector>

#include "index/IndexImpl.h"
#include "util/CancellationHandle.h"

namespace qlever {

// Build a new index based on this data.
void materializeToIndex(
    const IndexImpl& index, const std::string& newIndexName,
    const std::vector<LocalVocabIndex>& entries,
    const SharedLocatedTriplesSnapshot& snapshot,
    const ad_utility::SharedCancellationHandle& cancellationHandle,
    const std::string& logFileName);

}  // namespace qlever

#endif  // QLEVER_SRC_INDEX_INDEXREBUILDER_H
