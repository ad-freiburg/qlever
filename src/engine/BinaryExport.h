// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Robin Textor-Falconi <textorr@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_BINARY_EXPORT_H
#define QLEVER_SRC_ENGINE_BINARY_EXPORT_H

#include "global/Id.h"
#include "util/CancellationHandle.h"
#include "util/http/HttpClient.h"
#include "util/stream_generator.h"

namespace qlever::binary_export {
// Helper struct to temporarily store strings and aggregate them to avoid
// sending the same strings over and over again.
struct StringMapping {
  // Store the actual mapping from an id to the corresponding insertion order.
  // (The first newly inserted string will get id 0, the second id 1, and so
  // on.)
  // TODO<RobinTF, joka921> consider mapping from `Id::T` instead of `Id` to get
  // a cheaper lookup.
  ad_utility::HashMap<Id, uint64_t> stringMapping_;
  uint64_t numProcessedRows_ = 0;

  // Serialize the internal datastructure to a string and clear it.
  std::string flush(const Index& index);

  // Increment the row counter by one if the mapping contains elements.
  void nextRow() {
    if (!stringMapping_.empty()) {
      numProcessedRows_++;
    }
  }

  // Remap an `Id` to another `Id` which internally uses the `LocalVocab`
  // datatype, but instead of a pointer it uses the index provided by
  // `stringMapping_`, creating a new index if not already present.
  Id remapId(Id id);

  // Return true if the datastructure has grown large enough, or wasn't
  // cleared recently enough to avoid stalling the consumer.
  bool needsFlush() const {
    return numProcessedRows_ >= 100'000 || stringMapping_.size() >= 10'000;
  }
};

// Convert `originalId`, which might contain references into this process'
// memory space to an id that completely inlines a value, or one that only has
// a reference to the passed `stringMapping`.
Id toExportableId(Id originalId, const LocalVocab& localVocab,
                  StringMapping& stringMapping);

ad_utility::streams::stream_generator exportAsQLeverBinary(
    const QueryExecutionTree& qet,
    const parsedQuery::SelectClause& selectClause,
    LimitOffsetClause limitAndOffset,
    ad_utility::SharedCancellationHandle cancellationHandle);
// _____________________________________________________________________________
Result importBinaryHttpResponse(bool requestLaziness,
                                HttpOrHttpsResponse response,
                                const QueryExecutionContext& qec,
                                std::vector<ColumnIndex> resultSortedOn);
}  // namespace qlever::binary_export

#endif  // QLEVER_SRC_ENGINE_BINARY_EXPORT_H
