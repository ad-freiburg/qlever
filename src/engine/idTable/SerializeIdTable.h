#if false
//  Copyright 2025, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include "engine/LocalVocab.h"
#include "engine/idTable/IdTable.h"
#include "util/AllocatorWithLimit.h"
#include "util/Serializer/Serializer.h"
#include "util/Serializer/TripleSerializer.h"

namespace ad_utility::serialization {

// Serialize an IdTable without LocalVocab context (just raw IDs)
// This is used when the LocalVocab is handled separately
AD_SERIALIZE_FUNCTION_WITH_CONSTRAINT_WRITE(
    ad_utility::SimilarTo<T, IdTable>) {
  // Serialize dimensions
  serializer << arg.numRows();
  serializer << arg.numColumns();

  // Serialize each column as a vector of Ids
  for (size_t col = 0; col < arg.numColumns(); ++col) {
    ad_utility::detail::serializeIds(serializer, arg.getColumn(col));
  }
}

// Deserialize an IdTable without LocalVocab context
// The ID mapping must be applied after deserialization
AD_SERIALIZE_FUNCTION_WITH_CONSTRAINT_READ(
    ad_utility::SimilarTo<T, IdTable>) {
  // Deserialize dimensions
  size_t numRows, numColumns;
  serializer >> numRows;
  serializer >> numColumns;

  // Create the IdTable
  arg = IdTable{numColumns, ad_utility::makeUnlimitedAllocator<Id>()};

  // Deserialize each column
  for (size_t col = 0; col < numColumns; ++col) {
    // We need a dummy blank node function for the interface, but we don't
    // expect blank nodes in this context
    auto ids = ad_utility::detail::deserializeIds(
        serializer, absl::flat_hash_map<Id::T, Id>{},
        []() -> BlankNodeIndex {
          throw std::runtime_error(
              "Unexpected blank node in IdTable deserialization");
        });

    // Add this column to the table
    if (col == 0) {
      arg.resize(numRows);
    }
    for (size_t row = 0; row < numRows; ++row) {
      arg(row, col) = ids[row];
    }
  }
}

}  // namespace ad_utility::serialization
#endif
