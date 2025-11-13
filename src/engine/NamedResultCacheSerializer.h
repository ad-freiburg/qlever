//   Copyright 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include "engine/NamedResultCache.h"
#include "engine/SerializeColumnIndexAndTypeInfo.h"
#include "rdfTypes/SerializeVariable.h"
#include "util/AllocatorWithLimit.h"
#include "util/Serializer/SerializeHashMap.h"
#include "util/Serializer/SerializeOptional.h"
#include "util/Serializer/SerializeString.h"
#include "util/Serializer/SerializeVector.h"
#include "util/Serializer/Serializer.h"
#include "util/Serializer/TripleSerializer.h"

namespace ad_utility::serialization {

// Serialization for NamedResultCache::Value
// This serializes the complete Value including LocalVocab with proper ID
// remapping
AD_SERIALIZE_FUNCTION_WITH_CONSTRAINT(
    (ad_utility::SimilarTo<T, NamedResultCache::Value>)) {
  if constexpr (WriteSerializer<S>) {
    // Serialize the LocalVocab first (required for ID remapping)
    ad_utility::detail::serializeLocalVocab(serializer, arg.localVocab_);

    // Serialize the IdTable (uses the serializeIds helper which handles
    // LocalVocab IDs)
    serializer << arg.result_->numRows();
    serializer << arg.result_->numColumns();
    for (size_t col = 0; col < arg.result_->numColumns(); ++col) {
      ad_utility::detail::serializeIds(serializer, arg.result_->getColumn(col));
    }

    // Serialize VariableToColumnMap manually (can't use HashMap serialization
    // because Variable is not default-constructible)
    serializer << arg.varToColMap_.size();
    for (const auto& [var, colInfo] : arg.varToColMap_) {
      serializer << var;
      serializer << colInfo;
    }

    // Serialize resultSortedOn (vector of ColumnIndex)
    serializer << arg.resultSortedOn_;

    // Serialize cacheKey (string)
    serializer << arg.cacheKey_;

    // Serialize cachedGeoIndex (optional)
    bool hasGeoIndex = arg.cachedGeoIndex_.has_value();
    serializer << hasGeoIndex;
    if (hasGeoIndex) {
      serializer << arg.cachedGeoIndex_.value();
    }
  } else {
    // Deserialize the LocalVocab and get the ID mapping
    auto [localVocab, mapping] =
        ad_utility::detail::deserializeLocalVocab(serializer);

    // Deserialize the IdTable with ID mapping applied
    size_t numRows, numColumns;
    serializer >> numRows;
    serializer >> numColumns;

    IdTable idTable{numColumns, ad_utility::makeUnlimitedAllocator<Id>()};
    for (size_t col = 0; col < numColumns; ++col) {
      auto ids = ad_utility::detail::deserializeIds(
          serializer, mapping, []() -> BlankNodeIndex {
            throw std::runtime_error(
                "Unexpected blank node in NamedResultCache deserialization");
          });
      if (col == 0) {
        idTable.resize(numRows);
      }
      for (size_t row = 0; row < numRows; ++row) {
        idTable(row, col) = ids[row];
      }
    }

    // Deserialize VariableToColumnMap manually
    size_t mapSize;
    serializer >> mapSize;
    VariableToColumnMap varToColMap;
    for (size_t i = 0; i < mapSize; ++i) {
      Variable var{"?dummy"};  // Variable needs a non-empty name
      serializer >> var;
      ColumnIndexAndTypeInfo colInfo{0, ColumnIndexAndTypeInfo::AlwaysDefined};
      serializer >> colInfo;
      varToColMap[std::move(var)] = colInfo;
    }

    // Deserialize resultSortedOn
    std::vector<ColumnIndex> resultSortedOn;
    serializer >> resultSortedOn;

    // Deserialize cacheKey
    std::string cacheKey;
    serializer >> cacheKey;

    // Deserialize cachedGeoIndex
    bool hasGeoIndex;
    serializer >> hasGeoIndex;
    std::optional<SpatialJoinCachedIndex> cachedGeoIndex;
    if (hasGeoIndex) {
      cachedGeoIndex.emplace(SpatialJoinCachedIndex::TagForSerialization{});
      serializer >> cachedGeoIndex.value();
    }

    // Construct the Value
    arg = NamedResultCache::Value{
        std::make_shared<const IdTable>(std::move(idTable)),
        std::move(varToColMap),
        std::move(resultSortedOn),
        std::move(localVocab),
        std::move(cacheKey),
        std::move(cachedGeoIndex)};
  }
}

}  // namespace ad_utility::serialization
