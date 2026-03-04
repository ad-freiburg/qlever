// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_ENGINE_NAMEDRESULTCACHESERIALIZER_H
#define QLEVER_SRC_ENGINE_NAMEDRESULTCACHESERIALIZER_H

#include <boost/math/tools/roots.hpp>

#include "engine/NamedResultCache.h"
#include "util/AllocatorWithLimit.h"
#include "util/Serializer/SerializeString.h"
#include "util/Serializer/SerializeVector.h"
#include "util/Serializer/Serializer.h"
#include "util/Serializer/TripleSerializer.h"

// _____________________________________________________________________________
CPP_template_def(typename Serializer)(
    requires ad_utility::serialization::WriteSerializer<
        Serializer>) void NamedResultCache::writeToSerializer(Serializer&
                                                                  serializer)
    const {
  auto lock = cache_.wlock();
  std::vector<std::pair<Key, std::shared_ptr<const Value>>> entries;
  for (const auto& key : lock->getAllNonpinnedKeys()) {
    entries.emplace_back(key, (*lock)[key]);
    AD_CORRECTNESS_CHECK(entries.back().second != nullptr);
  }

  // Serialize the number of entries.
  serializer << entries.size();

  // Serialize each entry.
  for (const auto& [key, value] : entries) {
    serializer << key;
    serializer << *value;
  }
}

// _____________________________________________________________________________
CPP_template_def(typename Serializer)(
    requires ad_utility::serialization::ReadSerializer<
        Serializer>) void NamedResultCache::
    readFromSerializer(Serializer& serializer, Value::Allocator allocator,
                       ad_utility::BlankNodeManager& blankNodeManager) {
  // Clear the cache first.
  clear();

  // Deserialize the number of entries.
  size_t numEntries;
  serializer >> numEntries;

  // Deserialize each entry and add to the cache.
  for (size_t i = 0; i < numEntries; ++i) {
    // Deserialize the key
    Key key;
    serializer >> key;

    // Deserialize the value.
    Value value;
    value.allocatorForSerialization_ = allocator;
    value.blankNodeManagerForSerialization_ = blankNodeManager;
    serializer >> value;

    // Use the store method to maintain consistency.
    store(key, std::move(value));
  }
}

namespace ad_utility::serialization {

// Serialization for `NamedResultCache::Value`
// This serializes the complete Value including the `LocalVocab` with proper ID
// remapping.
AD_SERIALIZE_FUNCTION_WITH_CONSTRAINT(
    (ad_utility::SimilarTo<T, NamedResultCache::Value>)) {
  if constexpr (WriteSerializer<S>) {
    // Serialize the LocalVocab first (required for ID remapping).
    ad_utility::detail::serializeLocalVocab(serializer, arg.localVocab_);

    // Serialize the IdTable (uses the `serializeIds` helper which handles
    // LocalVocab IDs).
    serializer << arg.result_->numRows();
    serializer << arg.result_->numColumns();
    for (const auto& col : arg.result_->getColumns()) {
      // NOTE: Although the code for serialization of a local vocab above is
      // already incorporated, we currently still let local vocab entries throw
      // an exception, because there are some caveats in the serialization that
      // don't work yet, and will only be mitigated in the future. NOTE2: Even
      // though we disallow the local vocab, it is crucial to serialize the
      // local vocab because of possible added blank node indices, which we do
      // handle correctly, and which also rely on the local vocab.
      // TODO<joka921> Mitigate the inconsistencies in the serializer, and then
      // allow local vocab entries here.
      AD_CORRECTNESS_CHECK(
          ql::ranges::find(col, Datatype::LocalVocabIndex, &Id::getDatatype) ==
              col.end(),
          "Named result cache entries that contain local vocab entries "
          "currently cannot be serialized. Note that local vocab entries can "
          "also occur if SPARQL UPDATE operations have been performed on the "
          "index before creating the named cached result.");
      ad_utility::detail::serializeIds(serializer, col);
    }

    // Serialize VariableToColumnMap manually (`Variable` is not
    // default-constructible, so we cannot automatically read the hash map from
    // a serializer, and therefore for consistency we also manually handling the
    // writing to the serializer, s.t. we do not depend on the internals of
    // HashMap serialization.
    serializer << arg.varToColMap_.size();
    for (const auto& [var, colInfo] : arg.varToColMap_) {
      serializer << var;
      serializer << colInfo;
    }

    // Serialize resultSortedOn (vector of ColumnIndex).
    serializer << arg.resultSortedOn_;

    // Serialize cacheKey (string).
    serializer << arg.cacheKey_;

    // Serialize the `cachedGeoIndex_`. Note: The `cachedGeoIndex_` is not
    // default-constructible, so we use manual serialization (see the comment
    // above for the manual serialization of the `varToColMap_` for details).
    bool hasGeoIndex = arg.cachedGeoIndex_.has_value();
    serializer << hasGeoIndex;
    if (hasGeoIndex) {
      serializer << arg.cachedGeoIndex_.value();
    }
  } else {
    // Deserialize the LocalVocab and get the ID mapping.
    AD_CORRECTNESS_CHECK(arg.blankNodeManagerForSerialization_.has_value());
    auto [localVocab, mapping] = ad_utility::detail::deserializeLocalVocab(
        serializer, &arg.blankNodeManagerForSerialization_.value());

    // Deserialize the IdTable with ID mapping applied.
    size_t numRows, numColumns;
    serializer >> numRows;
    serializer >> numColumns;

    AD_CORRECTNESS_CHECK(arg.allocatorForSerialization_.has_value());
    IdTable idTable{numColumns, arg.allocatorForSerialization_.value()};
    idTable.resize(numRows);
    for (auto&& col : idTable.getColumns()) {
      ad_utility::detail::deserializeIds(serializer, mapping, col);
    }

    // Deserialize VariableToColumnMap manually.
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

    // Deserialize `resultSortedOn`.
    std::vector<ColumnIndex> resultSortedOn;
    serializer >> resultSortedOn;

    // Deserialize `cacheKey`.
    std::string cacheKey;
    serializer >> cacheKey;

    // Deserialize `cachedGeoIndex`.
    bool hasGeoIndex;
    serializer >> hasGeoIndex;
    std::optional<SpatialJoinCachedIndex> cachedGeoIndex;
    if (hasGeoIndex) {
      cachedGeoIndex.emplace(SpatialJoinCachedIndex::TagForSerialization{});
      serializer >> cachedGeoIndex.value();
    }

    // Construct the `Value`.
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
#endif  // QLEVER_SRC_ENGINE_NAMEDRESULTCACHESERIALIZER_H
