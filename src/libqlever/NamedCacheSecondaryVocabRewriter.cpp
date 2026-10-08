// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "libqlever/NamedCacheSecondaryVocabRewriter.h"

#include <numeric>
#include <optional>
#include <string>
#include <string_view>

#include "backports/algorithm.h"
#include "engine/ExplicitIdTableOperation.h"
#include "index/IdTableUtils.h"
#include "index/LocalVocabEntry.h"
#include "libqlever/CanonicalRowOrder.h"
#include "util/CompactStringVector.h"
#include "util/Exception.h"
#include "util/HashSet.h"

namespace qlever::namedCacheSecondaryVocab {

namespace {
// If the word of `entry` is contained in the vocabulary of the main index, or
// can be encoded directly in an `Id` (see `EncodedIriManager`), then return
// the `Id` of that word, else return `std::nullopt`.
//
// NOTE: The position of an `entry` whose word is contained in the secondary
// vocabulary of its own index is the `Id` in that vocabulary. For such an
// entry, `std::nullopt` is returned as well, because its word is looked up in
// the secondary vocabulary that is passed to the functions of this file.
std::optional<Id> idInMainVocab(const LocalVocabEntry& entry) {
  auto [lowerBound, upperBound] = entry.positionInVocab();
  if (lowerBound == upperBound) {
    return std::nullopt;
  }
  Id id = Id::fromBits(lowerBound.get());
  if (id.getDatatype() == Datatype::SecondaryVocabIndex) {
    return std::nullopt;
  }
  return id;
}

// Insert the words of all those `Id`s of type `LocalVocabIndex` of `column`
// into `newWords` that are neither contained in the vocabulary of the main
// index nor in the `secondaryVocab` (see `addNewWordsToSecondaryVocab`). The
// `handledEntries` are the `LocalVocabEntry`s that have already been handled
// (also by previous calls), each of which is handled only once.
void collectNewWords(
    ql::span<const Id> column, const SecondaryVocabulary& secondaryVocab,
    ad_utility::HashSet<const LocalVocabEntry*>& handledEntries,
    ad_utility::HashSet<std::string>& newWords) {
  for (Id id : column) {
    if (id.getDatatype() != Datatype::LocalVocabIndex) {
      continue;
    }
    const LocalVocabEntry* entry = id.getLocalVocabIndex();
    if (!handledEntries.insert(entry).second ||
        idInMainVocab(*entry).has_value()) {
      continue;
    }
    const auto& word = entry->toStringRepresentation();
    if (!secondaryVocab.getId(word).has_value()) {
      newWords.insert(word);
    }
  }
}
}  // namespace

// _____________________________________________________________________________
bool containsLocalVocabIds(const NamedResultCache::Value& value) {
  return IdTableUtils::containsLocalVocabIds(
      ExplicitIdTableOperation::viewOf(value.result_));
}

// _____________________________________________________________________________
size_t addNewWordsToSecondaryVocab(const Entries& entries,
                                   SecondaryVocabulary& secondaryVocab) {
  // The same `LocalVocabEntry` typically occurs in many rows, so each of them
  // is handled only once.
  ad_utility::HashSet<const LocalVocabEntry*> handledEntries;
  ad_utility::HashSet<std::string> newWords;
  for (const auto& [key, value] : entries) {
    auto view = ExplicitIdTableOperation::viewOf(value->result_);
    for (const auto& column : view.getColumns()) {
      collectNewWords(column, secondaryVocab, handledEntries, newWords);
    }
  }

  // The words of a segment have to be sorted (see
  // `SecondaryVocabulary::appendSegment`).
  std::vector<std::string_view> sortedNewWords{newWords.begin(),
                                               newWords.end()};
  ql::ranges::sort(sortedNewWords);
  CompactVectorOfStrings<char> segment;
  segment.build(sortedNewWords);
  secondaryVocab.appendSegment(std::move(segment));
  return sortedNewWords.size();
}

// _____________________________________________________________________________
Id rewriteId(Id id, const SecondaryVocabulary& secondaryVocab) {
  if (id.getDatatype() != Datatype::LocalVocabIndex) {
    return id;
  }
  const LocalVocabEntry& entry = *id.getLocalVocabIndex();
  if (auto idOfWord = idInMainVocab(entry); idOfWord.has_value()) {
    return idOfWord.value();
  }
  const auto& word = entry.toStringRepresentation();
  auto index = secondaryVocab.getId(word);
  AD_CONTRACT_CHECK(index.has_value(), "The word ", word,
                    " is contained neither in the vocabulary of the main "
                    "index nor in the secondary vocabulary, call "
                    "`addNewWordsToSecondaryVocab` first");
  return Id::makeFromSecondaryVocabIndex(index.value());
}

// _____________________________________________________________________________
std::vector<ColumnIndex> canonicalColumnOrder(
    const VariableToColumnMap& varToColMap, size_t numColumns) {
  std::vector<std::pair<std::string_view, ColumnIndex>> variables;
  for (const auto& [variable, info] : varToColMap) {
    AD_CONTRACT_CHECK(info.columnIndex_ < numColumns);
    variables.emplace_back(variable.name(), info.columnIndex_);
  }
  ql::ranges::sort(variables);
  std::vector<ColumnIndex> result;
  std::vector<bool> isUsed(numColumns, false);
  for (const auto& [name, column] : variables) {
    if (!isUsed[column]) {
      isUsed[column] = true;
      result.push_back(column);
    }
  }
  for (ColumnIndex column = 0; column < numColumns; ++column) {
    if (!isUsed[column]) {
      result.push_back(column);
    }
  }
  return result;
}

// _____________________________________________________________________________
CanonicalizedValue canonicalizeWithPermutation(
    const NamedResultCache::Value& value,
    const SecondaryVocabulary& secondaryVocab,
    const NamedResultCache::Value::Allocator& allocator,
    bool sortOnAllColumns) {
  // Copy the table (via the given `allocator`), and rewrite the copy in place.
  auto view = ExplicitIdTableOperation::viewOf(value.result_);
  IdTable table{view.numColumns(), allocator};
  table.insertAtEnd(view);
  if (containsLocalVocabIds(value)) {
    for (auto column : table.getColumns()) {
      ql::ranges::for_each(column, [&secondaryVocab](Id& id) {
        id = rewriteId(id, secondaryVocab);
      });
    }
  }

  // Bring the columns into canonical order. This is necessary because the
  // order of the columns of a result depends on the query plan, which may
  // change when the data changes. The columns of the geo index are identified
  // by their variable, so the index is not affected.
  auto varToColMap = value.varToColMap_;
  auto resultSortedOn = value.resultSortedOn_;
  auto oldColumnOfNewColumn =
      canonicalColumnOrder(value.varToColMap_, table.numColumns());
  if (!ql::ranges::is_sorted(oldColumnOfNewColumn)) {
    table.setColumnSubset(oldColumnOfNewColumn);
    auto newColumnOfOldColumn = invertPermutation(oldColumnOfNewColumn);
    for (auto& [variable, info] : varToColMap) {
      info.columnIndex_ = newColumnOfOldColumn[info.columnIndex_];
    }
    for (auto& column : resultSortedOn) {
      column = newColumnOfOldColumn[column];
    }
  }

  if (sortOnAllColumns) {
    resultSortedOn.resize(table.numColumns());
    std::iota(resultSortedOn.begin(), resultSortedOn.end(), ColumnIndex{0});
  }

  std::optional<SpatialJoinCachedIndex> geoIndex = value.cachedGeoIndex_;
  auto permutation =
      canonicalSortingPermutation(table.asStaticView<0>(), resultSortedOn);
  if (!ql::ranges::is_sorted(permutation)) {
    table = permuteRows(table.asStaticView<0>(), permutation, allocator);
    if (geoIndex.has_value()) {
      // `withPermutedRows` requires the new row of each old row, whereas
      // `permutation` contains the old row of each new row.
      geoIndex = geoIndex->withPermutedRows(invertPermutation(permutation));
    }
  }

  return CanonicalizedValue{
      NamedResultCache::Value{std::make_shared<const IdTable>(std::move(table)),
                              std::move(varToColMap), std::move(resultSortedOn),
                              value.localVocab_.clone(), value.cacheKey_,
                              std::move(geoIndex)},
      std::move(permutation)};
}

// _____________________________________________________________________________
NamedResultCache::Value canonicalizeForSerialization(
    const NamedResultCache::Value& value,
    const SecondaryVocabulary& secondaryVocab,
    const NamedResultCache::Value::Allocator& allocator,
    bool sortOnAllColumns) {
  return canonicalizeWithPermutation(value, secondaryVocab, allocator,
                                     sortOnAllColumns)
      .value_;
}

}  // namespace qlever::namedCacheSecondaryVocab
