// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "libqlever/NamedCacheSecondaryVocabRewriter.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include "backports/StartsWithAndEndsWith.h"
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
    ConstIdColumnRef column, const SecondaryVocabulary& secondaryVocab,
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
    // The columns without a variable are not written (see
    // `canonicalColumnOrder`), so their words are not needed.
    for (ColumnIndex column :
         canonicalColumnOrder(value->varToColMap_, view.numColumns())) {
      collectNewWords(view.getColumn(column), secondaryVocab, handledEntries,
                      newWords);
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
  ad_utility::HashSet<ColumnIndex> usedColumns;
  for (const auto& [name, column] : variables) {
    if (usedColumns.insert(column).second) {
      result.push_back(column);
    }
  }
  return result;
}

// _____________________________________________________________________________
std::string canonicalCacheKey(
    std::string_view cacheKey,
    ql::span<const ColumnIndex> oldColumnOfNewColumn) {
  static constexpr std::string_view prefix = "CANONICALIZED FOR SERIALIZATION ";
  if (ql::starts_with(cacheKey, prefix)) {
    return std::string{cacheKey};
  }
  return absl::StrCat(prefix, "(", cacheKey, ") COLUMNS [",
                      absl::StrJoin(oldColumnOfNewColumn, ", "), "]");
}

// _____________________________________________________________________________
CanonicalizedValue canonicalizeWithPermutation(
    const NamedResultCache::Value& value,
    const SecondaryVocabulary& secondaryVocab,
    const NamedResultCache::Value::Allocator& allocator) {
  // Bring the columns of the table into canonical order. The canonical column
  // order is necessary because the order of the columns of a result depends on
  // the query plan, which may change when the data changes. The columns
  // without a variable are dropped, because no query can refer to them. The
  // columns of the geo index are identified by their variable, so the index is
  // not affected.
  auto view = ExplicitIdTableOperation::viewOf(value.result_);
  auto oldColumnOfNewColumn =
      canonicalColumnOrder(value.varToColMap_, view.numColumns());
  auto columnSubset = view.asColumnSubsetView(oldColumnOfNewColumn);

  // Adapt the `varToColMap` and the `resultSortedOn` to the new columns.
  constexpr auto droppedColumn = std::numeric_limits<ColumnIndex>::max();
  std::vector<ColumnIndex> newColumnOfOldColumn(view.numColumns(),
                                                droppedColumn);
  for (auto [newColumn, oldColumn] :
       ::ranges::views::enumerate(oldColumnOfNewColumn)) {
    newColumnOfOldColumn[oldColumn] = newColumn;
  }
  auto varToColMap = value.varToColMap_;
  for (auto& [variable, info] : varToColMap) {
    info.columnIndex_ = newColumnOfOldColumn[info.columnIndex_];
  }
  // The rows are still sorted by the prefix of `resultSortedOn` that consists
  // only of columns that were not dropped.
  std::vector<ColumnIndex> resultSortedOn;
  for (ColumnIndex column : value.resultSortedOn_) {
    if (newColumnOfOldColumn[column] == droppedColumn) {
      break;
    }
    resultSortedOn.push_back(newColumnOfOldColumn[column]);
  }

  auto makeValue = [&](ExplicitIdTableOperation::IdTableOrView result,
                       std::optional<SpatialJoinCachedIndex> geoIndex) {
    return NamedResultCache::Value{
        std::move(result),
        std::move(varToColMap),
        std::move(resultSortedOn),
        value.localVocab_.clone(),
        canonicalCacheKey(value.cacheKey_, oldColumnOfNewColumn),
        std::move(geoIndex)};
  };

  // If no `Id` has to be rewritten and the rows already are in canonical
  // order (the common case, for example for an index scan), then the copy
  // only consists of a view of the columns of `value`, which avoids copying
  // the (possibly very large) table.
  if (!containsLocalVocabIds(value) &&
      canonicalRowOrder::isInCanonicalOrder(columnSubset, resultSortedOn)) {
    return CanonicalizedValue{
        makeValue(std::move(columnSubset), value.cachedGeoIndex_),
        std::nullopt};
  }

  // Otherwise, copy the columns (via the given `allocator`), and rewrite the
  // copy in place.
  IdTable table{oldColumnOfNewColumn.size(), allocator};
  table.insertAtEnd(columnSubset);
  if (containsLocalVocabIds(value)) {
    for (auto column : table.getColumns()) {
      ql::ranges::for_each(column, [&secondaryVocab](Id& id) {
        id = rewriteId(id, secondaryVocab);
      });
    }
  }

  // The rows are sorted by `resultSortedOn` first, so the kept prefix of the
  // sort order that the query plan produced stays valid.
  // TODO<joka921> Distinguish between an explicit sort order of the query
  // (via `INTERNAL SORT BY`, which has to be kept) and a sort order that the
  // query plan produced by accident (which may change when the plan changes,
  // and could be replaced by a plan-independent order to keep the diffs of
  // two blobs small). This information is currently not stored in a
  // `NamedResultCache::Value`.
  std::optional<SpatialJoinCachedIndex> geoIndex = value.cachedGeoIndex_;
  std::optional<std::vector<size_t>> oldRowOfNewRow;
  auto permutation = canonicalRowOrder::canonicalSortingPermutation(
      table.asStaticView<0>(), resultSortedOn);
  if (!ql::ranges::is_sorted(permutation)) {
    table = canonicalRowOrder::permuteRows(table.asStaticView<0>(), permutation,
                                           allocator);
    if (geoIndex.has_value()) {
      // `withPermutedRows` requires the new row of each old row, whereas
      // `permutation` contains the old row of each new row.
      geoIndex = geoIndex->withPermutedRows(
          canonicalRowOrder::invertPermutation(permutation));
    }
    oldRowOfNewRow = std::move(permutation);
  }

  return CanonicalizedValue{
      makeValue(std::make_shared<const IdTable>(std::move(table)),
                std::move(geoIndex)),
      std::move(oldRowOfNewRow)};
}

// _____________________________________________________________________________
NamedResultCache::Value canonicalizeForSerialization(
    const NamedResultCache::Value& value,
    const SecondaryVocabulary& secondaryVocab,
    const NamedResultCache::Value::Allocator& allocator) {
  return canonicalizeWithPermutation(value, secondaryVocab, allocator).value_;
}

}  // namespace qlever::namedCacheSecondaryVocab
