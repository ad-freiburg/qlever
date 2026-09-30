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

#include "backports/algorithm.h"
#include "engine/ExplicitIdTableOperation.h"
#include "index/LocalVocabEntry.h"
#include "util/CompactStringVector.h"
#include "util/Exception.h"
#include "util/HashSet.h"

namespace qlever::namedCacheSecondaryVocab {

namespace {
// If the word of `entry` is contained in the vocabulary of the main index, or
// can be encoded directly in an `Id` (see `EncodedIriManager`), then return
// the `Id` of that word, else return `std::nullopt`. Note that the position of
// an `entry` that is contained in the secondary vocabulary of its own index is
// the `Id` in that vocabulary, for which `std::nullopt` is returned as well,
// because such a word is looked up in the secondary vocabulary that is passed
// to the functions of this file.
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

// Return true iff the row `a` of `table` is less than the row `b` when
// comparing lexicographically by the `sortedOn` columns.
bool rowLess(const IdTable& table, const std::vector<ColumnIndex>& sortedOn,
             size_t a, size_t b) {
  for (ColumnIndex column : sortedOn) {
    Id idA = table(a, column);
    Id idB = table(b, column);
    if (idA != idB) {
      return idA < idB;
    }
  }
  return false;
}

// Return the permutation of the rows of `table` that sorts it by the
// `sortedOn` columns (the row at position `i` of the sorted table is the row
// `result[i]` of `table`), or `std::nullopt` if `table` already is sorted.
std::optional<std::vector<size_t>> sortingPermutation(
    const IdTable& table, const std::vector<ColumnIndex>& sortedOn) {
  auto less = [&table, &sortedOn](size_t a, size_t b) {
    return rowLess(table, sortedOn, a, b);
  };
  std::vector<size_t> permutation(table.numRows());
  std::iota(permutation.begin(), permutation.end(), size_t{0});
  if (ql::ranges::is_sorted(permutation, less)) {
    return std::nullopt;
  }
  ql::ranges::stable_sort(permutation, less);
  return permutation;
}

// Return the rows of `table`, permuted via `permutation` (see
// `sortingPermutation`).
IdTable permuteRows(const IdTable& table,
                    const std::vector<size_t>& permutation,
                    NamedResultCache::Value::Allocator allocator) {
  IdTable result{table.numColumns(), std::move(allocator)};
  result.resize(table.numRows());
  for (size_t column = 0; column < table.numColumns(); ++column) {
    auto source = table.getColumn(column);
    auto target = result.getColumn(column);
    for (size_t row = 0; row < permutation.size(); ++row) {
      target[row] = source[permutation[row]];
    }
  }
  return result;
}
}  // namespace

// _____________________________________________________________________________
bool containsLocalVocabIds(const NamedResultCache::Value& value) {
  auto view = ExplicitIdTableOperation::viewOf(value.result_);
  return ql::ranges::any_of(view.getColumns(), [](const auto& column) {
    return ql::ranges::find(column, Datatype::LocalVocabIndex,
                            &Id::getDatatype) != ql::ranges::end(column);
  });
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
  }

  // The words of a segment have to be sorted (see
  // `SecondaryVocabulary::appendSegment`).
  std::vector<std::string> sortedNewWords{newWords.begin(), newWords.end()};
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
NamedResultCache::Value rewriteToSecondaryVocab(
    const NamedResultCache::Value& value,
    const SecondaryVocabulary& secondaryVocab,
    NamedResultCache::Value::Allocator allocator) {
  auto view = ExplicitIdTableOperation::viewOf(value.result_);
  IdTable table{view.numColumns(), allocator};
  table.resize(view.numRows());
  for (size_t column = 0; column < view.numColumns(); ++column) {
    ql::ranges::transform(
        view.getColumn(column), table.getColumn(column).begin(),
        [&secondaryVocab](Id id) { return rewriteId(id, secondaryVocab); });
  }

  std::optional<SpatialJoinCachedIndex> geoIndex = value.cachedGeoIndex_;
  if (auto permutation = sortingPermutation(table, value.resultSortedOn_);
      permutation.has_value()) {
    table = permuteRows(table, permutation.value(), allocator);
    if (geoIndex.has_value()) {
      std::vector<size_t> newRowOfOldRow(permutation->size());
      for (size_t newRow = 0; newRow < permutation->size(); ++newRow) {
        newRowOfOldRow[(*permutation)[newRow]] = newRow;
      }
      geoIndex = geoIndex->withPermutedRows(newRowOfOldRow);
    }
  }

  return NamedResultCache::Value{
      std::make_shared<const IdTable>(std::move(table)),
      value.varToColMap_,
      value.resultSortedOn_,
      value.localVocab_.clone(),
      value.cacheKey_,
      std::move(geoIndex)};
}

}  // namespace qlever::namedCacheSecondaryVocab
