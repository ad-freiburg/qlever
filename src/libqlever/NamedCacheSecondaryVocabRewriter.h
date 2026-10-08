// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_LIBQLEVER_NAMEDCACHESECONDARYVOCABREWRITER_H
#define QLEVER_SRC_LIBQLEVER_NAMEDCACHESECONDARYVOCABREWRITER_H

#include <string>
#include <string_view>
#include <vector>

#include "backports/span.h"
#include "engine/NamedResultCache.h"
#include "global/Id.h"
#include "index/vocabulary/SecondaryVocabulary.h"

// Rewrite the entries of a `NamedResultCache` such that they no longer contain
// any `Id` of type `LocalVocabIndex`, which is required to persist them (in
// particular in the blobs of `NamedCachedQueryBlobManager`): an `Id` of that
// type is a pointer into the memory of the current process. A word of such an
// `Id` that is contained in the vocabulary of the main index (or that can be
// encoded directly in an `Id`, see `EncodedIriManager`) is rewritten to the
// `Id` of that word, every other word is added to a secondary vocabulary
// (see `index/vocabulary/SecondaryVocabulary.h`), and its `Id` is rewritten to
// the corresponding `Id` of type `Datatype::SecondaryVocabIndex`.
//
// The rewriting consists of two passes: first,
// `addNewWordsToSecondaryVocab` collects the words of all entries and adds the
// new ones to the secondary vocabulary as a single segment, which has to be
// sorted and is therefore only known once all entries have been seen. Then,
// each entry can be canonicalized via `canonicalizeForSerialization` (or
// `canonicalizeWithPermutation`), one at a time, so that the canonicalized
// copies of the entries never have to be kept in memory all at once. The
// canonicalization of an entry rewrites its `Id`s (only if it contains any
// `Id` of type `LocalVocabIndex`), drops its columns without a variable, and
// brings its other columns and its rows into canonical order. This is done for
// every entry that is written to a blob, no matter whether it contains local
// vocab `Id`s, because the order of the rows and columns has to be canonical
// for the diff of two blobs to be small.
//
// The secondary vocabulary is passed in by the caller, and may already contain
// words, so that the same functions can also extend a preexisting secondary
// vocabulary (for example the one of a previously written blob) by the words
// of additional entries, without changing the `Id`s of its words.
//
// PRECONDITION for all functions below: If the index of the entries has a
// secondary vocabulary itself (for example because it was loaded from a blob),
// then the secondary vocabulary that is passed in has to be an extension of
// that one. That is, it has to contain all of its words at the same global
// indices, for example because it was obtained via
// `SecondaryVocabulary::clone`. This is because the `Id`s of type
// `Datatype::SecondaryVocabIndex` of the entries are kept as they are.
namespace qlever::namedCacheSecondaryVocab {

// The entries of a `NamedResultCache`, as returned by
// `NamedResultCache::getAllEntriesSortedByKey`.
using Entries = NamedResultCache::Entries;

// Return true iff the result of `value` contains at least one `Id` of type
// `LocalVocabIndex`, that is, iff the `Id`s of `value` have to be rewritten
// (see `canonicalizeForSerialization`) before it can be persisted. This also
// counts the `Id`s in columns without a variable, which are dropped by
// `canonicalizeForSerialization` and thus need no rewriting.
bool containsLocalVocabIds(const NamedResultCache::Value& value);

// Append a single segment to the `secondaryVocab` that consists of the words
// of all those `Id`s of type `LocalVocabIndex` of the `entries` that are
// neither contained in the vocabulary of the main index (or encodable, see
// above), nor already in the `secondaryVocab`. Return the number of these new
// words; if there are none, then the `secondaryVocab` stays unchanged.
// Afterwards, each of the `entries` can be canonicalized via
// `canonicalizeForSerialization`.
size_t addNewWordsToSecondaryVocab(const Entries& entries,
                                   SecondaryVocabulary& secondaryVocab);

// Return `id`, rewritten such that it is not of type `LocalVocabIndex` (see
// above). An `Id` of any other type is returned unchanged. Throw if `id`
// refers to a word that is neither contained in the vocabulary of the main
// index nor in the `secondaryVocab`, which means that
// `addNewWordsToSecondaryVocab` has not been called for the entry that
// contains `id`.
Id rewriteId(Id id, const SecondaryVocabulary& secondaryVocab);

// Return the order in which the columns of a table with `numColumns` columns
// and the given `varToColMap` are written to a blob: the columns of the
// variables in the lexicographic order of the variable names (a column that
// belongs to several variables is written only once). The columns without a
// variable are not written at all, because no query can refer to them. The
// result is the vector `oldColumnOfNewColumn`, which contains a subset of
// `0, ..., numColumns - 1`.
std::vector<ColumnIndex> canonicalColumnOrder(
    const VariableToColumnMap& varToColMap, size_t numColumns);

// Return the cache key of the canonicalized copy of an entry with the given
// `cacheKey`, whose columns are the columns `oldColumnOfNewColumn` of the
// entry (see `canonicalColumnOrder` and `canonicalizeForSerialization`). The
// cache key of an operation is derived from the cache keys of its children and
// the indices of the columns that it uses. The copy therefore needs a key that
// differs from the one of the original entry, because its columns and rows are
// in a different order and some of its columns may have been dropped. The key
// contains the `oldColumnOfNewColumn`, because two entries with the same
// `cacheKey` (which does not depend on the names of the variables) may have
// different variables, and thus different column orders in the copy. The
// copy is then determined by the `cacheKey` and the `oldColumnOfNewColumn`.
// The key is deterministic, so that two blobs of the same entry stay equal.
// The key of an entry that already is such a copy (for example because it was
// loaded from a blob) is returned unchanged, because canonicalizing it again
// does not change it.
std::string canonicalCacheKey(std::string_view cacheKey,
                              ql::span<const ColumnIndex> oldColumnOfNewColumn);

// The result of `canonicalizeWithPermutation`.
struct CanonicalizedValue {
  // The canonicalized copy of the value (see `canonicalizeForSerialization`).
  NamedResultCache::Value value_;
  // The vector `oldRowOfNewRow`: its element `i` is the row of the original
  // table that is the row `i` of the table of `value_` (the identity if the
  // rows were not permuted).
  std::vector<size_t> oldRowOfNewRow_;
};

// Return a copy of `value` that is ready to be written to a blob, which means:
// all `Id`s are rewritten via `rewriteId` (only if `value` contains any `Id`
// of type `LocalVocabIndex`, see `containsLocalVocabIds`), and the rows are in
// canonical order (see `CanonicalRowOrder.h`) with respect to the
// `resultSortedOn_` of the copy. The columns of the copy are the ones given
// by `canonicalColumnOrder`, in that order (the plan of a query may produce
// its columns in a different order after a change of the data, which would
// make two otherwise equal blobs differ); `varToColMap_` and `resultSortedOn_`
// of the copy are adapted accordingly. In particular, the `resultSortedOn_` of
// the copy is the longest prefix of the `resultSortedOn_` of `value` that
// contains no dropped column, so the sort order that the query plan produced
// is kept. The `cacheKey_` of the copy is the `canonicalCacheKey` of the one of
// `value`.
// The `value` itself is not modified. The table of the copy is an owning
// `IdTable` that is allocated via `allocator`.
//
// The rewritten `Id`s no longer compare like the `Id`s that they replace:
// all `Id`s of type `Datatype::SecondaryVocabIndex` compare greater than all
// `Id`s of the main vocabulary, whereas a `LocalVocabIndex` compares by the
// position at which its word would be sorted into the main vocabulary (see
// `ValueId::compareThreeWay`). The rows of the copy are therefore sorted
// again. If the rows are permuted, then the mapping from shapes to rows of the
// `cachedGeoIndex_` is permuted accordingly (see
// `SpatialJoinCachedIndex::withPermutedRows`).
//
// NOTE: Keeping the `resultSortedOn_` of `value` is required for queries with
// an explicit `INTERNAL SORT BY`, whose sort order is part of the query. For
// other queries, the sort order depends on the query plan and may differ
// between two runs of the same query, which makes the diff of the two blobs
// large. Distinguishing the two cases is left to the code that pins the
// queries.
//
// NOTE: The `localVocab_` of the copy is a clone of the one of `value`, which
// is still needed for the blank nodes that the entry may contain. Its words,
// however, are no longer referenced by any `Id` of the copy.
NamedResultCache::Value canonicalizeForSerialization(
    const NamedResultCache::Value& value,
    const SecondaryVocabulary& secondaryVocab,
    const NamedResultCache::Value::Allocator& allocator);

// Same as `canonicalizeForSerialization`, but additionally return the
// permutation of the rows that was applied.
CanonicalizedValue canonicalizeWithPermutation(
    const NamedResultCache::Value& value,
    const SecondaryVocabulary& secondaryVocab,
    const NamedResultCache::Value::Allocator& allocator);

}  // namespace qlever::namedCacheSecondaryVocab

#endif  // QLEVER_SRC_LIBQLEVER_NAMEDCACHESECONDARYVOCABREWRITER_H
