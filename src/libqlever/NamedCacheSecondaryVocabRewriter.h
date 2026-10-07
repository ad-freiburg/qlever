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

#include <memory>
#include <utility>
#include <vector>

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
// `Id` of type `LocalVocabIndex`), and additionally brings its columns and rows
// into canonical order. This is done for every entry that is written to a
// blob, no matter whether it contains local vocab `Id`s, because the order
// of the rows has to be canonical for the diff of two blobs to be small.
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
// `LocalVocabIndex`, that is, iff `value` has to be rewritten via
// rewritten (see `canonicalizeForSerialization`) before it can be persisted.
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
// and the given `varToColMap` are written to a blob: first the columns of the
// variables in the lexicographic order of the variable names, then all columns
// without a variable in their original order. The result is the vector
// `oldColumnOfNewColumn`, a permutation of `0, ..., numColumns - 1`.
std::vector<ColumnIndex> canonicalColumnOrder(
    const VariableToColumnMap& varToColMap, size_t numColumns);

// The result of `canonicalizeWithPermutation`.
struct CanonicalizedValue {
  // The canonicalized copy of the value (see `canonicalizeForSerialization`).
  NamedResultCache::Value value_;
  // The row of the original table that is the row `i` of the table of `value_`
  // at the position `i` (the identity if the rows were not permuted).
  std::vector<size_t> oldRowOfNewRow_;
};

// Return a copy of `value` that is ready to be written to a blob, which means:
// all `Id`s are rewritten via `rewriteId` (only if `value` contains any `Id`
// of type `LocalVocabIndex`, see `containsLocalVocabIds`), and the rows are in
// canonical order (see `CanonicalRowOrder.h`) with respect to the
// `resultSortedOn_` of the copy. The columns of the copy are in the order given
// by `canonicalColumnOrder` (the plan of a query may produce its columns in a
// different order after a change of the data, which would make two otherwise
// equal blobs differ); `varToColMap_` and `resultSortedOn_` of the copy are
// adapted accordingly. The `value` itself is not modified. The table
// of the copy is an owning `IdTable` that is allocated via `allocator`.
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
// If `sortOnAllColumns` is true, then the `resultSortedOn_` of the copy is
// replaced by all of its columns in increasing order (so the rows are sorted
// lexicographically by all columns, which is a true statement about the copy).
// This makes the canonical order independent of the `resultSortedOn_` of
// `value`, which depends on the query plan, and thus may differ between two
// runs of the same query on the same data.
//
// NOTE: The `localVocab_` of the copy is a clone of the one of `value`, which
// is still needed for the blank nodes that the entry may contain. Its words,
// however, are no longer referenced by any `Id` of the copy.
NamedResultCache::Value canonicalizeForSerialization(
    const NamedResultCache::Value& value,
    const SecondaryVocabulary& secondaryVocab,
    const NamedResultCache::Value::Allocator& allocator,
    bool sortOnAllColumns = false);

// Same as `canonicalizeForSerialization`, but additionally return the
// permutation of the rows that was applied.
CanonicalizedValue canonicalizeWithPermutation(
    const NamedResultCache::Value& value,
    const SecondaryVocabulary& secondaryVocab,
    const NamedResultCache::Value::Allocator& allocator,
    bool sortOnAllColumns = false);

}  // namespace qlever::namedCacheSecondaryVocab

#endif  // QLEVER_SRC_LIBQLEVER_NAMEDCACHESECONDARYVOCABREWRITER_H
