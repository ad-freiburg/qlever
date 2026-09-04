// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_VOCABULARY_BUILDFILTEREDVOCABULARY_H
#define QLEVER_SRC_INDEX_VOCABULARY_BUILDFILTEREDVOCABULARY_H

#include <string>
#include <vector>

#include "index/vocabulary/PolymorphicVocabulary.h"
#include "index/vocabulary/VocabularyType.h"

// The result of `buildFilteredVocabulary` below.
struct FilteredVocabulary {
  // The filtered vocabulary. Its active alternative is one of the
  // `...WithHoles` types (see `ad_utility::VocabularyType`).
  PolymorphicVocabulary vocabulary_;
  // The type of `vocabulary_`, which the reading side needs in order to
  // `resetToType` before deserializing.
  ad_utility::VocabularyType type_;
};

// Build an in-memory copy of `vocabulary` that contains all its entries except
// those that match any of the regexes in `excludedEntryRegexes` (matched as a
// full match via `ad_utility::RegexSet`, like the regexes for
// `IndexImpl::setBlankNodeIriRegexes`). In contrast to those, the regexes here
// are matched against every vocabulary entry, not only against the IRIs, so
// that literals can be excluded as well;
// to exclude only IRIs, let the regex start with `<` and end with `>`. The
// surviving entries keep their original vocabulary indices, so that `Id`s that
// refer to them stay valid; the result therefore is a vocabulary with holes
// (see `VocabularyInMemoryBinSearch`), and looking up an index that was
// excluded yields `ad_utility::vocabulary::placeholderForMissingVocabIndex`.
//
// The result is a compressed vocabulary
// (`ad_utility::VocabularyType::InMemoryCompressedWithHoles`) if and only if
// `vocabulary` is compressed, and an uncompressed one
// (`ad_utility::VocabularyType::InMemoryUncompressedWithHoles`) otherwise. The
// `temporaryBasename` is used for the intermediate on-disk representation of
// the result (the vocabulary implementations can only be built via a
// `WordWriter` that writes to disk); all files that are created there (which
// the `WordWriter` reports via `fileSuffixes`) are deleted again before
// returning (also if an exception is thrown), because both of the possible
// result types load everything into memory when they are opened.
//
// Throw if `vocabulary` is a `SplitVocabulary` (i.e. a vocabulary of type
// `on-disk-compressed-geo-split`): its marker-encoded indices are not ascending
// in the order of the (sorted) words, which a vocabulary with holes requires,
// and its precomputed `GeometryInfo` cannot be represented by any of the
// possible result types. Also throw if `excludedEntryRegexes` contains a string
// that is not a valid regular expression, and if `vocabulary` already is a
// vocabulary with holes (which is not an inherent restriction, but simply has
// no use case yet, see the exception message).
//
// PRECONDITION: `excludedEntryRegexes` must not be empty; the caller has to
// handle that case (in which the complete vocabulary survives and hence nothing
// has to be built) itself.
FilteredVocabulary buildFilteredVocabulary(
    const PolymorphicVocabulary& vocabulary,
    const std::vector<std::string>& excludedEntryRegexes,
    const std::string& temporaryBasename);

#endif  // QLEVER_SRC_INDEX_VOCABULARY_BUILDFILTEREDVOCABULARY_H
