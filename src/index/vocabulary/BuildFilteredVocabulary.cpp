// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/vocabulary/BuildFilteredVocabulary.h"

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>

#include <string>
#include <utility>
#include <vector>

#include "index/vocabulary/CompressedVocabulary.h"
#include "index/vocabulary/SplitVocabulary.h"
#include "index/vocabulary/VocabularyInMemory.h"
#include "index/vocabulary/VocabularyInMemoryBinSearch.h"
#include "index/vocabulary/VocabularyInternalExternal.h"
#include "util/Exception.h"
#include "util/File.h"
#include "util/Log.h"
#include "util/RegexSet.h"
#include "util/TypeTraits.h"

namespace {

// The number of entries that `writeSurvivingEntries` below has kept and
// dropped, for logging purposes.
struct NumKeptAndDropped {
  size_t numKept_ = 0;
  size_t numDropped_ = 0;
};

// Determine the `VocabularyType` of the filtered vocabulary from the type of
// the source `vocabulary`: an uncompressed source yields an uncompressed
// vocabulary with holes, a compressed source a compressed one. Throw for the
// cases that are not supported (see `buildFilteredVocabulary`).
ad_utility::VocabularyType targetVocabularyType(
    const PolymorphicVocabulary& vocabulary) {
  return std::visit(
      [](const auto& vocab) -> ad_utility::VocabularyType {
        using T = std::decay_t<decltype(vocab)>;
        // NOTE: The types below are listed explicitly (instead of using a
        // catch-all `else`), so that adding another alternative to
        // `PolymorphicVocabulary::Variant` (see also `VocabularyType.h`) makes
        // the `static_assert` in the last branch fail, which forces a decision
        // about the new alternative instead of silently throwing at runtime.
        if constexpr (ad_utility::SameAsAny<T, VocabularyInMemory,
                                            VocabularyInternalExternal>) {
          return ad_utility::VocabularyType::InMemoryUncompressedWithHoles;
        } else if constexpr (ad_utility::SameAsAny<
                                 T, CompressedVocabulary<VocabularyInMemory>,
                                 CompressedVocabulary<
                                     VocabularyInternalExternal>>) {
          return ad_utility::VocabularyType::InMemoryCompressedWithHoles;
        } else if constexpr (ad_utility::SameAsAny<
                                 T, VocabularyInMemoryBinSearch,
                                 CompressedVocabulary<
                                     VocabularyInMemoryBinSearch>>) {
          AD_THROW(
              "Filtering a vocabulary that already is a vocabulary with holes "
              "(vocabulary type \"in-memory-uncompressed-with-holes\" or "
              "\"in-memory-compressed-with-holes\") is not supported. NOTE: "
              "This is not an inherent restriction, as the entries of such a "
              "vocabulary can be scanned together with their (non-contiguous) "
              "indices just like those of any other vocabulary, so that "
              "filtering them would simply yield a vocabulary with more holes. "
              "It is deliberately not implemented (and hence also not tested), "
              "because there currently is no use case for it.");
        } else {
          static_assert(
              ad_utility::SameAsAny<T, SplitGeoVocabulary<CompressedVocabulary<
                                           VocabularyInternalExternal>>>);
          AD_THROW(
              "Filtering a vocabulary of type "
              "\"on-disk-compressed-geo-split\" is not supported, because its "
              "marker-encoded vocabulary indices are not ascending in the "
              "order of the sorted words, and because its precomputed "
              "geometry information cannot be represented by a vocabulary "
              "with holes");
        }
      },
      vocabulary.getUnderlyingVocabulary());
}

// Write all entries of `vocabulary` that match none of the `regexes` to the
// `wordWriter`, keeping their original vocabulary indices, and `finish` the
// `wordWriter`. Return the number of kept and dropped entries.
template <typename WordWriter>
NumKeptAndDropped writeSurvivingEntries(const PolymorphicVocabulary& vocabulary,
                                        const ad_utility::RegexSet& regexes,
                                        WordWriter& wordWriter) {
  NumKeptAndDropped result;
  // NOTE: `entry.word_` is only valid until the range is advanced (see
  // `IndexAndWord`), so it is consumed immediately inside the loop.
  for (const IndexAndWord& entry : vocabulary.scanAll()) {
    if (regexes.matchesAny(entry.word_)) {
      ++result.numDropped_;
      continue;
    }
    wordWriter(entry.word_, entry.index_);
    ++result.numKept_;
  }
  wordWriter.finish();
  return result;
}

// Build the filtered vocabulary at `temporaryBasename` using the `WordWriter`
// of the given `Vocabulary` (which is one of the two vocabularies with holes),
// and delete all the files that were written again. Return the filtered
// vocabulary together with the number of kept and dropped entries.
//
// NOTE: Both of the possible vocabulary types load all their contents into
// memory in `open`, so the temporary files can be (and are) deleted as soon as
// the vocabulary is opened. This also happens if an exception is thrown.
template <typename Vocabulary>
std::pair<PolymorphicVocabulary, NumKeptAndDropped> buildAndDeleteFiles(
    const PolymorphicVocabulary& vocabulary,
    const ad_utility::RegexSet& regexes, const std::string& temporaryBasename,
    ad_utility::VocabularyType type) {
  // The names of the files that the `WordWriter` below creates, filled as soon
  // as that writer exists (which is what knows the suffixes).
  std::vector<std::string> temporaryFilenames;
  // NOTE: This is deliberately declared before the scope of the `wordWriter`
  // below, so that the files are deleted only after that writer has been
  // destroyed. The destructor of a `WordWriter` writes the remaining buffers to
  // disk if `finish` was not called, which is exactly what happens when an
  // exception is thrown.
  absl::Cleanup deleteTemporaryFiles = [&temporaryFilenames]() {
    for (const std::string& filename : temporaryFilenames) {
      // Do not warn if the file does not exist: if an exception was thrown,
      // some of the files may never have been created.
      ad_utility::deleteFile(filename, false);
    }
  };

  NumKeptAndDropped numKeptAndDropped;
  {
    typename Vocabulary::WordWriter wordWriter{temporaryBasename};
    for (std::string_view suffix : wordWriter.fileSuffixes()) {
      temporaryFilenames.push_back(absl::StrCat(temporaryBasename, suffix));
    }
    numKeptAndDropped = writeSurvivingEntries(vocabulary, regexes, wordWriter);
  }

  PolymorphicVocabulary result;
  result.open(temporaryBasename, type);
  return {std::move(result), numKeptAndDropped};
}
}  // namespace

// _____________________________________________________________________________
FilteredVocabulary buildFilteredVocabulary(
    const PolymorphicVocabulary& vocabulary,
    const std::vector<std::string>& excludedEntryRegexes,
    const std::string& temporaryBasename) {
  AD_CONTRACT_CHECK(
      !excludedEntryRegexes.empty(),
      "`buildFilteredVocabulary` requires a non-empty list of regexes");
  ad_utility::RegexSet regexes{excludedEntryRegexes,
                               "for excluding vocabulary entries"};
  auto type = targetVocabularyType(vocabulary);

  // NOTE: `CompressedVocabulary::makeDiskWriterPtr` cannot be used here,
  // because the `WordWriter` of a vocabulary with holes does not implement the
  // `WordWriterBase` interface (its `operator()` takes the index of the word
  // instead of the `isExternal` flag).
  using CompressedWithHoles = CompressedVocabulary<VocabularyInMemoryBinSearch>;
  auto [filteredVocabulary, numKeptAndDropped] =
      type == ad_utility::VocabularyType::InMemoryCompressedWithHoles
          ? buildAndDeleteFiles<CompressedWithHoles>(vocabulary, regexes,
                                                     temporaryBasename, type)
          : buildAndDeleteFiles<VocabularyInMemoryBinSearch>(
                vocabulary, regexes, temporaryBasename, type);

  AD_LOG_INFO << "Built a filtered vocabulary of type " << type.toString()
              << " with " << numKeptAndDropped.numKept_ << " entries, dropped "
              << numKeptAndDropped.numDropped_
              << " entries that matched one of the " << regexes.size()
              << " given regexes" << std::endl;
  return FilteredVocabulary{std::move(filteredVocabulary), type};
}
