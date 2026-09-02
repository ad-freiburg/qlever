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
#include <re2/re2.h>

#include <memory>

#include "backports/algorithm.h"
#include "index/vocabulary/CompressedVocabulary.h"
#include "index/vocabulary/SplitVocabulary.h"
#include "index/vocabulary/VocabularyInMemory.h"
#include "index/vocabulary/VocabularyInMemoryBinSearch.h"
#include "index/vocabulary/VocabularyInternalExternal.h"
#include "util/Exception.h"
#include "util/File.h"
#include "util/Log.h"
#include "util/TypeTraits.h"

namespace {

// The number of entries that `writeSurvivingEntries` below has kept and
// dropped, for logging purposes.
struct NumKeptAndDropped {
  size_t numKept_ = 0;
  size_t numDropped_ = 0;
};

// Compile each of the `regexes` into an `RE2` object. Throw a descriptive
// exception for a string that is not a valid regular expression.
std::vector<std::unique_ptr<re2::RE2>> compileRegexes(
    const std::vector<std::string>& regexes) {
  std::vector<std::unique_ptr<re2::RE2>> compiledRegexes;
  for (const std::string& regex : regexes) {
    // `RE2` does not throw for an invalid pattern but stores an error state,
    // which we turn into a user-readable exception here.
    auto compiledRegex = std::make_unique<re2::RE2>(regex, re2::RE2::Quiet);
    if (!compiledRegex->ok()) {
      throw std::runtime_error{absl::StrCat(
          "The regex \"", regex,
          "\" for excluding vocabulary entries is not a valid regular "
          "expression (as understood by Google's RE2 library): ",
          compiledRegex->error())};
    }
    compiledRegexes.push_back(std::move(compiledRegex));
  }
  return compiledRegexes;
}

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
              "\"in-memory-compressed-with-holes\") is not supported");
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

// Write all entries of `vocabulary` that do not match any of the `regexes` to
// the `wordWriter`, keeping their original vocabulary indices, and `finish` the
// `wordWriter`. Return the number of kept and dropped entries.
template <typename WordWriter>
NumKeptAndDropped writeSurvivingEntries(
    const PolymorphicVocabulary& vocabulary,
    const std::vector<std::unique_ptr<re2::RE2>>& regexes,
    WordWriter& wordWriter) {
  NumKeptAndDropped result;
  // NOTE: `entry.word_` is only valid until the range is advanced (see
  // `IndexAndWord`), so it is consumed immediately inside the loop.
  for (const IndexAndWord& entry : vocabulary.scanAll()) {
    bool isExcluded = ql::ranges::any_of(regexes, [&entry](const auto& regex) {
      return re2::RE2::FullMatch(entry.word_, *regex);
    });
    if (isExcluded) {
      ++result.numDropped_;
      continue;
    }
    wordWriter(entry.word_, entry.index_);
    ++result.numKept_;
  }
  wordWriter.finish();
  return result;
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
  auto regexes = compileRegexes(excludedEntryRegexes);
  auto type = targetVocabularyType(vocabulary);
  bool isCompressed =
      type == ad_utility::VocabularyType::InMemoryCompressedWithHoles;

  // The names of the files that the `WordWriter`s below create. For the
  // compressed case, the suffixes are those that `CompressedVocabulary` (see
  // its `wordsSuffix` and `decodersSuffix`) and the underlying
  // `VocabularyInMemoryBinSearch` append to the basename that is passed to
  // `open`.
  std::vector<std::string> temporaryFilenames;
  if (isCompressed) {
    temporaryFilenames = {absl::StrCat(temporaryBasename, ".words"),
                          absl::StrCat(temporaryBasename, ".words.ids"),
                          absl::StrCat(temporaryBasename, ".codebooks")};
  } else {
    temporaryFilenames = {temporaryBasename,
                          absl::StrCat(temporaryBasename, ".ids")};
  }
  // Both of the possible vocabulary types load all their contents into memory
  // in `open`, so the temporary files can be (and are) deleted again as soon as
  // the vocabulary is opened. This also happens if an exception is thrown.
  absl::Cleanup deleteTemporaryFiles = [&temporaryFilenames]() {
    for (const std::string& filename : temporaryFilenames) {
      // Do not warn if the file does not exist: if an exception was thrown,
      // some of the files may never have been created.
      ad_utility::deleteFile(filename, false);
    }
  };

  NumKeptAndDropped numKeptAndDropped;
  if (isCompressed) {
    // NOTE: `CompressedVocabulary::makeDiskWriterPtr` cannot be used here,
    // because the `WordWriter` of a vocabulary with holes does not implement
    // the `WordWriterBase` interface (its `operator()` takes the index of the
    // word instead of the `isExternal` flag).
    CompressedVocabulary<VocabularyInMemoryBinSearch>::WordWriter wordWriter{
        temporaryFilenames.at(0), temporaryFilenames.at(2)};
    numKeptAndDropped = writeSurvivingEntries(vocabulary, regexes, wordWriter);
  } else {
    VocabularyInMemoryBinSearch::WordWriter wordWriter{temporaryBasename};
    numKeptAndDropped = writeSurvivingEntries(vocabulary, regexes, wordWriter);
  }

  FilteredVocabulary result{PolymorphicVocabulary{}, type};
  result.vocabulary_.open(temporaryBasename, type);
  AD_LOG_INFO << "Built a filtered vocabulary of type " << type.toString()
              << " with " << numKeptAndDropped.numKept_ << " entries, dropped "
              << numKeptAndDropped.numDropped_
              << " entries that matched one of the " << regexes.size()
              << " given regexes" << std::endl;
  return result;
}
