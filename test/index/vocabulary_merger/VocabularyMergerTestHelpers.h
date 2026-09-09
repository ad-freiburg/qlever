// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_TEST_INDEX_VOCABULARY_MERGER_VOCABULARYMERGERTESTHELPERS_H
#define QLEVER_TEST_INDEX_VOCABULARY_MERGER_VOCABULARYMERGERTESTHELPERS_H

#include <absl/strings/str_cat.h>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "../../util/IdTestHelpers.h"
#include "global/VocabIndex.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/vocabulary_merger/QueueWord.h"

// Helpers that are shared by the tests of the vocabulary merger (see
// `index/VocabularyMerger.h` and the individual stages in
// `index/vocabulary_merger/`).
namespace vocabularyMergerTestHelpers {

// Shorthand for the global ID that a word has in the merged vocabulary.
inline auto V = ad_utility::testing::VocabId;
// Shorthand for the local index that a word has inside a partial vocabulary.
inline auto L = &VocabIndex::make;
// Shorthand for the ID of a blank node.
inline auto BN = ad_utility::testing::BlankNodeId;

// A `WordComparator` that simply compares the words lexicographically.
constexpr auto lessThan = [](std::string_view a, std::string_view b) {
  return std::less<>{}(a, b);
};

// Create the `QueueWord` for the occurrence of `word` with the given
// `localIndex` in the partial vocabulary `partialFileId`.
inline ad_utility::vocabulary_merger::detail::QueueWord makeQueueWord(
    std::string word, bool isExternal, size_t partialFileId,
    uint64_t localIndex) {
  return ad_utility::vocabulary_merger::detail::QueueWord{
      TripleComponentWithIndex{std::move(word), isExternal, localIndex},
      partialFileId};
}

// The suffixes and filenames of a set of partial vocabularies, as created by
// `makePartialVocabularyFiles`.
struct PartialVocabularyFiles {
  // The suffixes `"0"`, `"1"`, ..., in the form in which `mergeVocabulary`
  // expects them.
  std::vector<std::string> suffixes_;
  // The file that holds the words of each of the partial vocabularies.
  std::vector<std::string> wordsFiles_;
  // The file that holds the ID map of each of the partial vocabularies.
  std::vector<std::string> idMapFiles_;
};

// Return the suffixes and the filenames for `numPartialVocabularies` partial
// vocabularies with the given `basename`, exactly as the vocabulary merger
// derives them.
//
// NOTE: This only computes the names, it creates no files. Combine it with
// `ad_utility::testing::useFreshWorkingDirectory()` (see
// `test/util/FileTestHelpers.h`), such that the files that a test creates are
// cleaned up again.
inline PartialVocabularyFiles makePartialVocabularyFiles(
    const std::string& basename, size_t numPartialVocabularies) {
  PartialVocabularyFiles files;
  for (size_t i = 0; i < numPartialVocabularies; ++i) {
    files.suffixes_.push_back(std::to_string(i));
    files.wordsFiles_.push_back(
        absl::StrCat(basename, PARTIAL_VOCAB_WORDS_INFIX, i));
    files.idMapFiles_.push_back(
        absl::StrCat(basename, PARTIAL_VOCAB_IDMAP_INFIX, i));
  }
  return files;
}

// A `WordCallback` that appends each word together with its `isExternal` flag
// to `words` and assigns the words the consecutive indices `0, 1, ...`.
inline auto makeCollectingWordCallback(
    std::vector<std::pair<std::string, bool>>& words) {
  return [&words](std::string_view word, bool isExternal) -> uint64_t {
    words.emplace_back(word, isExternal);
    return words.size() - 1;
  };
}

// A `WordCallback` that only counts the words and assigns them the consecutive
// indices `0, 1, ...`.
inline auto makeCountingWordCallback(size_t& numWords) {
  return [&numWords](std::string_view, bool) -> uint64_t { return numWords++; };
}
}  // namespace vocabularyMergerTestHelpers

#endif  // QLEVER_TEST_INDEX_VOCABULARY_MERGER_VOCABULARYMERGERTESTHELPERS_H
