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

#include "../../util/FileTestHelpers.h"
#include "../../util/IdTestHelpers.h"
#include "global/VocabIndex.h"
#include "index/PartialVocabularyFilenames.h"
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
constexpr std::less<> lessThan{};

// Create the `QueueWord` for the occurrence of `word` with the given
// `localIndex` in the partial vocabulary `partialFileId`.
inline ad_utility::vocabulary_merger::detail::QueueWord makeQueueWord(
    std::string word, bool isExternal, size_t partialFileId,
    uint64_t localIndex) {
  return ad_utility::vocabulary_merger::detail::QueueWord{
      TripleComponentWithIndex{std::move(word), isExternal, localIndex},
      partialFileId};
}

// The number and filenames of a set of partial vocabularies, as created by
// `makePartialVocabularyFiles`.
struct PartialVocabularyFiles {
  // The number of partial vocabularies, in the form in which `mergeVocabulary`
  // expects it.
  size_t numPartialVocabularies_ = 0;
  // The file that holds the words of each of the partial vocabularies.
  std::vector<std::string> wordsFiles_;
  // The file that holds the ID map of each of the partial vocabularies. Hand
  // these to the `IdMapBatchWriter` and the `VocabularyMergePipeline`, which
  // take the filenames directly.
  std::vector<std::string> idMapFiles_;
};

// Return the number and the filenames of `numPartialVocabularies` partial
// vocabularies with the given `basename`, exactly as the vocabulary merger
// derives them.
//
// NOTE: This only computes the names, it creates no files. A test that
// actually creates them should use
// `makePartialVocabularyFilenamesInFreshDirectory` below, such that they are
// cleaned up again.
inline PartialVocabularyFiles makePartialVocabularyFiles(
    const std::string& basename, size_t numPartialVocabularies) {
  PartialVocabularyFiles files;
  files.numPartialVocabularies_ = numPartialVocabularies;
  for (size_t i = 0; i < numPartialVocabularies; ++i) {
    files.wordsFiles_.push_back(partialVocabularyWordsFilename(basename, i));
    files.idMapFiles_.push_back(partialVocabularyIdMapFilename(basename, i));
  }
  return files;
}

// Switch to a fresh working directory and return the filenames of
// `numPartialVocabularies` partial vocabularies with the given `basename`
// there (see `makePartialVocabularyFiles` above), together with the
// `absl::Cleanup` that restores the previous working directory and deletes the
// fresh one (see `ad_utility::testing::useFreshWorkingDirectory` in
// `test/util/FileTestHelpers.h`). Use it as
// `auto [filenames, cleanup] = makePartialVocabularyFilenamesInFreshDirectory(
//      basename, numPartialVocabularies);`.
//
// NOTE: This also only computes the names, the files themselves are created by
// the code under test.
inline auto makePartialVocabularyFilenamesInFreshDirectory(
    const std::string& basename, size_t numPartialVocabularies) {
  auto cleanup = ad_utility::testing::useFreshWorkingDirectory();
  return std::pair{makePartialVocabularyFiles(basename, numPartialVocabularies),
                   std::move(cleanup)};
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
