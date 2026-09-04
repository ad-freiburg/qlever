// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>

#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../../util/GTestHelpers.h"
#include "../../util/IdTestHelpers.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/vocabulary_merger/IdMap.h"
#include "index/vocabulary_merger/MergePipeline.h"
#include "util/File.h"

using namespace ad_utility::vocabulary_merger;
using ad_utility::vocabulary_merger::detail::QueueWord;
using ad_utility::vocabulary_merger::detail::VocabularyMergePipeline;
using ad_utility::vocabulary_merger::detail::WordBatch;
using ad_utility::vocabulary_merger::detail::WordBatchBuilder;

namespace {
auto V = ad_utility::testing::VocabId;

// A `WordComparator` that simply compares the words lexicographically and
// ignores the `isExternal` flags.
constexpr auto lessThan = [](std::string_view a, bool, std::string_view b,
                             bool) { return std::less<>{}(a, b); };

// Create the `QueueWord` for the occurrence of `word` with the given
// `localIndex` in the partial vocabulary `partialFileId`.
QueueWord makeQueueWord(std::string word, bool isExternal, size_t partialFileId,
                        uint64_t localIndex) {
  return QueueWord{
      TripleComponentWithIndex{std::move(word), isExternal, localIndex},
      partialFileId};
}
}  // namespace

// _____________________________________________________________________________
// Push the batches of a `WordBatchBuilder` through the pipeline (which is the
// second to fourth stage of the merging) and check the vocabulary that it
// writes as well as the resulting partial ID maps.
TEST(VocabularyMergePipeline, writeWordsAndIdMaps) {
  static constexpr size_t numFiles = 2;
  std::string basename = absl::StrCat(gtestCurrentTestName(), "-");
  std::vector<std::string> filenames;
  for (size_t i = 0; i < numFiles; ++i) {
    filenames.push_back(absl::StrCat(basename, PARTIAL_VOCAB_IDMAP_INFIX, i));
  }
  absl::Cleanup cleanup = [&filenames] {
    for (const auto& filename : filenames) {
      ad_utility::deleteFile(filename, false);
    }
  };

  std::vector<std::pair<std::string, bool>> vocabulary;
  auto wordCallback = [&vocabulary](std::string_view word,
                                    bool isExternal) -> uint64_t {
    vocabulary.emplace_back(word, isExternal);
    return vocabulary.size() - 1;
  };
  std::vector<std::unique_ptr<re2::RE2>> noRegexes;

  VocabularyMetaData metaData;
  {
    VocabularyMergePipeline pipeline{basename, numFiles};
    WordBatchBuilder builder;
    auto push = [&pipeline, &wordCallback, &noRegexes](WordBatch batch) {
      pipeline.push(std::move(batch), wordCallback, noRegexes);
    };
    // `"a"` is only in the first partial vocabulary, `"b"` in both (and
    // externalized in the second one), `"c"` only in the second one.
    builder.addMergedWords({makeQueueWord("\"a\"", false, 0, 0),
                            makeQueueWord("\"b\"", false, 0, 1),
                            makeQueueWord("\"b\"", true, 1, 0),
                            makeQueueWord("\"c\"", false, 1, 1)},
                           lessThan, push);
    builder.finish(push);
    metaData = pipeline.finish();
  }

  EXPECT_THAT(vocabulary,
              ::testing::ElementsAre(::testing::Pair("\"a\"", false),
                                     ::testing::Pair("\"b\"", true),
                                     ::testing::Pair("\"c\"", false)));
  EXPECT_EQ(metaData.numWordsTotal(), 3u);
  EXPECT_THAT(getIdMapFromFile(filenames[0]),
              ::testing::ElementsAre(IdMapEntry{0, V(0)}, IdMapEntry{1, V(1)}));
  EXPECT_THAT(getIdMapFromFile(filenames[1]),
              ::testing::ElementsAre(IdMapEntry{0, V(1)}, IdMapEntry{1, V(2)}));
}

// _____________________________________________________________________________
// A pipeline to which no batch was pushed creates empty ID maps and empty
// metadata.
TEST(VocabularyMergePipeline, noBatches) {
  std::string basename = absl::StrCat(gtestCurrentTestName(), "-");
  std::string filename = absl::StrCat(basename, PARTIAL_VOCAB_IDMAP_INFIX, 0);
  absl::Cleanup cleanup = [&filename] {
    ad_utility::deleteFile(filename, false);
  };
  VocabularyMergePipeline pipeline{basename, 1};
  auto metaData = pipeline.finish();
  EXPECT_EQ(metaData.numWordsTotal(), 0u);
  EXPECT_THAT(getIdMapFromFile(filename), ::testing::IsEmpty());
}
