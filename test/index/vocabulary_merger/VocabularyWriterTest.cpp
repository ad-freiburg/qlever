// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include <string>
#include <utility>
#include <vector>

#include "VocabularyMergerTestHelpers.h"
#include "index/vocabulary_merger/VocabularyWriter.h"

using namespace ad_utility::vocabulary_merger;
using namespace vocabularyMergerTestHelpers;
using ad_utility::vocabulary_merger::detail::LocalIdxToBatchMapping;
using ad_utility::vocabulary_merger::detail::LocalIdxToBatchMappings;
using ad_utility::vocabulary_merger::detail::UniqueWord;
using ad_utility::vocabulary_merger::detail::VocabularyWriter;
using ::testing::Pair;

// _____________________________________________________________________________
// The words of a batch are written to the vocabulary in order, and the
// resulting global IDs are stored in the returned batch. Blank nodes (`_:...`
// as well as IRIs that are fully matched by one of the regexes) are not
// written to the vocabulary, but get a blank node ID.
TEST(VocabularyWriter, writeWordsAndBlankNodes) {
  VocabularyWriter writer;
  std::vector<std::pair<std::string, bool>> written;
  auto wordCallback = makeCollectingWordCallback(written);
  ad_utility::RegexSet blankNodeIriRegexes{{"<http://ex/bn_.*>"},
                                           "for the test"};

  std::vector<UniqueWord> uniqueWords{UniqueWord{"\"lit\"", false},
                                      UniqueWord{"_:blank", false},
                                      UniqueWord{"<http://ex/bn_1>", false},
                                      UniqueWord{"<http://ex/other>", true}};
  // A single index mapping, which refers to the last of the words.
  LocalIdxToBatchMappings localIdxMappings;
  localIdxMappings.mappings_.push_back(
      LocalIdxToBatchMapping{2, 3, VocabIndex::make(17)});
  localIdxMappings.numMappings_ = 1;

  auto batch =
      writer.writeWordsToVocabulary(uniqueWords, std::move(localIdxMappings),
                                    wordCallback, blankNodeIriRegexes);

  // The two blank nodes were not written to the vocabulary, the `isExternal`
  // flag was passed on.
  EXPECT_THAT(written, ::testing::ElementsAre(Pair("\"lit\"", false),
                                              Pair("<http://ex/other>", true)));
  // Each of the words gets exactly one global ID, in the order of the words.
  EXPECT_THAT(batch.globalIds_,
              ::testing::ElementsAre(V(0), BN(0), BN(1), V(1)));
  // The index mappings are passed through unchanged.
  ASSERT_EQ(batch.localIdxMappings_.numMappings_, 1u);
  const auto& mapping = batch.localIdxMappings_.mappings_[0];
  EXPECT_EQ(mapping.partialVocabularyIndex_, 2u);
  EXPECT_EQ(mapping.indexOfWordInBatch_, 3u);
  EXPECT_EQ(mapping.indexOfWordInPartialVocabulary_, VocabIndex::make(17));
  // Only the words that were actually added to the vocabulary are counted.
  EXPECT_EQ(writer.metaData().numWordsTotal(), 2u);
}

// _____________________________________________________________________________
// The state of the writer is carried over from one batch to the next, in
// particular the indices of the blank nodes and the metadata.
TEST(VocabularyWriter, stateIsCarriedOverBetweenBatches) {
  VocabularyWriter writer;
  size_t numWords = 0;
  auto wordCallback = makeCountingWordCallback(numWords);
  ad_utility::RegexSet noRegexes;

  auto firstBatch = writer.writeWordsToVocabulary(
      {UniqueWord{"\"a\"", false}, UniqueWord{"_:x", false}},
      LocalIdxToBatchMappings{}, wordCallback, noRegexes);
  EXPECT_THAT(firstBatch.globalIds_, ::testing::ElementsAre(V(0), BN(0)));

  auto secondBatch = writer.writeWordsToVocabulary(
      {UniqueWord{"\"b\"", false}, UniqueWord{"_:y", false}},
      LocalIdxToBatchMappings{}, wordCallback, noRegexes);
  EXPECT_THAT(secondBatch.globalIds_, ::testing::ElementsAre(V(1), BN(1)));

  EXPECT_EQ(writer.metaData().numWordsTotal(), 2u);
}

// _____________________________________________________________________________
// A batch without any word yields no global IDs.
TEST(VocabularyWriter, emptyBatch) {
  VocabularyWriter writer;
  auto wordCallback = [](std::string_view, bool) -> uint64_t {
    ADD_FAILURE() << "The word callback must not be called";
    return 0;
  };
  ad_utility::RegexSet noRegexes;
  auto batch = writer.writeWordsToVocabulary({}, LocalIdxToBatchMappings{},
                                             wordCallback, noRegexes);
  EXPECT_THAT(batch.globalIds_, ::testing::IsEmpty());
  EXPECT_EQ(writer.metaData().numWordsTotal(), 0u);
}
