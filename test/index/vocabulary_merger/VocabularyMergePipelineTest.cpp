// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include <chrono>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "../../util/FileTestHelpers.h"
#include "../../util/GTestHelpers.h"
#include "VocabularyMergerTestHelpers.h"
#include "index/vocabulary_merger/IdMap.h"
#include "index/vocabulary_merger/MergePipeline.h"
#include "index/vocabulary_merger/WordBatchBuilder.h"
#include "util/SourceLocation.h"
#include "util/TransparentFunctors.h"

using namespace ad_utility::vocabulary_merger;
using namespace vocabularyMergerTestHelpers;
using ad_utility::vocabulary_merger::detail::IdMapBatch;
using ad_utility::vocabulary_merger::detail::VocabularyMergePipeline;
using ad_utility::vocabulary_merger::detail::VocabularyMergePipelineImpl;
using ad_utility::vocabulary_merger::detail::WordBatch;
using ad_utility::vocabulary_merger::detail::WordBatchBuilder;
using ::testing::Pair;

namespace {
// The basename of the partial vocabularies that the tests below use. It needs
// no test-specific part, because each test that actually creates files runs in
// its own working directory (see `useFreshWorkingDirectory`).
const std::string partialVocabBasename = "vocab-";

// An ID map writer (the third stage of the pipeline) that fails on the first
// batch. The real `IdMapBatchWriter` cannot fail (see
// `VocabularyMergePipelineImpl::runAndCatchException`), so this is the only way
// to test that a failure of that stage is propagated.
class ThrowingIdMapBatchWriter {
 public:
  // Same interface as the `IdMapBatchWriter`, but the arguments are ignored
  // (nothing is written, so there also are no files to clean up).
  ThrowingIdMapBatchWriter([[maybe_unused]] const std::string& basename,
                           [[maybe_unused]] const std::vector<std::string>&
                               partialVocabularySuffixes) {}

  void writeBatch([[maybe_unused]] const IdMapBatch& batch) {
    throw std::runtime_error{"The ID map could not be written"};
  }

  void finish() {}
};

// Return the `WordBatchCallback` that pushes the batches of a
// `WordBatchBuilder` into the given `pipeline`. This is a template, because the
// tests use different instantiations of the `VocabularyMergePipelineImpl`.
template <typename Pipeline, typename WordCallback>
auto makePush(Pipeline& pipeline, WordCallback& wordCallback,
              const ad_utility::RegexSet& regexes) {
  return [&pipeline, &wordCallback, &regexes](WordBatch batch) {
    pipeline.push(std::move(batch), wordCallback, regexes);
  };
}

// Push a batch with the single word `"a"` into the `pipeline`, wait for the
// failure that this triggers, and then check that a batch that is pushed after
// the failure is skipped and that `finish()` rethrows a `std::runtime_error`
// whose message contains `expectedMessage`. Call `checkAfterFailure` in
// between, that is after the failure of the first batch, but before the second
// batch is pushed. This is a template for the same reason as `makePush` above.
template <typename Pipeline, typename WordCallback,
          typename CheckAfterFailure = ad_utility::Noop>
void expectFailureIsPropagated(
    Pipeline& pipeline, WordCallback& wordCallback,
    const ad_utility::RegexSet& regexes, std::string_view expectedMessage,
    const CheckAfterFailure& checkAfterFailure = ad_utility::noop,
    ad_utility::source_location loc = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(loc);
  WordBatchBuilder builder;
  auto push = makePush(pipeline, wordCallback, regexes);
  builder.addMergedWords({makeQueueWord("\"a\"", false, 0, 0)}, lessThan, push);
  builder.finish(push);
  // The batch is processed asynchronously, so wait for the failure. NOTE: The
  // `finish()` below would also wait, but it throws.
  while (!pipeline.hasFailed()) {
    // Sleep a little, else this loop would needlessly burn a whole CPU core.
    std::this_thread::sleep_for(std::chrono::microseconds{50});
  }
  checkAfterFailure();

  builder.addMergedWords({makeQueueWord("\"b\"", false, 0, 1)}, lessThan, push);
  builder.finish(push);
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(pipeline.finish(),
                                        ::testing::HasSubstr(expectedMessage),
                                        std::runtime_error);
}
}  // namespace

// _____________________________________________________________________________
// Push the batches of a `WordBatchBuilder` through the pipeline (which is the
// second to fourth stage of the merging) and check the vocabulary that it
// writes as well as the resulting partial ID maps.
TEST(VocabularyMergePipeline, writeWordsAndIdMaps) {
  auto cleanup = ad_utility::testing::useFreshWorkingDirectory();
  auto files = makePartialVocabularyFiles(partialVocabBasename, 2);

  std::vector<std::pair<std::string, bool>> vocabulary;
  auto wordCallback = makeCollectingWordCallback(vocabulary);
  ad_utility::RegexSet noRegexes;

  VocabularyMetaData metaData;
  {
    VocabularyMergePipeline pipeline{partialVocabBasename, files.suffixes_};
    WordBatchBuilder builder;
    auto push = makePush(pipeline, wordCallback, noRegexes);
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
              ::testing::ElementsAre(Pair("\"a\"", false), Pair("\"b\"", true),
                                     Pair("\"c\"", false)));
  EXPECT_EQ(metaData.numWordsTotal(), 3u);
  EXPECT_THAT(
      getIdMapFromFile(files.idMapFiles_[0]),
      ::testing::ElementsAre(IdMapEntry{L(0), V(0)}, IdMapEntry{L(1), V(1)}));
  EXPECT_THAT(
      getIdMapFromFile(files.idMapFiles_[1]),
      ::testing::ElementsAre(IdMapEntry{L(0), V(1)}, IdMapEntry{L(1), V(2)}));
}

// _____________________________________________________________________________
// A pipeline to which no batch was pushed creates empty ID maps and empty
// metadata.
TEST(VocabularyMergePipeline, noBatches) {
  auto cleanup = ad_utility::testing::useFreshWorkingDirectory();
  auto files = makePartialVocabularyFiles(partialVocabBasename, 1);
  VocabularyMergePipeline pipeline{partialVocabBasename, files.suffixes_};
  auto metaData = pipeline.finish();
  EXPECT_EQ(metaData.numWordsTotal(), 0u);
  EXPECT_THAT(getIdMapFromFile(files.idMapFiles_[0]), ::testing::IsEmpty());
}

// _____________________________________________________________________________
// An exception that is thrown by one of the stages (here by the word callback
// on the thread of the `wordWriterQueue_`) must not escape that thread. It is
// reported by `hasFailed()` and rethrown by `finish()`, and the batches that
// are pushed after the failure are skipped.
TEST(VocabularyMergePipeline, exceptionFromAStageIsPropagated) {
  auto cleanup = ad_utility::testing::useFreshWorkingDirectory();
  auto files = makePartialVocabularyFiles(partialVocabBasename, 1);

  size_t numCalls = 0;
  auto wordCallback = [&numCalls](std::string_view, bool) -> uint64_t {
    ++numCalls;
    throw std::runtime_error{"The vocabulary could not be written"};
  };
  ad_utility::RegexSet noRegexes;

  VocabularyMergePipeline pipeline{partialVocabBasename, files.suffixes_};
  expectFailureIsPropagated(pipeline, wordCallback, noRegexes,
                            "could not be written");
  // A batch that is pushed after the failure is skipped, so the callback is
  // called exactly once.
  EXPECT_EQ(numCalls, 1u);
}

// _____________________________________________________________________________
// An exception that is thrown by the third stage (the writing of the partial
// ID maps, here simulated by a `ThrowingIdMapBatchWriter`) must not escape the
// thread of the `idMapWriterQueue_`. It is reported by `hasFailed()` and
// rethrown by `finish()`, and no further batch is handed to that stage.
TEST(VocabularyMergePipeline, exceptionFromTheIdMapWritingIsPropagated) {
  std::vector<std::pair<std::string, bool>> vocabulary;
  auto wordCallback = makeCollectingWordCallback(vocabulary);
  ad_utility::RegexSet noRegexes;

  VocabularyMergePipelineImpl<ThrowingIdMapBatchWriter> pipeline{
      partialVocabBasename, {"0"}};
  expectFailureIsPropagated(
      pipeline, wordCallback, noRegexes, "ID map could not be written",
      [&vocabulary] {
        // The word itself was written by the second stage before the third one
        // failed.
        EXPECT_THAT(vocabulary, ::testing::ElementsAre(Pair("\"a\"", false)));
      });
  // The batch that was pushed after the failure was skipped, so no further
  // word was written.
  EXPECT_THAT(vocabulary, ::testing::ElementsAre(Pair("\"a\"", false)));
}
