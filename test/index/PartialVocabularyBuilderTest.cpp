// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

#include "../util/GTestHelpers.h"
#include "../util/TripleComponentTestHelpers.h"
#include "backports/algorithm.h"
#include "global/Constants.h"
#include "global/SpecialIds.h"
#include "index/PartialVocabularyBuilder.h"
#include "rdfTypes/Iri.h"
#include "util/HashMap.h"

namespace {
using namespace qlever::partialVocabularyBuilder;
using ad_utility::testing::iri;
using ad_utility::testing::tripleComponentLiteral;
using ::testing::HasSubstr;

// A triple, reconstructed from the local IDs and the words of the partial
// vocabulary that they refer to. The strings are the `toRdfLiteral`
// representations of the components.
using StringTriple = std::array<std::string, NumColumnsIndexBuilding>;

// An asynchronous "parser" that delivers the given `batches` one after the
// other, and `nullopt` once they are exhausted. If `failAtBatch` is set, the
// call that would deliver the batch with that index fails with a
// `std::runtime_error` instead, and all subsequent calls deliver `nullopt`
// (the same contract as the real parsers, see `AsyncRdfParserBase`).
//
// The buffers that the callers pass in are not used for the batches (which are
// fixed), but they are checked to be empty, and those that already have a
// capacity (i.e. that were recycled by their caller) are counted in
// `*numCallsWithReusedBuffer` if that pointer is not null.
class MockParser : public AsyncRdfParserBase {
 private:
  std::vector<std::vector<TurtleTriple>> batches_;
  std::optional<size_t> failAtBatch_;
  std::atomic<size_t>* numCallsWithReusedBuffer_;
  std::mutex mutex_;
  size_t nextBatch_ = 0;
  bool failed_ = false;

 public:
  MockParser(const ql::any_io_executor& executor,
             std::vector<std::vector<TurtleTriple>> batches,
             std::optional<size_t> failAtBatch = std::nullopt,
             std::atomic<size_t>* numCallsWithReusedBuffer = nullptr)
      : AsyncRdfParserBase{executor},
        batches_{std::move(batches)},
        failAtBatch_{failAtBatch},
        numCallsWithReusedBuffer_{numCallsWithReusedBuffer} {}

 protected:
  void asyncGetBatchImpl(std::vector<TurtleTriple> buffer,
                         Handler handler) override {
    EXPECT_TRUE(buffer.empty());
    if (numCallsWithReusedBuffer_ != nullptr && buffer.capacity() > 0) {
      numCallsWithReusedBuffer_->fetch_add(1);
    }
    std::lock_guard l{mutex_};
    if (failed_ || nextBatch_ >= batches_.size()) {
      handler(nullptr, std::nullopt);
      return;
    }
    if (failAtBatch_ == nextBatch_) {
      failed_ = true;
      ++nextBatch_;
      handler(std::make_exception_ptr(std::runtime_error{"parse error"}),
              std::nullopt);
      return;
    }
    handler(nullptr, std::move(batches_.at(nextBatch_++)));
  }
};

// A mock for the `Index` template parameter of `PartialVocabularyBuilder.h`.
// `processTriple` keeps all components as strings (and extracts the language
// tag of the object, like `IndexImpl::processTriple`), and
// `writePartialVocabulary` decodes the local IDs back to strings via the
// words of the written partial vocabulary and stores the result in memory.
class MockIndex {
 public:
  struct PartialVocabulary {
    std::set<std::string> words_;
    std::vector<StringTriple> triples_;
  };

 private:
  std::mutex mutex_;
  std::map<size_t, PartialVocabulary> written_;
  bool throwOnWrite_ = false;

 public:
  explicit MockIndex(bool throwOnWrite = false) : throwOnWrite_{throwOnWrite} {}

  ProcessedTriple processTriple(TurtleTriple&& triple) const {
    ProcessedTriple result;
    if (triple.object_.isLiteral() &&
        triple.object_.getLiteral().hasLanguageTag()) {
      result.langtag_ =
          std::string(triple.object_.getLiteral().getLanguageTag());
    }
    result.triple_ =
        Triple{std::move(triple.subject_), std::move(triple.predicate_),
               std::move(triple.object_), std::move(triple.graphIri_)};
    return result;
  }

  void writePartialVocabulary(size_t partialVocabIdx,
                              const ItemMapAndBuffer& items,
                              std::vector<IdRow>& localIds) {
    if (throwOnWrite_) {
      throw std::runtime_error{"write error"};
    }
    PartialVocabulary vocab;
    ad_utility::HashMap<uint64_t, std::string> wordsByLocalId;
    for (const auto& [word, indexAndFlag] : items.map_) {
      vocab.words_.emplace(word);
      wordsByLocalId[indexAndFlag.index()] = std::string{word};
    }
    for (const auto& row : localIds) {
      StringTriple triple;
      for (size_t i = 0; i < row.size(); ++i) {
        EXPECT_EQ(row[i].getDatatype(), Datatype::VocabIndex);
        // Every local ID must refer to a word of this partial vocabulary.
        triple[i] = wordsByLocalId.at(row[i].getVocabIndex().get());
      }
      vocab.triples_.push_back(std::move(triple));
    }
    std::lock_guard l{mutex_};
    auto [it, wasInserted] =
        written_.try_emplace(partialVocabIdx, std::move(vocab));
    // Each index must be written exactly once.
    EXPECT_TRUE(wasInserted) << "Index " << partialVocabIdx << " written twice";
  }

  // The partial vocabularies that were written, by index.
  const std::map<size_t, PartialVocabulary>& written() const {
    return written_;
  }

  // All the triples of all written partial vocabularies, sorted.
  std::vector<StringTriple> allTriplesSorted() const {
    std::vector<StringTriple> result;
    for (const auto& [idx, vocab] : written_) {
      result.insert(result.end(), vocab.triples_.begin(), vocab.triples_.end());
    }
    ql::ranges::sort(result);
    return result;
  }
};

// Create the triple `<s{i}> <p> <o{i}>` in the default graph. If
// `subjectPadding` is nonzero, append that many `x` characters to the subject,
// so that the strings of the triple can dominate the memory of a partial
// vocabulary.
TurtleTriple makeTriple(size_t i, size_t subjectPadding = 0) {
  return TurtleTriple{
      iri(absl::StrCat("<s", i, std::string(subjectPadding, 'x'), ">")),
      iri("<p>"), iri(absl::StrCat("<o", i, ">"))};
}

// The `toRdfLiteral` representation of `component`.
std::string str(const TripleComponent& component) {
  return toRdfLiteral(component);
}

// The `StringTriple` that `triple` (in the default graph) is expected to be
// decoded to by `MockIndex::writePartialVocabulary`.
StringTriple expectedStringTriple(const TurtleTriple& triple) {
  return {str(triple.subject_), str(triple.predicate_), str(triple.object_),
          std::string{DEFAULT_GRAPH_IRI}};
}

// The words that every partial vocabulary contains in addition to the
// components of its triples: the special IRIs which every `ItemMapManager`
// adds to its map.
std::set<std::string> specialWords() {
  std::set<std::string> result;
  for (const auto& [specialIri, id] : qlever::specialIds()) {
    result.insert(specialIri);
  }
  return result;
}

// Run the complete pipeline of `PartialVocabularyBuilder.h` with `numThreads`
// task chains on the given `batches`, and return the shared state (for the
// counters) together with the mock index (for the written vocabularies).
struct RunResult {
  size_t numPartialVocabularies_;
  size_t numTriples_;
  bool stopRequested_;
  // The number of `asyncGetBatch` calls that passed a buffer which a task
  // chain had already used for a previous batch, see `MockParser`.
  size_t numCallsWithReusedBuffer_;
};
RunResult run(MockIndex& index, std::vector<std::vector<TurtleTriple>> batches,
              FirstPassMemoryBudget budget, size_t numThreads,
              std::optional<size_t> failAtBatch = std::nullopt) {
  TripleComponentComparator comparator;

  FirstPassSharedState<MockIndex> shared{&index, &comparator, budget};
  // Outlives `runTaskChains` (and hence the `MockParser` that writes it).
  std::atomic<size_t> numCallsWithReusedBuffer{0};
  runTaskChains(shared, numThreads,
                [&batches, failAtBatch,
                 &numCallsWithReusedBuffer](const ql::any_io_executor& executor)
                    -> std::unique_ptr<AsyncRdfParserBase> {
                  return std::make_unique<MockParser>(
                      executor, std::move(batches), failAtBatch,
                      &numCallsWithReusedBuffer);
                });
  return {shared.nextPartialVocabIdx_.load(), shared.numTriples_.load(),
          shared.stopRequested_.load(), numCallsWithReusedBuffer.load()};
}

// Call `run` above with a memory budget that is never reached, so that a task
// chain writes its partial vocabulary as soon as it has at least
// `maxTriplesPerPartial` input triples.
RunResult run(MockIndex& index, std::vector<std::vector<TurtleTriple>> batches,
              size_t maxTriplesPerPartial, size_t numThreads,
              std::optional<size_t> failAtBatch = std::nullopt) {
  return run(index, std::move(batches),
             FirstPassMemoryBudget{ad_utility::MemorySize::terabytes(1),
                                   maxTriplesPerPartial},
             numThreads, failAtBatch);
}
}  // namespace

// _____________________________________________________________________________
TEST(PartialVocabularyBuilder, singleChainSeveralPartialVocabularies) {
  // Three batches of two triples each, with three triples per partial
  // vocabulary. A single chain processes the batches in order, so the first
  // partial vocabulary contains the first two batches (the threshold is
  // checked after each complete batch), and the second one the remaining
  // batch (written at the end of the input).
  std::vector<std::vector<TurtleTriple>> batches{
      {makeTriple(0), makeTriple(1)},
      {makeTriple(2), makeTriple(3)},
      {makeTriple(4), makeTriple(5)}};
  MockIndex index;
  auto result = run(index, batches, 3, 1);
  EXPECT_EQ(result.numPartialVocabularies_, 2u);
  EXPECT_EQ(result.numTriples_, 6u);
  EXPECT_FALSE(result.stopRequested_);

  const auto& written = index.written();
  ASSERT_EQ(written.size(), 2u);
  const auto& first = written.at(0);
  const auto& second = written.at(1);
  EXPECT_THAT(first.triples_,
              ::testing::ElementsAre(expectedStringTriple(makeTriple(0)),
                                     expectedStringTriple(makeTriple(1)),
                                     expectedStringTriple(makeTriple(2)),
                                     expectedStringTriple(makeTriple(3))));
  EXPECT_THAT(second.triples_,
              ::testing::ElementsAre(expectedStringTriple(makeTriple(4)),
                                     expectedStringTriple(makeTriple(5))));

  // The words of a partial vocabulary are exactly the components of its
  // triples plus the special IRIs.
  auto expectedWords = specialWords();
  for (const auto& triple : first.triples_) {
    expectedWords.insert(triple.begin(), triple.end());
  }
  EXPECT_EQ(first.words_, expectedWords);
  expectedWords = specialWords();
  for (const auto& triple : second.triples_) {
    expectedWords.insert(triple.begin(), triple.end());
  }
  EXPECT_EQ(second.words_, expectedWords);
}

// _____________________________________________________________________________
TEST(PartialVocabularyBuilder, batchBuffersAreReused) {
  // A single chain consumes four batches one after the other, which takes five
  // calls (the last one reports the end of the input). It hands the buffer of
  // every batch it has consumed back to the parser, so only the very first of
  // those calls passes a buffer without a capacity.
  std::vector<std::vector<TurtleTriple>> batches{
      {makeTriple(0)}, {makeTriple(1)}, {makeTriple(2)}, {makeTriple(3)}};
  MockIndex index;
  auto result = run(index, batches, 100, 1);
  EXPECT_EQ(result.numTriples_, 4u);
  EXPECT_EQ(result.numCallsWithReusedBuffer_, 4u);
}

// _____________________________________________________________________________
TEST(PartialVocabularyBuilder, severalChainsRacingForBatches) {
  // Twenty batches of three triples each, five triples per partial
  // vocabulary, four task chains racing for the batches. Which chain gets
  // which batch is nondeterministic, so only order-independent properties are
  // checked.
  constexpr size_t numBatches = 20;
  constexpr size_t batchSize = 3;
  constexpr size_t maxTriplesPerPartial = 5;
  std::vector<std::vector<TurtleTriple>> batches;
  std::vector<StringTriple> expectedTriples;
  for (size_t b = 0; b < numBatches; ++b) {
    auto& batch = batches.emplace_back();
    for (size_t i = 0; i < batchSize; ++i) {
      batch.push_back(makeTriple(b * batchSize + i));
      expectedTriples.push_back(expectedStringTriple(batch.back()));
    }
  }
  ql::ranges::sort(expectedTriples);

  MockIndex index;
  auto result = run(index, batches, maxTriplesPerPartial, 4);
  EXPECT_EQ(result.numTriples_, numBatches * batchSize);
  EXPECT_FALSE(result.stopRequested_);

  // Every index below the counter was written exactly once (the `MockIndex`
  // already checks that no index was written twice).
  const auto& written = index.written();
  ASSERT_EQ(written.size(), result.numPartialVocabularies_);
  size_t expectedIdx = 0;
  for (const auto& [idx, vocab] : written) {
    EXPECT_EQ(idx, expectedIdx++);
    // A chain writes as soon as it has at least `maxTriplesPerPartial`
    // triples, checked after each complete batch, so no partial vocabulary is
    // empty or larger than that threshold plus one batch.
    EXPECT_GE(vocab.triples_.size(), 1u);
    EXPECT_LE(vocab.triples_.size(), maxTriplesPerPartial + batchSize - 1);
    auto expectedWords = specialWords();
    for (const auto& triple : vocab.triples_) {
      expectedWords.insert(triple.begin(), triple.end());
    }
    EXPECT_EQ(vocab.words_, expectedWords);
  }
  // Together, the partial vocabularies contain each input triple exactly once.
  EXPECT_EQ(index.allTriplesSorted(), expectedTriples);
}

// _____________________________________________________________________________
TEST(PartialVocabularyBuilder, emptyInput) {
  MockIndex index;
  auto result = run(index, {}, 3, 3);
  EXPECT_EQ(result.numPartialVocabularies_, 0u);
  EXPECT_EQ(result.numTriples_, 0u);
  EXPECT_FALSE(result.stopRequested_);
  EXPECT_TRUE(index.written().empty());
}

// _____________________________________________________________________________
TEST(PartialVocabularyBuilder, languageTaggedLiteralAddsInternalTriples) {
  // A language-tagged literal gives rise to two additional internal triples
  // (see `mapTripleToIds`), which are counted in `numTriples_` and written to
  // the same partial vocabulary.
  TurtleTriple triple{iri("<s>"), iri("<p>"),
                      tripleComponentLiteral("\"hello\"", "@en")};
  MockIndex index;
  auto result = run(index, {{triple}}, 1, 1);
  EXPECT_EQ(result.numPartialVocabularies_, 1u);
  EXPECT_EQ(result.numTriples_, 3u);

  auto graph = std::string{DEFAULT_GRAPH_IRI};
  auto langTag = str(
      TripleComponent{ad_utility::triple_component::Iri::fromLangtag("en")});
  auto langTaggedPredicate =
      str(TripleComponent{triple.predicate_.getIri().withLanguageTag("en")});
  auto langPredicate = str(TripleComponent{iri(LANGUAGE_PREDICATE)});
  StringTriple expectedOriginal{str(triple.subject_), str(triple.predicate_),
                                str(triple.object_), graph};
  StringTriple expectedLangTagged{str(triple.subject_), langTaggedPredicate,
                                  str(triple.object_), graph};
  StringTriple expectedLangPredicate{str(triple.object_), langPredicate,
                                     langTag, graph};
  ASSERT_EQ(index.written().size(), 1u);
  EXPECT_THAT(index.written().at(0).triples_,
              ::testing::ElementsAre(expectedOriginal, expectedLangTagged,
                                     expectedLangPredicate));
}

// _____________________________________________________________________________
TEST(PartialVocabularyBuilder, parserErrorIsPropagated) {
  // The call for the fifth batch fails. The error is rethrown by
  // `runTaskChains`, and the partial vocabularies that were written before the
  // failure are still consistent.
  std::vector<std::vector<TurtleTriple>> batches;
  for (size_t b = 0; b < 10; ++b) {
    batches.push_back({makeTriple(2 * b), makeTriple(2 * b + 1)});
  }
  MockIndex index;
  std::optional<RunResult> result;
  AD_EXPECT_THROW_WITH_MESSAGE(result = run(index, batches, 3, 3, 4),
                               HasSubstr("parse error"));
  EXPECT_FALSE(result.has_value());
  // At most the four batches before the failing one were delivered.
  EXPECT_LE(index.allTriplesSorted().size(), 8u);
}

// _____________________________________________________________________________
TEST(PartialVocabularyBuilder, errorFromWithinHandlerIsPropagated) {
  // An exception that is thrown from within a step of a task chain (here by
  // `writePartialVocabulary`) is rethrown by `runTaskChains`.
  std::vector<std::vector<TurtleTriple>> batches;
  for (size_t b = 0; b < 5; ++b) {
    batches.push_back({makeTriple(b)});
  }
  MockIndex index{true};
  AD_EXPECT_THROW_WITH_MESSAGE(run(index, batches, 1, 2),
                               HasSubstr("write error"));
  EXPECT_TRUE(index.written().empty());
}

// _____________________________________________________________________________
TEST(PartialVocabularyBuilder, memoryBudgetDeterminesPartialVocabularies) {
  // Twenty batches with a single triple each, whose strings take about 300 KB
  // each. The strings are stored in blocks of `MonotonicBuffer::BLOCK_SIZE`
  // (1 MiB), three of them per block. With a budget of 3 MB, a single chain
  // therefore writes a partial vocabulary as soon as the third block has been
  // allocated, i.e. after every seventh triple.
  constexpr size_t numBatches = 20;
  constexpr size_t stringSize = 300'000;
  static_assert(MonotonicBuffer::BLOCK_SIZE == 1 << 20);
  std::vector<std::vector<TurtleTriple>> batches;
  std::vector<StringTriple> expectedTriples;
  for (size_t b = 0; b < numBatches; ++b) {
    batches.push_back({makeTriple(b, stringSize)});
    expectedTriples.push_back(expectedStringTriple(batches.back().front()));
  }
  ql::ranges::sort(expectedTriples);

  MockIndex index;
  auto result =
      run(index, batches,
          FirstPassMemoryBudget{ad_utility::MemorySize::megabytes(3)}, 1);
  EXPECT_EQ(result.numTriples_, numBatches);
  EXPECT_GE(result.numPartialVocabularies_, 2u);
  EXPECT_LE(result.numPartialVocabularies_, 4u);
  const auto& written = index.written();
  ASSERT_EQ(written.size(), result.numPartialVocabularies_);
  for (const auto& [idx, vocab] : written) {
    // No partial vocabulary has more strings than fit into 3 MB, and all but
    // the last one are (almost) full.
    EXPECT_LE(vocab.triples_.size() * stringSize, 3'000'000u);
    if (idx + 1 < written.size()) {
      EXPECT_GE(vocab.triples_.size() * stringSize, 2'000'000u);
    }
  }
  EXPECT_EQ(index.allTriplesSorted(), expectedTriples);
}

// _____________________________________________________________________________
TEST(PartialVocabularyBuilder, tinyMemoryBudgetStillTakesOneBatchEach) {
  // Even with a budget of a single byte, every partial vocabulary contains at
  // least one complete batch.
  std::vector<std::vector<TurtleTriple>> batches{
      {makeTriple(0), makeTriple(1)}, {makeTriple(2)}, {makeTriple(3)}};
  MockIndex index;
  auto result = run(index, batches,
                    FirstPassMemoryBudget{ad_utility::MemorySize::bytes(1)}, 1);
  EXPECT_EQ(result.numPartialVocabularies_, 3u);
  EXPECT_EQ(result.numTriples_, 4u);
  const auto& written = index.written();
  ASSERT_EQ(written.size(), 3u);
  EXPECT_EQ(written.at(0).triples_.size(), 2u);
  EXPECT_EQ(written.at(1).triples_.size(), 1u);
  EXPECT_EQ(written.at(2).triples_.size(), 1u);
}

// _____________________________________________________________________________
TEST(PartialVocabularyBuilder, largeMemoryBudgetYieldsOnePartialVocabulary) {
  // With a budget that is never reached, a single chain writes exactly one
  // partial vocabulary at the end of its input.
  std::vector<std::vector<TurtleTriple>> batches;
  for (size_t b = 0; b < 50; ++b) {
    batches.push_back({makeTriple(2 * b), makeTriple(2 * b + 1)});
  }
  MockIndex index;
  auto result =
      run(index, batches,
          FirstPassMemoryBudget{ad_utility::MemorySize::gigabytes(1)}, 1);
  EXPECT_EQ(result.numPartialVocabularies_, 1u);
  EXPECT_EQ(result.numTriples_, 100u);
}

// _____________________________________________________________________________
TEST(PartialVocabularyBuilder, computeMemoryPerTaskChain) {
  using ad_utility::MemorySize;
  // The parser gets `NUM_PARSER_BUFFERS_PER_THREAD` buffers per thread, the
  // rest of the limit is divided evenly among the threads.
  {
    auto [cleanup, logStream] = setGlobalLoggingStreamToStringStream();
    auto result = computeMemoryPerTaskChain(MemorySize::gigabytes(10),
                                            MemorySize::megabytes(10), 4);
    EXPECT_EQ(result.getBytes(), (MemorySize::gigabytes(10).getBytes() -
                                  4 * NUM_PARSER_BUFFERS_PER_THREAD *
                                      MemorySize::megabytes(10).getBytes()) /
                                     4);
    EXPECT_THAT(logStream.str(), ::testing::Not(HasSubstr("too small")));
  }

  // A limit that is too small for the given number of threads yields the
  // minimum and a warning: for 100 MB, the parser alone (960 MB for 32
  // threads) exceeds the limit, for 1 GB the remaining 40 MB are not enough
  // for 32 threads.
  for (auto limit : {MemorySize::megabytes(100), MemorySize::gigabytes(1)}) {
    auto [cleanup, logStream] = setGlobalLoggingStreamToStringStream();
    auto result =
        computeMemoryPerTaskChain(limit, MemorySize::megabytes(10), 32);
    EXPECT_EQ(result, MIN_MEMORY_PER_TASK_CHAIN);
    EXPECT_THAT(logStream.str(), HasSubstr("too small"));
    EXPECT_THAT(logStream.str(), HasSubstr("32 threads"));
  }
}

// _____________________________________________________________________________
TEST(PartialVocabularyBuilder, itemMapAndBufferCountsItsMemory) {
  ItemMapAndBuffer items;
  EXPECT_EQ(items.numBytesAllocated(), 0u);
  // A single string allocates a whole block of the buffer, and the hash map
  // allocates its backing array.
  auto word = items.buffer_.addString("hello");
  items.map_.try_emplace(word, PartialVocabIndexWithExternalFlag{0, false});
  EXPECT_GE(items.numBytesAllocated(), MonotonicBuffer::BLOCK_SIZE);
  // A string that is larger than a block gets a block of its own.
  std::string large(3 * MonotonicBuffer::BLOCK_SIZE, 'a');
  EXPECT_EQ(items.buffer_.addString(large), large);
  EXPECT_GE(items.numBytesAllocated(), 4 * MonotonicBuffer::BLOCK_SIZE);
  EXPECT_LT(items.numBytesAllocated(), 5 * MonotonicBuffer::BLOCK_SIZE);
  // The strings are deallocated by `clear`, but the hash map keeps its memory.
  items.clear();
  EXPECT_GT(items.numBytesAllocated(), 0u);
  EXPECT_LT(items.numBytesAllocated(), MonotonicBuffer::BLOCK_SIZE);
  EXPECT_TRUE(items.map_.empty());
}
