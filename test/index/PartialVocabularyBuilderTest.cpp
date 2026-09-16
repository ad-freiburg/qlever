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
#include "util/CachingMemoryResource.h"
#include "util/Conversions.h"
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
class MockParser : public AsyncRdfParserBase {
 private:
  std::vector<std::vector<TurtleTriple>> batches_;
  std::optional<size_t> failAtBatch_;
  std::mutex mutex_;
  size_t nextBatch_ = 0;
  bool failed_ = false;

 public:
  MockParser(const ql::any_io_executor& executor,
             std::vector<std::vector<TurtleTriple>> batches,
             std::optional<size_t> failAtBatch = std::nullopt)
      : AsyncRdfParserBase{executor},
        batches_{std::move(batches)},
        failAtBatch_{failAtBatch} {}

 protected:
  void asyncGetBatchImpl(Handler handler) override {
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
      result.langtag_ = std::string(
          asStringViewUnsafe(triple.object_.getLiteral().getLanguageTag()));
    }
    result.triple_ =
        Triple{std::move(triple.subject_), std::move(triple.predicate_),
               std::move(triple.object_), std::move(triple.graphIri_)};
    return result;
  }

  void writePartialVocabulary(size_t partialVocabIdx, ItemMapAndBuffer items,
                              std::vector<IdRow> localIds) {
    if (throwOnWrite_) {
      throw std::runtime_error{"write error"};
    }
    PartialVocabulary vocab;
    ad_utility::HashMap<uint64_t, std::string> wordsByLocalId;
    for (const auto& [word, idAndFlag] : items.map_) {
      vocab.words_.emplace(word);
      wordsByLocalId[idAndFlag.id()] = std::string{word};
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

// Create the triple `<s{i}> <p> <o{i}>` in the default graph.
TurtleTriple makeTriple(size_t i) {
  return TurtleTriple{iri(absl::StrCat("<s", i, ">")), iri("<p>"),
                      iri(absl::StrCat("<o", i, ">"))};
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
};
RunResult run(MockIndex& index, std::vector<std::vector<TurtleTriple>> batches,
              size_t linesPerPartial, size_t numThreads,
              std::optional<size_t> failAtBatch = std::nullopt) {
  ad_utility::CachingMemoryResource cachingMemoryResource;
  ItemAlloc itemAlloc(&cachingMemoryResource);
  TripleComponentComparator comparator;

  FirstPassSharedState<MockIndex> shared{&index, &comparator, itemAlloc,
                                         linesPerPartial};
  runTaskChains(shared, numThreads,
                [&batches, failAtBatch](const ql::any_io_executor& executor)
                    -> std::unique_ptr<AsyncRdfParserBase> {
                  return std::make_unique<MockParser>(
                      executor, std::move(batches), failAtBatch);
                });
  return {shared.nextPartialVocabIdx_.load(), shared.numTriples_.load(),
          shared.stopRequested_.load()};
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
TEST(PartialVocabularyBuilder, severalChainsRacingForBatches) {
  // Twenty batches of three triples each, five triples per partial
  // vocabulary, four task chains racing for the batches. Which chain gets
  // which batch is nondeterministic, so only order-independent properties are
  // checked.
  constexpr size_t numBatches = 20;
  constexpr size_t batchSize = 3;
  constexpr size_t linesPerPartial = 5;
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
  auto result = run(index, batches, linesPerPartial, 4);
  EXPECT_EQ(result.numTriples_, numBatches * batchSize);
  EXPECT_FALSE(result.stopRequested_);

  // Every index below the counter was written exactly once (the `MockIndex`
  // already checks that no index was written twice).
  const auto& written = index.written();
  ASSERT_EQ(written.size(), result.numPartialVocabularies_);
  size_t expectedIdx = 0;
  for (const auto& [idx, vocab] : written) {
    EXPECT_EQ(idx, expectedIdx++);
    // A chain writes as soon as it has at least `linesPerPartial` triples,
    // checked after each complete batch, so no partial vocabulary is empty or
    // larger than that threshold plus one batch.
    EXPECT_GE(vocab.triples_.size(), 1u);
    EXPECT_LE(vocab.triples_.size(), linesPerPartial + batchSize - 1);
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
  auto langTag =
      str(TripleComponent{ad_utility::convertLangtagToEntityUri("en")});
  auto langTaggedPredicate =
      str(TripleComponent{ad_utility::convertToLanguageTaggedPredicate(
          triple.predicate_.getIri(), "en")});
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
