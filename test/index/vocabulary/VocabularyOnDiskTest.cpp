// Copyright 2022 - 2026 The QLever Authors, in particular:
//
// 2022-2026 Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de), UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>

#include <atomic>
#include <chrono>
#include <exception>
#include <future>
#include <thread>

#include "../../util/GTestHelpers.h"
#include "../../util/MmapVectorLegacyFormat.h"
#include "./VocabularyTestHelpers.h"
#include "backports/algorithm.h"
#include "global/Constants.h"
#include "index/vocabulary/VocabularyOnDisk.h"
#include "util/File.h"
#include "util/Forward.h"
#include "util/MmapVector.h"
#include "util/Random.h"
#include "util/jthread.h"

namespace {
using namespace vocabulary_test;

// Store a `VocabularyOnDisk` and read it back from file. For each instance of
// `VocabularyCreator` that exists at the same time, a different filename has to
// be chosen.
class VocabularyCreator {
 private:
  std::string vocabFilename_;

 public:
  explicit VocabularyCreator(std::string filename)
      : vocabFilename_{std::move(filename)} {
    deleteVocabularyFiles<VocabularyOnDisk>(vocabFilename_);
  }
  // Move-only: a moved-from creator has an empty filename and deletes nothing.
  VocabularyCreator(VocabularyCreator&& other) noexcept
      : vocabFilename_{std::exchange(other.vocabFilename_, {})} {}
  VocabularyCreator& operator=(VocabularyCreator&&) =
      delete;  // not needed (TODO: why?)
  VocabularyCreator(const VocabularyCreator&) = delete;
  VocabularyCreator& operator=(const VocabularyCreator&) = delete;

  ~VocabularyCreator() {
    if (!vocabFilename_.empty()) {
      deleteVocabularyFiles<VocabularyOnDisk>(vocabFilename_);
    }
  }

  // Create and return a `VocabularyOnDisk` from words.
  void createVocabularyImpl(const std::vector<std::string>& words) {
    auto writer = VocabularyOnDisk::WordWriter(vocabFilename_);
    for (const auto& [i, word] : ::ranges::views::enumerate(words)) {
      EXPECT_EQ(writer(word, false), static_cast<uint64_t>(i));
    }
    writer.readableName() = "blubb";
    EXPECT_EQ(writer.readableName(), "blubb");
    static std::atomic<unsigned> doFinish = 0;
    // In some tests, call `finish` explicitly, in others let the destructor
    // handle this.
    if (doFinish.fetch_add(1) % 2 == 0) {
      writer.finish();
    }
  }

  // Create and return a `VocabularyOnDisk` from words. The ids will be [0, ..
  // words.size()). Keep the default `preferIoUring == true` to use `io_uring`
  // where available, or pass `false` to force the synchronous `pread` fallback
  // backend, which covers that path in the concurrency tests.
  auto createVocabulary(const std::vector<std::string>& words,
                        bool preferIoUring = true) {
    createVocabularyImpl(words);
    VocabularyOnDisk vocabulary;
    vocabulary.open(vocabFilename_, preferIoUring);
    return vocabulary;
  }
};

// Owns a `VocabularyOnDisk` together with the `VocabularyCreator` that manages
// its backing file, so the file lives as long as the vocabulary reading from
// it.
class VocabularyOnDiskHandle {
 public:
  VocabularyOnDiskHandle(std::string filename,
                         const std::vector<std::string>& words,
                         bool preferIoUring = true)
      : creator_{std::move(filename)},
        vocabulary_{creator_.createVocabulary(words, preferIoUring)} {}

  // Non-copyable/movable: a copy would give two `creator_`s the same file, so
  // both destructors would unlink it (double free).
  VocabularyOnDiskHandle(const VocabularyOnDiskHandle&) = delete;
  VocabularyOnDiskHandle& operator=(const VocabularyOnDiskHandle&) = delete;
  VocabularyOnDiskHandle(VocabularyOnDiskHandle&&) = delete;
  VocabularyOnDiskHandle& operator=(VocabularyOnDiskHandle&&) = delete;

 private:
  // `vocabulary_` is declared after `creator_`, because the `vocabulary_`
  // should be destroyed before the `creator_`: the `vocabulary_` must be torn
  // down before the `creator_` unlinks the file.
  VocabularyCreator creator_;
  VocabularyOnDisk vocabulary_;

 public:
  // Access the underlying vocabulary transparently, so call sites can treat
  // the handle like the `VocabularyOnDisk` it wraps.
  VocabularyOnDisk& operator*() { return vocabulary_; }
  VocabularyOnDisk* operator->() { return &vocabulary_; }
};

// Return a handle that owns a vocabulary of `words` together with its backing
// file, both of which live until the handle is destroyed. Use a different file
// per backend (`.dat` and `.sync.dat`), so one test can hold both.
VocabularyOnDiskHandle createVocabularyFromWords(
    const std::vector<std::string>& words, bool preferIoUring = true) {
  return VocabularyOnDiskHandle{
      absl::StrCat(gtestCurrentTestName(),
                   preferIoUring ? ".dat" : ".sync.dat"),
      words, preferIoUring};
}

auto createVocabulary() {
  return [c = VocabularyCreator{absl::StrCat(gtestCurrentTestName(), ".dat")}](
             auto&&... args) mutable {
    return c.createVocabulary(AD_FWD(args)...);
  };
}

VocabularyOnDiskHandle createExampleVocabulary() {
  return createVocabularyFromWords({"alpha", "delta", "beta", "42", "gamma"});
}

// _____________________________________________________________________________
// Return `numWords` deterministic words of varying lengths, the first of which
// is the empty word, for the concurrency tests below.
std::vector<std::string> makeConcurrentTestWords(size_t numWords) {
  AD_CONTRACT_CHECK(numWords > 0);
  std::vector<std::string> words{std::string{}};
  ql::ranges::copy(
      ql::views::iota(size_t{1}, numWords) | ql::views::transform([](size_t i) {
        return absl::StrCat("word", i, "_", std::string(i % 17, 'x'));
      }),
      std::back_inserter(words));
  return words;
}

// _____________________________________________________________________________
// Return `numBatches` batches of `batchSize` vocabulary indices each, drawn
// from `[0, vocabSize)` with a fixed seed, so the batches are the same in every
// run. Generate them once on the calling thread; workers only read them.
std::vector<std::vector<size_t>> makeConcurrentTestBatches(size_t vocabSize,
                                                           size_t numBatches,
                                                           size_t batchSize) {
  AD_CONTRACT_CHECK(vocabSize > 0);
  ad_utility::SlowRandomIntGenerator<size_t> randomIndex{
      0, vocabSize - 1, ad_utility::RandomSeed::make(42)};
  std::vector<std::vector<size_t>> batches;
  batches.reserve(numBatches);
  for ([[maybe_unused]] size_t batch : ql::views::iota(size_t{0}, numBatches)) {
    auto& indices = batches.emplace_back(batchSize);
    ql::ranges::generate(indices, [&randomIndex] { return randomIndex(); });
  }
  return batches;
}

// _____________________________________________________________________________
// Return one snapshot per batch in `batches`, taken via serial `lookupBatch`
// calls on the calling thread, as the reference that the concurrent runs below
// must reproduce byte-identically.
std::vector<std::vector<std::string>> snapshotBatchesSerial(
    const VocabularyOnDisk& vocab,
    const std::vector<std::vector<size_t>>& batches) {
  std::vector<std::vector<std::string>> snapshots;
  snapshots.reserve(batches.size());
  for (const auto& batch : batches) {
    auto result = vocab.lookupBatch(batch);
    snapshots.emplace_back(result->begin(), result->end());
  }
  return snapshots;
}

// Hold the per-thread snapshots of `snapshotBatchesConcurrent` together with
// the error message of each worker (empty if the worker succeeded).
struct ConcurrentSnapshots {
  std::vector<std::vector<std::vector<std::string>>> snapshots_;
  std::vector<std::string> errors_;
};

// _____________________________________________________________________________
// Run `lookupBatch` concurrently, where thread `t` looks up
// `perThreadBatches[t]` in order and snapshots each result. Record worker
// exceptions in `errors_` (each worker writes only its own entry), so they
// surface as test failures instead of `std::terminate`.
ConcurrentSnapshots snapshotBatchesConcurrent(
    const VocabularyOnDisk& vocab,
    const std::vector<std::vector<std::vector<size_t>>>& perThreadBatches) {
  const size_t numThreads = perThreadBatches.size();
  ConcurrentSnapshots result;
  result.snapshots_.resize(numThreads);
  result.errors_.resize(numThreads);
  {
    // Use `JThread`, which joins on destruction, also if starting a later
    // thread throws.
    std::vector<ad_utility::JThread> threads;
    threads.reserve(numThreads);
    for (size_t t : ql::views::iota(size_t{0}, numThreads)) {
      threads.emplace_back([&vocab, &batches = perThreadBatches[t],
                            &threadSnapshots = result.snapshots_[t],
                            &error = result.errors_[t]] {
        try {
          threadSnapshots.reserve(batches.size());
          for (const auto& batch : batches) {
            auto words = vocab.lookupBatch(batch);
            threadSnapshots.emplace_back(words->begin(), words->end());
          }
        } catch (const std::exception& e) {
          error = e.what();
        } catch (...) {
          error = "unknown exception";
        }
      });
    }
  }
  return result;
}

// _____________________________________________________________________________
// Assert that no worker failed and that thread `t`'s `k`-th snapshot equals
// `expected[(t * rotationPerThread + k) % expected.size()]`, i.e. that every
// thread reproduced the serial reference for its (possibly rotated) batch
// order.
void expectConcurrentSnapshotsMatchSerial(
    const ConcurrentSnapshots& concurrent,
    const std::vector<std::vector<std::string>>& expected,
    size_t rotationPerThread = 0) {
  for (const auto& error : concurrent.errors_) {
    EXPECT_TRUE(error.empty()) << error;
  }
  for (const auto& [t, threadSnapshots] :
       ::ranges::views::enumerate(concurrent.snapshots_)) {
    ASSERT_EQ(threadSnapshots.size(), expected.size());
    for (const auto& [k, snapshot] :
         ::ranges::views::enumerate(threadSnapshots)) {
      EXPECT_THAT(snapshot,
                  ::testing::ElementsAreArray(
                      expected[(static_cast<size_t>(t) * rotationPerThread +
                                static_cast<size_t>(k)) %
                               expected.size()]))
          << "thread " << t << " batch " << k;
    }
  }
}

// Create a `VocabularyOnDisk` from `words` and assert that `scanAll` yields
// exactly those words in order: both as bare words and as `IndexAndWord`s with
// contiguous indices `0, 1, 2, ...` (also across batch boundaries).
void expectScanAllYields(const std::vector<std::string>& words) {
  VocabularyCreator creator{gtestCurrentTestName()};
  auto vocabulary = creator.createVocabulary(words);

  EXPECT_THAT(scanAllToVector(vocabulary.scanAll()),
              ::testing::ElementsAreArray(words));

  auto indexAndWords = scanAllToIndexAndWordVector(vocabulary.scanAll());
  ASSERT_EQ(indexAndWords.size(), words.size());
  for (size_t i = 0; i < words.size(); ++i) {
    EXPECT_EQ(indexAndWords[i].first, i) << "at index " << i;
    EXPECT_EQ(indexAndWords[i].second, words[i]) << "at index " << i;
  }
}

}  // namespace

TEST(VocabularyOnDisk, LowerUpperBoundStdLess) {
  testUpperAndLowerBoundWithStdLess(createVocabulary());
}

TEST(VocabularyOnDisk, LowerUpperBoundNumeric) {
  testUpperAndLowerBoundWithNumericComparator(createVocabulary());
}

TEST(VocabularyOnDisk, AccessOperator) {
  testAccessOperatorForUnorderedVocabulary(createVocabulary());
}

TEST(VocabularyOnDisk, AccessOperatorWithNonContiguousIds) {
  std::vector<std::string> words{"game",  "4",      "nobody", "33",
                                 "alpha", "\n\1\t", "222",    "1111"};
  std::vector<uint64_t> ids{2, 4, 8, 16, 17, 19, 42, 42 * 42 + 7};
  testAccessOperatorForUnorderedVocabulary(createVocabulary());
}

TEST(VocabularyOnDisk, EmptyVocabulary) {
  testEmptyVocabulary(createVocabulary());
}

// Older versions of QLever stored the offsets file as an
// `ad_utility::MmapVector<uint64_t>`. Such a file rounds its capacity up to a
// multiple of the page size, so the offsets array is followed by a (large)
// region of unused capacity before the metadata trailer at the very end. This
// test writes the offsets file in exactly that legacy format and makes sure
// that the current `VocabularyOnDisk` still reads it back correctly.
TEST(VocabularyOnDisk, ReadLegacyMmapVectorOffsetsFormat) {
  std::string vocabFilename = "vocabularyOnDisk.legacyMmapFormat";
  std::string offsetsFilename = vocabFilename + ".offsets";
  auto cleanup = makeVocabFileCleanup<VocabularyOnDisk>(vocabFilename);

  const std::array<std::string_view, 7> words{
      "alpha",
      "bravo",
      "charlie",
      "",
      "a longer word with spaces and \1\n\t control chars",
      "delta",
      "z"};

  // Write the words file (the plain concatenation of all words) and collect the
  // offsets: one offset per word plus a final offset marking the end of the
  // last word.
  std::vector<uint64_t> offsets;
  {
    ad_utility::File wordsFile{vocabFilename, "w"};
    uint64_t currentOffset = 0;
    for (std::string_view word : words) {
      offsets.push_back(currentOffset);
      currentOffset += wordsFile.write(word.data(), word.size());
    }
    offsets.push_back(currentOffset);
  }
  // Write the offsets file in the legacy `MmapVector<uint64_t>` on-disk layout,
  // which rounds its capacity up to a page boundary and appends the metadata
  // trailer, reproducing the legacy format with a region of unused capacity.
  ad_utility::testing::writeLegacyMmapVectorFile(offsetsFilename, offsets);

  // Sanity check that we actually exercise the "unused capacity" path: the
  // offsets file is considerably larger than the offsets plus the trailer alone
  // would require.
  ad_utility::File offsetsFile{offsetsFilename, "r"};
  EXPECT_GT(offsetsFile.sizeOfFile(),
            static_cast<off_t>((words.size() + 1) * sizeof(uint64_t)) +
                ad_utility::MmapVectorMetaData::numBytes);
  offsetsFile.close();

  VocabularyOnDisk vocabulary;
  vocabulary.open(vocabFilename);
  ASSERT_EQ(vocabulary.size(), words.size());
  for (size_t i = 0; i < words.size(); ++i) {
    EXPECT_EQ(vocabulary[i], words[i]) << "at index " << i;
  }
}

// _____________________________________________________________________________
TEST(VocabularyOnDisk, ScanAll) {
  // A basic scan over many small words (fits into a single batch).
  std::vector<std::string> words;
  for (size_t i = 0; i < 3000; ++i) {
    words.push_back(absl::StrCat("word", i, std::string(i % 7, 'x')));
  }
  expectScanAllYields(words);
}

// _____________________________________________________________________________
TEST(VocabularyOnDisk, ScanAllEmptyVocabulary) {
  VocabularyCreator creator{gtestCurrentTestName()};
  auto vocabulary = creator.createVocabulary({});
  auto range = vocabulary.scanAll();
  EXPECT_FALSE(range.get().has_value());
}

// _____________________________________________________________________________
TEST(VocabularyOnDisk, ScanAllByteLimitForcesMultipleBatches) {
  // `scanAll` caps a batch's word data at
  // `VOCABULARY_SCAN_MAX_WORD_DATA_PER_BATCH` (10 MB). Four words of 3 MB each
  // (12 MB total) therefore don't fit into a single batch: the byte limit (not
  // the word-count limit) forces a batch boundary after three words.
  constexpr size_t wordSize = 3'000'000;
  expectScanAllYields({std::string(wordSize, 'a'), std::string(wordSize, 'b'),
                       std::string(wordSize, 'c'), std::string(wordSize, 'd')});
}

// _____________________________________________________________________________
TEST(VocabularyOnDisk, ScanAllSingleWordExceedsLimit) {
  // A single word larger than `VOCABULARY_SCAN_MAX_WORD_DATA_PER_BATCH` (10 MB)
  // must still be scanned; it is returned in a batch of its own even though it
  // exceeds the limit, and the surrounding small words are unaffected.
  expectScanAllYields({"before", std::string(11'000'000, 'x'), "after"});
}

// A `lookupBatch` result must equal the individual `vocab[]` lookups for the
// same indices, including for reordered and duplicated indices.
TEST(VocabularyOnDisk, LookupBatchMatchesIndividualLookups) {
  auto vocab = createExampleVocabulary();
  std::array<size_t, 8> indices{2, 0, 3, 1, 1, 4, 0, 3};
  auto result = vocab->lookupBatch(indices);
  vocabulary_test::assertLookupResultMatchesVocabularyAtIndices(*vocab, result,
                                                                indices);
}

// An empty batch is an invalid request and must throw.
TEST(VocabularyOnDisk, LookupBatchEmptyThrows) {
  auto vocab = createExampleVocabulary();
  EXPECT_ANY_THROW(vocab->lookupBatch(ql::span<const size_t>{}));
}

// An out-of-range index in a batch must throw.
TEST(VocabularyOnDisk, LookupBatchOutOfRangeIndexThrows) {
  auto vocab = createExampleVocabulary();
  std::array<size_t, 2> indices{0, 99};
  EXPECT_ANY_THROW(vocab->lookupBatch(indices));
}

// Each batch yielded by `lookupBatchesStreamed` must equal the individual
// `vocab[]` lookups for that batch's indices, and the batches must be yielded
// in input order.
TEST(VocabularyOnDisk, LookupBatchesStreamedMatchesIndividualLookups) {
  auto vocab = createExampleVocabulary();

  std::vector<std::vector<size_t>> batches{{2, 0, 3}, {1}, {4, 0, 1}};
  // `VocabLookupInput` takes ownership of the batches, so keep a copy to
  // compare against.
  const auto expectedBatches = batches;
  auto streamed =
      vocab->lookupBatchesStreamed(VocabLookupInput{std::move(batches)});
  vocabulary_test::assertStreamedLookupMatchesVocabularyAtIndices(
      *vocab, streamed, expectedBatches);
}

// An empty input stream (no batches) is valid and must produce no results.
TEST(VocabularyOnDisk, LookupBatchesStreamedEmptyStreamYieldsNothing) {
  auto vocab = createExampleVocabulary();
  std::vector<std::vector<size_t>> noBatches;
  auto streamed =
      vocab->lookupBatchesStreamed(VocabLookupInput{std::move(noBatches)});
  EXPECT_EQ(ql::ranges::distance(streamed), 0);
}

// An out-of-range index within a streamed batch must throw when that batch is
// pulled.
TEST(VocabularyOnDisk, LookupBatchesStreamedOutOfRangeIndexThrows) {
  auto vocab = createExampleVocabulary();
  std::vector<std::vector<size_t>> batches{{0, 99}};
  auto streamed =
      vocab->lookupBatchesStreamed(VocabLookupInput{std::move(batches)});
  EXPECT_ANY_THROW({
    for ([[maybe_unused]] auto& r : streamed) {
    }
  });
}

// An empty batch within the stream is an invalid request and must throw when
// the batch is pulled (an empty input stream with no batches is still valid,
// see above).
TEST(VocabularyOnDisk, LookupBatchesStreamedEmptyBatchThrows) {
  auto vocab = createExampleVocabulary();
  std::vector<std::vector<size_t>> batches{{2, 0}, {}, {1}};
  auto streamed =
      vocab->lookupBatchesStreamed(VocabLookupInput{std::move(batches)});
  EXPECT_ANY_THROW({
    for ([[maybe_unused]] auto& r : streamed) {
    }
  });
}

// _____________________________________________________________________________
// Check that concurrent `lookupBatch` calls from many threads return
// byte-identical results to the serial path. Note that the serial reference run
// already gives the test thread an owned ring, so the workers use owned rings
// while the budget lasts and the shared pool afterwards.
TEST(VocabularyOnDisk, LookupBatchConcurrentMatchesSerial) {
  constexpr size_t numWords = 2000;
  constexpr size_t numThreads = 8;
  auto vocab = createVocabularyFromWords(makeConcurrentTestWords(numWords));
  auto batches =
      makeConcurrentTestBatches(numWords, /*numBatches=*/32, /*batchSize=*/64);
  auto expected = snapshotBatchesSerial(*vocab, batches);
  std::vector<std::vector<std::vector<size_t>>> perThreadBatches(numThreads,
                                                                 batches);
  expectConcurrentSnapshotsMatchSerial(
      snapshotBatchesConcurrent(*vocab, perThreadBatches), expected);
}

// _____________________________________________________________________________
// Check the same with the synchronous `pread` fallback backend forced, and
// with more threads than `NUM_VOCAB_BATCH_IO_MANAGERS`, so that both owned and
// pooled synchronous managers serve lookups.
TEST(VocabularyOnDisk, LookupBatchConcurrentSyncFallbackMatchesSerial) {
  constexpr size_t numWords = 1000;
  const size_t numThreads = 2 * NUM_VOCAB_BATCH_IO_MANAGERS + 1;
  auto vocab = createVocabularyFromWords(makeConcurrentTestWords(numWords),
                                         /*preferIoUring=*/false);
  auto batches =
      makeConcurrentTestBatches(numWords, /*numBatches=*/16, /*batchSize=*/32);
  auto expected = snapshotBatchesSerial(*vocab, batches);
  std::vector<std::vector<std::vector<size_t>>> perThreadBatches(numThreads,
                                                                 batches);
  expectConcurrentSnapshotsMatchSerial(
      snapshotBatchesConcurrent(*vocab, perThreadBatches), expected);
}

// _____________________________________________________________________________
// Check that the results still match the serial path with more threads than
// `NUM_VOCAB_BATCH_IO_MANAGERS`, where each thread processes the same batches
// in a different (rotated) order.
TEST(VocabularyOnDisk, LookupBatchOversubscribedThreadsMatchesSerial) {
  constexpr size_t numWords = 500;
  const size_t numThreads = 3 * NUM_VOCAB_BATCH_IO_MANAGERS + 1;
  auto vocab = createVocabularyFromWords(makeConcurrentTestWords(numWords));
  auto batches =
      makeConcurrentTestBatches(numWords, /*numBatches=*/24, /*batchSize=*/16);
  auto expected = snapshotBatchesSerial(*vocab, batches);
  // Let thread `t` look up all batches starting at batch `t`, wrapping around.
  std::vector<std::vector<std::vector<size_t>>> perThreadBatches;
  perThreadBatches.reserve(numThreads);
  for (size_t t : ql::views::iota(size_t{0}, numThreads)) {
    auto& rotated = perThreadBatches.emplace_back(batches);
    ql::ranges::rotate(rotated, rotated.begin() + t % batches.size());
  }
  expectConcurrentSnapshotsMatchSerial(
      snapshotBatchesConcurrent(*vocab, perThreadBatches), expected,
      /*rotationPerThread=*/1);
}

// _____________________________________________________________________________
// Keep more threads than `NUM_VOCAB_BATCH_IO_MANAGERS` alive after their first
// lookup, so that none of them can give back its ring. Check that exactly the
// budget of threads owns a ring, that the shared pool serves the remaining ones
// correctly, and that every ring is released when its thread exits.
TEST(VocabularyOnDisk, LookupBatchOwnedRingBudgetIsExactAndReleased) {
  for (bool preferIoUring : {true, false}) {
    const std::vector<std::string> words = makeConcurrentTestWords(300);
    auto vocab = createVocabularyFromWords(words, preferIoUring);
    const size_t numThreads = NUM_VOCAB_BATCH_IO_MANAGERS + 5;
    const std::vector<size_t> indices{0, 7, 299, 42, 7};
    std::vector<std::string> expected;
    ql::ranges::transform(indices, std::back_inserter(expected),
                          [&words](size_t i) { return words[i]; });

    std::atomic<size_t> numDone{0};
    std::promise<void> release;
    std::shared_future<void> released = release.get_future().share();
    ConcurrentSnapshots results;
    results.snapshots_.resize(numThreads);
    results.errors_.resize(numThreads);
    {
      std::vector<ad_utility::JThread> threads;
      // Release waiting workers on every exit path, so the joins in the
      // `JThread` destructors cannot block forever.
      absl::Cleanup releaseWorkers{[&release, &released] {
        if (released.wait_for(std::chrono::seconds{0}) !=
            std::future_status::ready) {
          release.set_value();
        }
      }};
      for (size_t t : ql::views::iota(size_t{0}, numThreads)) {
        // Give each worker its own copy of `released`: concurrent calls on one
        // `shared_future` object are a data race, calls on copies are not.
        threads.emplace_back([&, released,
                              &threadSnapshots = results.snapshots_[t],
                              &error = results.errors_[t]] {
          try {
            auto result = vocab->lookupBatch(indices);
            threadSnapshots.emplace_back(result->begin(), result->end());
          } catch (const std::exception& e) {
            error = e.what();
          }
          numDone.fetch_add(1);
          released.wait();
        });
      }
      while (numDone.load() < numThreads) {
        std::this_thread::yield();
      }
      // Expect a full budget, since all threads are alive and have looked up.
      EXPECT_EQ(vocab->numOwnedRingsForTesting(), NUM_VOCAB_BATCH_IO_MANAGERS);
    }
    // Expect that each thread released its ring when it exited.
    EXPECT_EQ(vocab->numOwnedRingsForTesting(), 0u);
    expectConcurrentSnapshotsMatchSerial(results, {expected});
  }
}

// _____________________________________________________________________________
// Check that reopening a vocabulary discards the rings that threads own for
// it, and that a thread that used a destroyed vocabulary can use a new one
// without leaking its old ring into the new budget.
TEST(VocabularyOnDisk, LookupBatchOwnedRingsAfterReopenAndDestruction) {
  const std::vector<std::string> words = makeConcurrentTestWords(100);
  const std::vector<size_t> indices{3, 1, 99};
  auto expectLookup = [&](const VocabularyOnDisk& vocab) {
    auto result = vocab.lookupBatch(indices);
    EXPECT_THAT(std::vector<std::string>(result->begin(), result->end()),
                ::testing::ElementsAre(words[3], words[1], words[99]));
  };
  const std::string filename = absl::StrCat(gtestCurrentTestName(), ".dat");
  {
    VocabularyCreator creator{filename};
    auto vocab = creator.createVocabulary(words);
    expectLookup(vocab);
    EXPECT_EQ(vocab.numOwnedRingsForTesting(), 1u);
    vocab.open(filename, /*preferIoUring=*/false);
    EXPECT_EQ(vocab.numOwnedRingsForTesting(), 0u);
    expectLookup(vocab);
    EXPECT_EQ(vocab.numOwnedRingsForTesting(), 1u);
  }
  // Note that the vocabulary above was destroyed while this thread still held
  // its ring.
  VocabularyCreator creator{filename};
  auto vocab = creator.createVocabulary(words);
  EXPECT_EQ(vocab.numOwnedRingsForTesting(), 0u);
  expectLookup(vocab);
  EXPECT_EQ(vocab.numOwnedRingsForTesting(), 1u);
}
