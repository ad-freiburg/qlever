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
#include <fcntl.h>
#include <gmock/gmock.h>
#include <unistd.h>

#include "../../util/GTestHelpers.h"
#include "../../util/MmapVectorLegacyFormat.h"
#include "../../util/PageCacheReadTestHelpers.h"
#include "../../util/RuntimeParametersTestHelpers.h"
#include "./VocabularyTestHelpers.h"
#include "backports/algorithm.h"
#include "global/Constants.h"
#include "global/RuntimeParameters.h"
#include "index/vocabulary/VocabularyOnDisk.h"
#include "util/File.h"
#include "util/Forward.h"
#include "util/MmapVector.h"

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
  // words.size()).
  auto createVocabulary(const std::vector<std::string>& words) {
    createVocabularyImpl(words);
    VocabularyOnDisk vocabulary;
    vocabulary.open(vocabFilename_);
    return vocabulary;
  }
};

// Owns a `VocabularyOnDisk` together with the `VocabularyCreator` that manages
// its backing file, so the file lives as long as the vocabulary reading from
// it.
class VocabularyOnDiskHandle {
 public:
  VocabularyOnDiskHandle(std::string filename,
                         const std::vector<std::string>& words)
      : creator_{std::move(filename)},
        vocabulary_{creator_.createVocabulary(words)} {}

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

VocabularyOnDiskHandle createVocabularyFromWords(
    const std::vector<std::string>& words) {
  return VocabularyOnDiskHandle{absl::StrCat(gtestCurrentTestName(), ".dat"),
                                words};
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

// Drop the pages of both files of the vocabulary created by
// `createExampleVocabulary` from the page cache, so that the reads of the
// page-cache fast path (`preadv2(RWF_NOWAIT)`) miss with `EAGAIN` and go
// through the batch manager instead. Best effort: on file systems that ignore
// `POSIX_FADV_DONTNEED` (e.g. tmpfs) or without `posix_fadvise` the pages stay
// cached, and the tests below then check the hit path only.
void evictExampleVocabularyFromPageCache() {
#ifdef POSIX_FADV_DONTNEED
  auto filename = absl::StrCat(gtestCurrentTestName(), ".dat");
  for (const auto& file : {filename, absl::StrCat(filename, ".offsets")}) {
    int fd = ::open(file.c_str(), O_RDONLY);
    ASSERT_GE(fd, 0) << file;
    // Only clean pages can be dropped.
    ::fdatasync(fd);
    ::posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);
    ::close(fd);
  }
#endif
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

// With `vocabulary-iouring-page-cache-fast-path`, the words and offsets that
// are in the page cache are read before the batch manager sees the rest. The
// result must be byte-identical to the result without the fast path, for runs
// of consecutive indices as well as for reordered and duplicated indices.
TEST(VocabularyOnDisk, LookupBatchPageCacheFastPathIsByteIdentical) {
  auto vocab = createExampleVocabulary();
  std::array<size_t, 13> indices{0, 1, 2, 3, 4, 2, 0, 3, 1, 1, 4, 0, 3};
  // The fast path is on by default; restore the default after the test.
  absl::Cleanup resetParameter{[]() {
    setRuntimeParameter<
        &RuntimeParameters::vocabularyIouringPageCacheFastPath_>(true);
  }};
  setRuntimeParameter<&RuntimeParameters::vocabularyIouringPageCacheFastPath_>(
      false);
  auto withoutFastPath = vocab->lookupBatch(indices);
  setRuntimeParameter<&RuntimeParameters::vocabularyIouringPageCacheFastPath_>(
      true);
  auto withFastPath = vocab->lookupBatch(indices);
  EXPECT_THAT(withFastPath, ::testing::ElementsAreArray(withoutFastPath));
  vocabulary_test::assertLookupResultMatchesVocabularyAtIndices(
      *vocab, withFastPath, indices);
}

// With the opt-in adaptive io_uring batch sizing enabled via the runtime
// parameters (and small bounds, so early flushes and forced submits both
// occur), `open` configures the vocabulary's batch managers with a controller
// and batched lookups still return the same words as individual lookups.
TEST(VocabularyOnDisk, LookupBatchWithAdaptiveBatchSizing) {
  auto cleanupEnabled = setRuntimeParameterForTest<
      &RuntimeParameters::ioUringAdaptiveBatchEnabled_>(true);
  auto cleanupMin = setRuntimeParameterForTest<
      &RuntimeParameters::ioUringAdaptiveBatchMinSize_>(size_t{1});
  auto cleanupMax = setRuntimeParameterForTest<
      &RuntimeParameters::ioUringAdaptiveBatchMaxSize_>(size_t{4});
  std::vector<std::string> words;
  for (size_t i = 0; i < 100; ++i) {
    words.push_back(absl::StrCat("word", i));
  }
  auto vocab = createVocabularyFromWords(words);
  std::vector<size_t> indices;
  for (size_t i = 0; i < 300; ++i) {
    indices.push_back((i * 37) % words.size());
  }
  auto result = vocab->lookupBatch(indices);
  vocabulary_test::assertLookupResultMatchesVocabularyAtIndices(*vocab, result,
                                                                indices);
}

// _____________________________________________________________________________
// With the fast path, the offsets and words that miss the page cache are
// submitted to the batch manager: `beginLookup` submits the offset pairs of the
// missed runs without waiting, `finishLookup` waits for them and then reads the
// missed words. The result must be the same as for cached files, for runs of
// consecutive indices as well as for reordered and duplicated indices.
TEST(VocabularyOnDisk, PageCacheFastPathMissesGoThroughTheManager) {
  auto vocab = createExampleVocabulary();
  ASSERT_TRUE(getRuntimeParameter<
              &RuntimeParameters::vocabularyIouringPageCacheFastPath_>());
  std::array<size_t, 9> indices{0, 1, 2, 4, 3, 3, 1, 2, 0};
  evictExampleVocabularyFromPageCache();
  auto result = vocab->lookupBatch(indices);
  vocabulary_test::assertLookupResultMatchesVocabularyAtIndices(*vocab, result,
                                                                indices);
  // Two lookups in flight at the same time (as in the depth-2 pipeline of the
  // CONSTRUCT export), finished in submission order.
  evictExampleVocabularyFromPageCache();
  auto first = vocab->beginLookup(indices);
  auto second = vocab->beginLookup(ql::span<const size_t>{indices}.subspan(3));
  auto firstResult = vocab->finishLookup(std::move(first));
  auto secondResult = vocab->finishLookup(std::move(second));
  vocabulary_test::assertLookupResultMatchesVocabularyAtIndices(
      *vocab, firstResult, indices);
  vocabulary_test::assertLookupResultMatchesVocabularyAtIndices(
      *vocab, secondResult, ql::span<const size_t>{indices}.subspan(3));
}

// _____________________________________________________________________________
// Every other page-cache read finds nothing cached.
int64_t everyOtherReadCached(int fd, const ::iovec* iov, int iovcnt,
                             int64_t offset) {
  static size_t numCalls = 0;
  if (numCalls++ % 2 == 1) {
    return pageCacheReadTestHelpers::nothingCached(fd, iov, iovcnt, offset);
  }
  return ad_utility::detail::systemPageCacheRead(fd, iov, iovcnt, offset);
}

// With the fast path on, reads that the page-cache read does not serve (none,
// some or all of them, and after the fast path was found unsupported) go
// through the batch manager; the result always matches `operator[]`.
TEST(VocabularyOnDisk, LookupBatchPageCacheMissesGoThroughTheManager) {
  if (!ad_utility::pageCacheFastPathIsSupported()) {
    GTEST_SKIP() << "preadv2(RWF_NOWAIT) is not available";
  }
  using pageCacheReadTestHelpers::ScopedPageCacheRead;
  auto vocab = createExampleVocabulary();
  // Runs of consecutive indices, reordered and duplicated indices.
  std::array<size_t, 13> indices{0, 1, 2, 3, 4, 2, 0, 3, 1, 1, 4, 0, 3};
  auto check = [&]() {
    vocabulary_test::assertLookupResultMatchesVocabularyAtIndices(
        *vocab, vocab->lookupBatch(indices), indices);
  };
  {
    ScopedPageCacheRead inject{&pageCacheReadTestHelpers::nothingCached};
    check();
  }
  {
    ScopedPageCacheRead inject{&everyOtherReadCached};
    check();
  }
  {
    ScopedPageCacheRead inject{&pageCacheReadTestHelpers::notSupported};
    check();
    EXPECT_FALSE(ad_utility::pageCacheFastPathIsSupported());
    // The flag is still set, but the fast path is not used any more.
    check();
  }
  EXPECT_TRUE(ad_utility::pageCacheFastPathIsSupported());
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
  // `beginLookup` throws before it submits any read. Destroying that handle
  // must neither wait on an unsubmitted batch nor lose the pooled I/O manager,
  // so a later lookup still works.
  EXPECT_ANY_THROW(vocab->beginLookup(indices));
  std::array<size_t, 3> validIndices{4, 0, 2};
  auto result = vocab->lookupBatch(validIndices);
  vocabulary_test::assertLookupResultMatchesVocabularyAtIndices(*vocab, result,
                                                                validIndices);
}

// A handle whose offset reads are in flight may be destroyed without calling
// `finishLookup`: its destructor drains the reads and returns the pooled I/O
// manager, so later lookups still work. `finishLookup` rejects a null handle.
TEST(VocabularyOnDisk, DroppedInFlightHandleReturnsManager) {
  auto vocab = createExampleVocabulary();
  std::array<size_t, 3> indices{4, 0, 2};
  // The fast path is on by default; restore the default after the test.
  absl::Cleanup resetParameter{[]() {
    setRuntimeParameter<
        &RuntimeParameters::vocabularyIouringPageCacheFastPath_>(true);
  }};
  // Drop more handles than the pool has managers. If a dropped handle kept its
  // manager, the pool would have to create new ones. Without the fast
  // path, every handle has a submitted offset batch that its destructor must
  // drain; with the fast path and evicted files, the missed offsets are
  // submitted the same way.
  for (bool fastPath : {false, true}) {
    setRuntimeParameter<
        &RuntimeParameters::vocabularyIouringPageCacheFastPath_>(fastPath);
    for (size_t round = 0; round < 2 * NUM_VOCAB_BATCH_IO_MANAGERS; ++round) {
      if (fastPath) {
        evictExampleVocabularyFromPageCache();
      }
      auto handle = vocab->beginLookup(indices);
    }
  }
  auto result = vocab->finishLookup(vocab->beginLookup(indices));
  vocabulary_test::assertLookupResultMatchesVocabularyAtIndices(*vocab, result,
                                                                indices);
  EXPECT_EQ(vocab->numIoManagers(), NUM_VOCAB_BATCH_IO_MANAGERS);
  EXPECT_ANY_THROW(vocab->finishLookup(nullptr));
}

// Before `open`, a vocabulary has no I/O manager pool.
TEST(VocabularyOnDisk, NoIoManagersBeforeOpen) {
  VocabularyOnDisk vocab;
  EXPECT_EQ(vocab.numIoManagers(), 0u);
}

// More lookups than the pool has managers may be in flight at once (the
// depth-2 lookup and the fibers of one CONSTRUCT batch hold several managers
// on one thread): `beginLookup` then creates a new manager instead of blocking
// on the empty pool, which would deadlock this thread. The extra managers stay
// in the pool and are reused by later lookups.
TEST(VocabularyOnDisk, MoreInFlightLookupsThanPooledManagers) {
  auto vocab = createExampleVocabulary();
  EXPECT_EQ(vocab->numIoManagers(), NUM_VOCAB_BATCH_IO_MANAGERS);
  const size_t numHandles = 2 * NUM_VOCAB_BATCH_IO_MANAGERS + 1;
  std::vector<std::vector<size_t>> indices;
  std::vector<std::unique_ptr<VocabLookupHandleBase>> handles;
  for (size_t i = 0; i < numHandles; ++i) {
    indices.push_back({i % vocab->size(), (i + 2) % vocab->size()});
    handles.push_back(vocab->beginLookup(indices.back()));
  }
  EXPECT_EQ(vocab->numIoManagers(), numHandles);
  // Finish in reverse order, so the completions of the first handles are
  // consumed last.
  for (size_t i = numHandles; i-- > 0;) {
    auto result = vocab->finishLookup(std::move(handles[i]));
    vocabulary_test::assertLookupResultMatchesVocabularyAtIndices(
        *vocab, result, indices[i]);
  }
  // All managers are back in the pool; later lookups create no new ones.
  for (size_t i = 0; i < numHandles; ++i) {
    auto result = vocab->lookupBatch(indices[i]);
    vocabulary_test::assertLookupResultMatchesVocabularyAtIndices(
        *vocab, result, indices[i]);
  }
  EXPECT_EQ(vocab->numIoManagers(), numHandles);
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
