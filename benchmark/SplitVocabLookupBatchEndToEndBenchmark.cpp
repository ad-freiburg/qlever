// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// Larger benchmark for `SplitVocabulary::lookupBatch`: write a two-way split
// vocabulary of 50,000 Wikidata-like IRIs through the actual write-to-disk
// path, read it back, and resolve a batch of 100,000 lookups with interleaved
// markers, duplicates, and reordered indices. As in
// `SplitVocabLookupBatchMicroBenchmark`, a per-word `operator[]` loop, the
// generic fallback `sequentialLookupBatch`, and `SplitVocabulary::lookupBatch`
// are compared, for `VocabularyInMemory` and for
// `CompressedVocabulary<VocabularyInMemory>` as underlying vocabularies. Every
// measurement repeats the batch until `SPLIT_VOCAB_MIN_SECONDS` (default 10; a
// positive integer of at most 3600) seconds have passed, after one untimed
// warm-up pass; the time per word is the measured time divided by the `words`
// in the measurement's metadata.

#include <absl/strings/str_cat.h>

#include <array>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "../benchmark/infrastructure/Benchmark.h"
#include "backports/StartsWithAndEndsWith.h"
#include "backports/algorithm.h"
#include "backports/filesystem.h"
#include "index/vocabulary/CompressedVocabulary.h"
#include "index/vocabulary/SplitVocabulary.h"
#include "index/vocabulary/SplitVocabularyImpl.h"
#include "index/vocabulary/VocabularyInMemory.h"
#include "index/vocabulary/VocabularyTypes.h"
#include "util/Exception.h"

namespace ad_benchmark {
namespace {

// Route words that start with `"a` to vocabulary 1 and all other words to
// vocabulary 0.
struct EndToEndSplitFunc {
  uint8_t operator()(std::string_view word) const {
    return ql::starts_with(word, "\"a");
  }
};

// Vocabulary 0 is stored under the base filename, vocabulary 1 with the suffix
// `.a`.
constexpr std::array<std::string_view, 2> endToEndFilenameSuffixes{"", ".a"};

// A split vocabulary with `Underlying` for both vocabulary 0 and vocabulary 1.
template <typename Underlying>
using EndToEndSplitVocab =
    SplitVocabulary<EndToEndSplitFunc, endToEndFilenameSuffixes, Underlying,
                    Underlying>;

constexpr size_t numWords = 50'000;
constexpr size_t batchSize = 100'000;

// Remove the directory tree `dir_` on destruction and ignore failures.
struct TempDirCleanup {
  ql::filesystem::path dir_;
  ~TempDirCleanup() {
    ql::error_code ec;
    ql::filesystem::remove_all(dir_, ec);
  }
};

// Parse a positive size from the environment variable `name`, or return
// `defaultValue` if it is not set. A value that is set but not a positive
// integer of at most `maxValue` fails a contract check instead of falling back
// to `defaultValue`.
size_t sizeFromEnvironment(const char* name, size_t defaultValue,
                           size_t maxValue) {
  const char* value = std::getenv(name);
  if (value == nullptr) {
    return defaultValue;
  }
  errno = 0;
  char* end = nullptr;
  const unsigned long parsed = std::strtoul(value, &end, 10);
  AD_CONTRACT_CHECK(end != value && *end == '\0' && errno != ERANGE);
  AD_CONTRACT_CHECK(parsed > 0 && parsed <= maxValue);
  return static_cast<size_t>(parsed);
}

// Add the measurement `name` to `group`: call `lookupOnce` (one pass over a
// batch of `wordsPerCall` words, returning the number of bytes looked up)
// again and again until at least `minSeconds` have passed. The number of
// calls, the number of words, and the mean number of bytes per call (rounded
// down) are recorded as metadata, so the time per word is the measured time
// divided by `words`.
template <typename LookupOnce>
void addTimedMeasurement(ResultGroup& group, const std::string& name,
                         double minSeconds, size_t wordsPerCall,
                         const LookupOnce& lookupOnce) {
  size_t calls = 0;
  size_t totalBytes = 0;
  auto& entry = group.addMeasurement(name, [&] {
    const auto start = std::chrono::steady_clock::now();
    do {
      totalBytes += lookupOnce();
      ++calls;
    } while (
        std::chrono::duration<double>(std::chrono::steady_clock::now() - start)
            .count() < minSeconds);
  });
  entry.metadata().addKeyValuePair("calls", calls);
  entry.metadata().addKeyValuePair("words", calls * wordsPerCall);
  entry.metadata().addKeyValuePair("bytes-per-call", totalBytes / calls);
}

// Generate deterministic Wikidata-like IRIs (as string literals), every third
// one routed to marker 1 via the `"a` prefix. Sort the words, as the
// vocabulary writers require sorted input.
std::vector<std::string> makeWords() {
  constexpr std::string_view alphabet{
      "abcdefghijklmnopqrstuvwxyz0123456789_:/.-#"};
  std::vector<std::string> words;
  words.reserve(numWords);
  for (size_t i = 0; i < numWords; ++i) {
    std::string word{i % 3 == 0 ? "\"ahttp://www.wikidata.org/entity/Q"
                                : "\"http://www.wikidata.org/entity/Q"};
    word += std::to_string(i);
    word += '/';
    for (size_t character = 0; character < 32; ++character) {
      word += alphabet[(i * 17 + character * 31) % alphabet.size()];
    }
    word += '"';
    words.push_back(std::move(word));
  }
  ql::ranges::sort(words);
  return words;
}

// Build a split vocabulary with `Underlying` vocabularies from `words` under
// `basename`, and measure the three lookup strategies on one large batch in
// non-ascending order with duplicates (mimicking the access pattern of
// batched ID-to-word resolution during query evaluation).
template <typename Underlying>
void measureUnderlying(BenchmarkResults& results, const std::string& label,
                       const std::vector<std::string>& words,
                       const ql::filesystem::path& basename,
                       double minSeconds) {
  EndToEndSplitVocab<Underlying> vocab;
  {
    const auto writerPtr = vocab.makeDiskWriterPtr(basename.string());
    for (const auto& word : words) {
      (*writerPtr)(word, false);
    }
    writerPtr->finish();
  }
  vocab.readFromFile(basename.string());

  std::vector<size_t> marked;
  for (const IndexAndWord& indexAndWord : vocab.scanAll()) {
    marked.push_back(static_cast<size_t>(indexAndWord.index_));
  }
  AD_CONTRACT_CHECK(!marked.empty());
  std::vector<size_t> batch;
  batch.reserve(batchSize);
  for (size_t i = 0; i < batchSize; ++i) {
    batch.push_back(marked[(i * 2654435761u) % marked.size()]);
  }

  // Validate the benchmark once, outside the timed region: the batched and the
  // fallback lookups must agree with `operator[]`.
  const auto checkBatch = [&](const std::vector<size_t>& batch) {
    const auto batched = vocab.lookupBatch(batch);
    const auto fallback =
        ad_utility::vocabulary::sequentialLookupBatch(vocab, batch);
    AD_CORRECTNESS_CHECK(batched.size() == batch.size());
    AD_CORRECTNESS_CHECK(fallback.size() == batch.size());
    for (size_t i = 0; i < batch.size(); ++i) {
      const std::string expected{vocab[batch[i]]};
      AD_CORRECTNESS_CHECK(batched[i] == expected);
      AD_CORRECTNESS_CHECK(fallback[i] == expected);
    }
  };
  checkBatch(batch);

  const auto sumSizes = [](const auto& result) {
    size_t totalBytes = 0;
    for (const auto& word : result) {
      totalBytes += word.size();
    }
    return totalBytes;
  };
  // Each of the following three lambdas looks up all words of `batch` once with
  // one strategy and returns the total number of bytes of the words.
  const auto runItemAt = [&] {
    size_t totalBytes = 0;
    for (size_t index : batch) {
      const std::string word{vocab[index]};
      totalBytes += word.size();
    }
    return totalBytes;
  };
  const auto runSequentialLookupBatch = [&] {
    return sumSizes(
        ad_utility::vocabulary::sequentialLookupBatch(vocab, batch));
  };
  const auto runBatched = [&] { return sumSizes(vocab.lookupBatch(batch)); };

  // Run every strategy once, untimed, to warm up caches and the allocator.
  runItemAt();
  runSequentialLookupBatch();
  runBatched();

  auto& group =
      results.addGroup(absl::StrCat(label, ": ", batchSize, " lookups into ",
                                    words.size(), " words in two markers"));
  addTimedMeasurement(group, "sequential operator[]", minSeconds, batchSize,
                      runItemAt);
  addTimedMeasurement(group, "fallback sequentialLookupBatch", minSeconds,
                      batchSize, runSequentialLookupBatch);
  addTimedMeasurement(group, "batched lookupBatch", minSeconds, batchSize,
                      runBatched);
  vocab.close();
}

class SplitVocabLookupBatchEndToEndBenchmark : public BenchmarkInterface {
 public:
  std::string name() const final {
    return "SplitVocabulary::lookupBatch (end to end)";
  }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results;
    const double minSeconds = static_cast<double>(
        sizeFromEnvironment("SPLIT_VOCAB_MIN_SECONDS", 10, 3600));
    const auto words = makeWords();
    const TempDirCleanup cleanup{ql::filesystem::temp_directory_path() /
                                 "splitVocabLookupBatchEndToEnd"};
    ql::error_code ec;
    ql::filesystem::remove_all(cleanup.dir_, ec);
    ql::filesystem::create_directories(cleanup.dir_);
    measureUnderlying<VocabularyInMemory>(results, "VocabularyInMemory", words,
                                          cleanup.dir_ / "inMemory",
                                          minSeconds);
    measureUnderlying<CompressedVocabulary<VocabularyInMemory>>(
        results, "CompressedVocabulary<VocabularyInMemory>", words,
        cleanup.dir_ / "compressed", minSeconds);
    return results;
  }
};

AD_REGISTER_BENCHMARK(SplitVocabLookupBatchEndToEndBenchmark);

}  // namespace
}  // namespace ad_benchmark
