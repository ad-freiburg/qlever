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
// measurement repeats the batch until `SPLIT_VOCAB_MIN_SECONDS` (default 10)
// have passed, after one untimed warm-up pass; the time per word is the
// measured time divided by the `words` in the measurement's metadata.

#include <absl/strings/str_cat.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <string_view>
#include <vector>

#include "../benchmark/infrastructure/Benchmark.h"
#include "backports/StartsWithAndEndsWith.h"
#include "backports/span.h"
#include "index/vocabulary/CompressedVocabulary.h"
#include "index/vocabulary/SplitVocabulary.h"
#include "index/vocabulary/SplitVocabularyImpl.h"
#include "index/vocabulary/VocabularyInMemory.h"
#include "index/vocabulary/VocabularyTypes.h"

namespace ad_benchmark {
namespace {

struct EndToEndSplitFunc {
  uint8_t operator()(std::string_view word) const {
    return ql::starts_with(word, "\"a");
  }
};

// Vocabulary 0 is stored under the base filename, vocabulary 1 with suffix
// ".a".
constexpr std::array<std::string_view, 2> endToEndFilenameSuffixes{"", ".a"};

template <typename Underlying>
using EndToEndSplitVocab =
    SplitVocabulary<EndToEndSplitFunc, endToEndFilenameSuffixes, Underlying,
                    Underlying>;

constexpr size_t numWords = 50'000;
constexpr size_t batchSize = 100'000;

// Remove a whole directory tree. Best effort: failures are ignored.
struct TempDirCleanup {
  std::filesystem::path dir_;
  ~TempDirCleanup() {
    std::error_code ec;
    std::filesystem::remove_all(dir_, ec);
  }
};

// Parse a positive size from the environment variable `name`, or return
// `defaultValue` if it is not set.
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
// calls, the number of words, and the bytes per call are recorded as metadata,
// so the time per word is the measured time divided by `words`.
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

// Deterministic Wikidata-like IRIs, every third one routed to marker 1 via
// the `"a` prefix. Sorted, as the vocabulary writers require sorted input.
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
  std::sort(words.begin(), words.end());
  return words;
}

// Build a split vocabulary with `Underlying` vocabularies from `words` under
// `basename`, and measure the three lookup strategies on one large batch in
// non-ascending order with duplicates (mimicking the access pattern of
// batched ID-to-word resolution during query evaluation).
template <typename Underlying>
void measureUnderlying(BenchmarkResults& results, const std::string& label,
                       const std::vector<std::string>& words,
                       const std::filesystem::path& basename,
                       double minSeconds) {
  EndToEndSplitVocab<Underlying> vocab;
  {
    auto writerPtr = vocab.makeDiskWriterPtr(basename.string());
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
  std::vector<size_t> batch;
  batch.reserve(batchSize);
  for (size_t i = 0; i < batchSize; ++i) {
    batch.push_back(marked[(i * 2654435761u) % marked.size()]);
  }

  const auto sumSizes = [](const auto& result) {
    size_t totalBytes = 0;
    for (const auto& word : result) {
      totalBytes += word.size();
    }
    return totalBytes;
  };
  const auto runSequential = [&](size_t numRepetitions) {
    size_t totalBytes = 0;
    for (size_t repetition = 0; repetition < numRepetitions; ++repetition) {
      for (size_t index : batch) {
        std::string word{vocab[index]};
        totalBytes += word.size();
      }
    }
    return totalBytes;
  };
  const auto runFallback = [&](size_t numRepetitions) {
    size_t totalBytes = 0;
    for (size_t repetition = 0; repetition < numRepetitions; ++repetition) {
      totalBytes +=
          sumSizes(ad_utility::vocabulary::sequentialLookupBatch(vocab, batch));
    }
    return totalBytes;
  };
  const auto runBatched = [&](size_t numRepetitions) {
    size_t totalBytes = 0;
    for (size_t repetition = 0; repetition < numRepetitions; ++repetition) {
      totalBytes += sumSizes(vocab.lookupBatch(batch));
    }
    return totalBytes;
  };

  // One untimed warm-up repetition of every strategy (caches, allocator).
  runSequential(1);
  runFallback(1);
  runBatched(1);

  auto& group =
      results.addGroup(absl::StrCat(label, ": ", batchSize, " lookups into ",
                                    words.size(), " words in two markers"));
  addTimedMeasurement(group, "sequential operator[]", minSeconds, batchSize,
                      [&] { return runSequential(1); });
  addTimedMeasurement(group, "fallback sequentialLookupBatch", minSeconds,
                      batchSize, [&] { return runFallback(1); });
  addTimedMeasurement(group, "batched lookupBatch", minSeconds, batchSize,
                      [&] { return runBatched(1); });
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
    TempDirCleanup cleanup{std::filesystem::temp_directory_path() /
                           "splitVocabLookupBatchEndToEnd"};
    std::error_code ec;
    std::filesystem::remove_all(cleanup.dir_, ec);
    std::filesystem::create_directories(cleanup.dir_);
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
