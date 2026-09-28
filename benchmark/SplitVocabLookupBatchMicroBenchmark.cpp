// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// Synthetic micro-benchmark for `SplitVocabulary::lookupBatch`: a small
// two-way split vocabulary and batches of 2,048 lookups. For two kinds of
// underlying vocabularies (`VocabularyInMemory`, whose words are plain
// in-memory strings, and `CompressedVocabulary<VocabularyInMemory>`, whose
// words have to be decompressed) it compares
// - a loop of per-word `operator[]` calls that copies each word into a
//   `std::string` and drops it again (no result is kept),
// - the generic fallback `sequentialLookupBatch` (what
//   `SplitVocabulary::lookupBatch` did before it was specialized: one owning
//   `std::string` per word, all kept alive in the result), and
// - `SplitVocabulary::lookupBatch`.
// Mixed-marker batches and single-marker batches are measured separately.
// Every measurement repeats the batch `SPLIT_VOCAB_MICRO_REPETITIONS` times
// (default 50) after one untimed warm-up repetition; the time per word is the
// measured time divided by `repetitions * 2048`.
// This is complemented by `SplitVocabLookupBatchEndToEndBenchmark`, which uses
// a larger vocabulary and batch.

#include <absl/strings/str_cat.h>

#include <algorithm>
#include <array>
#include <cerrno>
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

struct MicroSplitFunc {
  uint8_t operator()(std::string_view word) const {
    return ql::starts_with(word, "\"a");
  }
};

// Vocabulary 0 is stored under the base filename, vocabulary 1 with suffix
// ".a".
constexpr std::array<std::string_view, 2> microFilenameSuffixes{"", ".a"};

template <typename Underlying>
using MicroSplitVocab = SplitVocabulary<MicroSplitFunc, microFilenameSuffixes,
                                        Underlying, Underlying>;

constexpr size_t batchSize = 2048;

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

// Deterministic synthetic words: short tokens plus URI-like words with shared
// prefixes. Every third URI-like word starts with `"a` and is routed to
// marker 1. Sorted, as the vocabulary writers require sorted input.
std::vector<std::string> makeWords() {
  std::vector<std::string> words;
  words.reserve(512);
  for (size_t i = 0; i < 256; ++i) {
    words.push_back("\"w" + std::to_string(i) + "\"");
  }
  constexpr std::string_view alphabet{
      "abcdefghijklmnopqrstuvwxyz0123456789_:/.-#"};
  for (size_t i = 0; i < 256; ++i) {
    std::string word{i % 3 == 0 ? "\"aentity" : "\"entity"};
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

// Build a split vocabulary with `Underlying` vocabularies from `words` in
// `dir`, and measure the three lookup strategies (see the file comment) on a
// mixed-marker and a single-marker batch into a new group of `results`.
template <typename Underlying>
void measureUnderlying(BenchmarkResults& results, const std::string& label,
                       const std::vector<std::string>& words,
                       const std::filesystem::path& basename,
                       size_t repetitions) {
  using Vocab = MicroSplitVocab<Underlying>;
  Vocab vocab;
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
  // A batch in non-ascending order with duplicates, mixing both markers, and
  // a batch holding only marker-0 indices.
  std::vector<size_t> mixedBatch;
  mixedBatch.reserve(batchSize);
  for (size_t i = 0; i < batchSize; ++i) {
    mixedBatch.push_back(marked[(i * 2654435761u) % marked.size()]);
  }
  std::vector<size_t> markerZero;
  for (size_t index : marked) {
    if (Vocab::getMarker(index) == 0) {
      markerZero.push_back(index);
    }
  }
  std::vector<size_t> singleMarkerBatch;
  singleMarkerBatch.reserve(batchSize);
  for (size_t i = 0; i < batchSize; ++i) {
    singleMarkerBatch.push_back(markerZero[i % markerZero.size()]);
  }

  const auto sumSizes = [](const auto& result) {
    size_t totalBytes = 0;
    for (const auto& word : result) {
      totalBytes += word.size();
    }
    return totalBytes;
  };
  const auto runSequential = [&](const std::vector<size_t>& batch,
                                 size_t numRepetitions) {
    size_t totalBytes = 0;
    for (size_t repetition = 0; repetition < numRepetitions; ++repetition) {
      for (size_t index : batch) {
        std::string word{vocab[index]};
        totalBytes += word.size();
      }
    }
    return totalBytes;
  };
  const auto runFallback = [&](const std::vector<size_t>& batch,
                               size_t numRepetitions) {
    size_t totalBytes = 0;
    for (size_t repetition = 0; repetition < numRepetitions; ++repetition) {
      totalBytes +=
          sumSizes(ad_utility::vocabulary::sequentialLookupBatch(vocab, batch));
    }
    return totalBytes;
  };
  const auto runBatched = [&](const std::vector<size_t>& batch,
                              size_t numRepetitions) {
    size_t totalBytes = 0;
    for (size_t repetition = 0; repetition < numRepetitions; ++repetition) {
      totalBytes += sumSizes(vocab.lookupBatch(batch));
    }
    return totalBytes;
  };

  // One untimed warm-up repetition of every strategy (caches, allocator).
  for (const auto* batch : {&mixedBatch, &singleMarkerBatch}) {
    runSequential(*batch, 1);
    runFallback(*batch, 1);
    runBatched(*batch, 1);
  }

  auto& group = results.addGroup(
      absl::StrCat(label, ": ", batchSize, " lookups into ", words.size(),
                   " words, ", repetitions, " repetitions"));
  group.addMeasurement("sequential operator[], mixed markers",
                       [&] { return runSequential(mixedBatch, repetitions); });
  group.addMeasurement("fallback sequentialLookupBatch, mixed markers",
                       [&] { return runFallback(mixedBatch, repetitions); });
  group.addMeasurement("batched lookupBatch, mixed markers",
                       [&] { return runBatched(mixedBatch, repetitions); });
  group.addMeasurement("sequential operator[], single marker", [&] {
    return runSequential(singleMarkerBatch, repetitions);
  });
  group.addMeasurement("fallback sequentialLookupBatch, single marker", [&] {
    return runFallback(singleMarkerBatch, repetitions);
  });
  group.addMeasurement("batched lookupBatch, single marker", [&] {
    return runBatched(singleMarkerBatch, repetitions);
  });
  vocab.close();
}

class SplitVocabLookupBatchMicroBenchmark : public BenchmarkInterface {
 public:
  std::string name() const final {
    return "SplitVocabulary::lookupBatch (synthetic micro)";
  }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results;
    const size_t repetitions =
        sizeFromEnvironment("SPLIT_VOCAB_MICRO_REPETITIONS", 50, 1'000'000);
    const auto words = makeWords();
    TempDirCleanup cleanup{std::filesystem::temp_directory_path() /
                           "splitVocabLookupBatchMicro"};
    std::error_code ec;
    std::filesystem::remove_all(cleanup.dir_, ec);
    std::filesystem::create_directories(cleanup.dir_);
    measureUnderlying<VocabularyInMemory>(results, "VocabularyInMemory", words,
                                          cleanup.dir_ / "inMemory",
                                          repetitions);
    measureUnderlying<CompressedVocabulary<VocabularyInMemory>>(
        results, "CompressedVocabulary<VocabularyInMemory>", words,
        cleanup.dir_ / "compressed", repetitions);
    return results;
  }
};

AD_REGISTER_BENCHMARK(SplitVocabLookupBatchMicroBenchmark);

}  // namespace
}  // namespace ad_benchmark
