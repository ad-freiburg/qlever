// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// Synthetic micro-benchmark for `SplitVocabulary::lookupBatch` (PR 78): a
// small synthetic two-way vocabulary and tiny batches, comparing sequential
// per-word `operator[]` lookups against the partitioned batch lookup (one
// dispatch per participating sub-vocabulary plus zero-copy scatter back into
// input order). This is complemented by
// `SplitVocabLookupBatchEndToEndBenchmark`, which measures the full
// write-open-batch pipeline at scale.

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <string_view>
#include <vector>

#include "../benchmark/infrastructure/Benchmark.h"
#include "backports/StartsWithAndEndsWith.h"
#include "backports/span.h"
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

class SplitVocabLookupBatchMicroBenchmark : public BenchmarkInterface {
 private:
  using Vocab = SplitVocabulary<MicroSplitFunc, microFilenameSuffixes,
                                VocabularyInMemory, VocabularyInMemory>;

  // Remove the files of a vocabulary basename. Best effort: failures (e.g.
  // files that were never created) are ignored.
  struct TempFileCleanup {
    std::string basename_;
    ~TempFileCleanup() {
      std::error_code ec;
      std::filesystem::remove(basename_, ec);
      std::filesystem::remove(basename_ + ".a", ec);
    }
  };

  // Deterministic synthetic words: short tokens plus URI-like words with
  // shared prefixes. Every third word starts with `"a` and is routed to
  // marker 1. Sorted, as the vocabulary writers require sorted input.
  std::vector<std::string> words_;
  // Global (marker-encoded) indices of all words, in vocabulary order.
  std::vector<size_t> marked_;
  // Batch of lookups in non-ascending order with duplicates, mixing both
  // markers, plus a batch holding only marker-0 indices.
  std::vector<size_t> mixedBatch_;
  std::vector<size_t> singleMarkerBatch_;
  Vocab vocab_;
  TempFileCleanup cleanup_;

 public:
  SplitVocabLookupBatchMicroBenchmark() {
    words_.reserve(512);
    for (size_t i = 0; i < 256; ++i) {
      words_.push_back("\"w" + std::to_string(i) + "\"");
    }
    constexpr std::string_view alphabet{
        "abcdefghijklmnopqrstuvwxyz0123456789_:/.-#"};
    for (size_t i = 0; i < 256; ++i) {
      // Every third word goes to marker 1.
      std::string word{i % 3 == 0 ? "\"aentity" : "\"entity"};
      word += std::to_string(i);
      word += '/';
      for (size_t character = 0; character < 32; ++character) {
        word += alphabet[(i * 17 + character * 31) % alphabet.size()];
      }
      word += '"';
      words_.push_back(std::move(word));
    }
    std::sort(words_.begin(), words_.end());

    cleanup_.basename_ =
        (std::filesystem::temp_directory_path() / "splitVocabLookupBatchMicro")
            .string();
    {
      auto writerPtr = vocab_.makeDiskWriterPtr(cleanup_.basename_);
      for (const auto& word : words_) {
        (*writerPtr)(word, false);
      }
      writerPtr->finish();
    }
    vocab_.readFromFile(cleanup_.basename_);

    for (const IndexAndWord& indexAndWord : vocab_.scanAll()) {
      marked_.push_back(static_cast<size_t>(indexAndWord.index_));
    }

    mixedBatch_.reserve(2048);
    for (size_t i = 0; i < 2048; ++i) {
      mixedBatch_.push_back(marked_[(i * 2654435761u) % marked_.size()]);
    }
    std::vector<size_t> markerZero;
    for (size_t index : marked_) {
      if (Vocab::getMarker(index) == 0) {
        markerZero.push_back(index);
      }
    }
    singleMarkerBatch_.reserve(2048);
    for (size_t i = 0; i < 2048; ++i) {
      singleMarkerBatch_.push_back(markerZero[i % markerZero.size()]);
    }
  }

  std::string name() const final {
    return "SplitVocabulary::lookupBatch (synthetic micro)";
  }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results;
    auto& group = results.addGroup(
        "Synthetic micro-batches: 2,048 lookups into 512 words");
    const auto parseEnvironmentSize = [](const char* value,
                                         size_t defaultValue) {
      if (value == nullptr) {
        return defaultValue;
      }
      errno = 0;
      char* end = nullptr;
      const unsigned long parsed = std::strtoul(value, &end, 10);
      AD_CONTRACT_CHECK(end != value && *end == '\0' && errno != ERANGE);
      return static_cast<size_t>(parsed);
    };
    // Bound the workload even when configured through the environment.
    constexpr size_t maxRepetitions = 10'000;
    const size_t repetitions =
        parseEnvironmentSize(std::getenv("SPLIT_VOCAB_MICRO_REPETITIONS"), 50);
    AD_CONTRACT_CHECK(repetitions > 0);
    AD_CONTRACT_CHECK(repetitions <= maxRepetitions);

    const auto runBatched = [&](const std::vector<size_t>& batch) {
      size_t totalBytes = 0;
      for (size_t repetition = 0; repetition < repetitions; ++repetition) {
        auto result = vocab_.lookupBatch(batch);
        for (const auto& word : result) {
          totalBytes += word.size();
        }
      }
      return totalBytes;
    };
    // What `SplitVocabulary::lookupBatch` did before it partitioned by marker:
    // the generic fallback, one owning `std::string` per word, all kept alive
    // in the result. This is the baseline that the batched path replaces.
    const auto runFallback = [&](const std::vector<size_t>& batch) {
      size_t totalBytes = 0;
      for (size_t repetition = 0; repetition < repetitions; ++repetition) {
        auto result =
            ad_utility::vocabulary::sequentialLookupBatch(vocab_, batch);
        for (const auto& word : result) {
          totalBytes += word.size();
        }
      }
      return totalBytes;
    };
    const auto runSequential = [&](const std::vector<size_t>& batch) {
      size_t totalBytes = 0;
      for (size_t repetition = 0; repetition < repetitions; ++repetition) {
        for (size_t index : batch) {
          std::string word{vocab_[index]};
          totalBytes += word.size();
        }
      }
      return totalBytes;
    };

    group.addMeasurement("sequential operator[], mixed markers",
                         [&] { return runSequential(mixedBatch_); });
    group.addMeasurement("fallback sequentialLookupBatch, mixed markers",
                         [&] { return runFallback(mixedBatch_); });
    group.addMeasurement("batched lookupBatch, mixed markers",
                         [&] { return runBatched(mixedBatch_); });
    group.addMeasurement("sequential operator[], single marker",
                         [&] { return runSequential(singleMarkerBatch_); });
    group.addMeasurement("fallback sequentialLookupBatch, single marker",
                         [&] { return runFallback(singleMarkerBatch_); });
    group.addMeasurement("batched lookupBatch, single marker",
                         [&] { return runBatched(singleMarkerBatch_); });
    return results;
  }
};

AD_REGISTER_BENCHMARK(SplitVocabLookupBatchMicroBenchmark);

}  // namespace
}  // namespace ad_benchmark
