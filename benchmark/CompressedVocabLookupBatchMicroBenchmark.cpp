// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// Synthetic micro-benchmark for `CompressedVocabulary::lookupBatch` (PR 77):
// a small synthetic vocabulary and tiny batches, comparing sequential
// per-word `operator[]` lookups against the PMR-arena batch lookup. This is
// complemented by `CompressedVocabLookupBatchEndToEndBenchmark`, which measures
// the full write-open-batch pipeline at scale.

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <string_view>
#include <vector>

#include "../benchmark/infrastructure/Benchmark.h"
#include "backports/span.h"
#include "index/vocabulary/CompressedVocabulary.h"
#include "index/vocabulary/VocabularyInMemory.h"

namespace ad_benchmark {
namespace {

class CompressedVocabLookupBatchMicroBenchmark : public BenchmarkInterface {
 private:
  using Vocab = CompressedVocabulary<
      VocabularyInMemory, ad_utility::vocabulary::FsstSquaredCompressionWrapper,
      64>;

  // Remove the `*.words` and `*.codebooks` files of a vocabulary basename.
  // Best effort: failures (e.g. files that were never created) are ignored.
  struct TempFileCleanup {
    std::string basename_;
    ~TempFileCleanup() {
      if (basename_.empty()) {
        return;
      }
      std::error_code ec;
      std::filesystem::remove(basename_ + ".words", ec);
      std::filesystem::remove(basename_ + ".codebooks", ec);
    }
  };

  // Deterministic synthetic words: short tokens (arena edge cases) plus
  // URI-like words with shared prefixes (friendly to FSST and prefix
  // compression). Sorted, as the vocabulary writers require sorted input.
  std::vector<std::string> words_;
  // Batch of lookups in non-ascending order with duplicates.
  std::vector<size_t> batch_;
  Vocab vocab_;
  TempFileCleanup cleanup_;

 public:
  CompressedVocabLookupBatchMicroBenchmark() {
    words_.reserve(512);
    for (size_t i = 0; i < 256; ++i) {
      words_.push_back("w" + std::to_string(i));
    }
    constexpr std::string_view alphabet{
        "abcdefghijklmnopqrstuvwxyz0123456789_:/.-#"};
    for (size_t i = 0; i < 256; ++i) {
      std::string word{"http://www.wikidata.org/entity/Q"};
      for (size_t character = 0; character < 32; ++character) {
        word += alphabet[(i * 17 + character * 31) % alphabet.size()];
      }
      words_.push_back(std::move(word));
    }
    std::sort(words_.begin(), words_.end());

    batch_.reserve(2048);
    for (size_t i = 0; i < 2048; ++i) {
      batch_.push_back((i * 2654435761u) % words_.size());
    }

    cleanup_.basename_ = (std::filesystem::temp_directory_path() /
                          "compressedVocabLookupBatchMicro")
                             .string();
    {
      auto writerPtr = vocab_.makeDiskWriterPtr(cleanup_.basename_);
      for (const auto& word : words_) {
        (*writerPtr)(word, false);
      }
      writerPtr->finish();
    }
    vocab_.open(cleanup_.basename_);
  }

  std::string name() const final {
    return "CompressedVocabulary::lookupBatch (synthetic micro)";
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
    // Bound the workload even when configured through the environment. One
    // repetition takes about 0.2 ms, so the bound allows measurements of well
    // over 10 s (about 80,000 repetitions reach 10 s for both measurements).
    constexpr size_t maxRepetitions = 1'000'000;
    const size_t repetitions = parseEnvironmentSize(
        std::getenv("COMPRESSED_VOCAB_MICRO_REPETITIONS"), 50);
    AD_CONTRACT_CHECK(repetitions > 0);
    AD_CONTRACT_CHECK(repetitions <= maxRepetitions);

    group.addMeasurement("sequential operator[]", [&] {
      size_t totalBytes = 0;
      for (size_t repetition = 0; repetition < repetitions; ++repetition) {
        for (size_t idx : batch_) {
          totalBytes += vocab_[idx].size();
        }
      }
      return totalBytes;
    });
    group.addMeasurement("lookupBatch (PMR arena)", [&] {
      size_t totalBytes = 0;
      for (size_t repetition = 0; repetition < repetitions; ++repetition) {
        const auto result = vocab_.lookupBatch(
            ql::span<const size_t>{batch_.data(), batch_.size()});
        for (size_t i = 0; i < result.size(); ++i) {
          totalBytes += result[i].size();
        }
      }
      return totalBytes;
    });
    return results;
  }
};

AD_REGISTER_BENCHMARK(CompressedVocabLookupBatchMicroBenchmark);

}  // namespace
}  // namespace ad_benchmark
