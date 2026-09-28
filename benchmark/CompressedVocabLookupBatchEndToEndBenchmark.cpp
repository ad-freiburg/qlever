// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// Full end-to-end benchmark for `CompressedVocabulary::lookupBatch` (PR 77):
// build a realistically sized compressed vocabulary through the actual
// write-to-disk path, open it back, and resolve a large batch of lookups,
// comparing sequential per-word `operator[]` lookups against the PMR-arena
// batch lookup. This is complemented by
// `CompressedVocabLookupBatchMicroBenchmark`, which measures per-word costs on
// tiny synthetic batches.

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
#include "index/vocabulary/VocabularyOnDisk.h"

namespace ad_benchmark {
namespace {

class CompressedVocabLookupBatchEndToEndBenchmark : public BenchmarkInterface {
 private:
  // WTO + FSST-squared compression over on-disk storage, with a small block
  // size so the benchmark spans multiple decoders (like production
  // vocabularies, which use one decoder per 2^20 words).
  static constexpr size_t numWordsPerBlock = 4096;
  using Vocab = CompressedVocabulary<
      VocabularyOnDisk, ad_utility::vocabulary::FsstSquaredCompressionWrapper,
      numWordsPerBlock>;

  // Remove a whole directory tree. Best effort: failures are ignored.
  struct TempDirCleanup {
    std::filesystem::path dir_;
    ~TempDirCleanup() {
      if (dir_.empty()) {
        return;
      }
      std::error_code ec;
      std::filesystem::remove_all(dir_, ec);
    }
  };

  // Deterministic Wikidata-like IRIs. Sorted, as the vocabulary writers
  // require sorted input.
  std::vector<std::string> words_;
  // Large batch of lookups in non-ascending order with duplicates, mimicking
  // the access pattern of batch ID-to-word resolution during query evaluation.
  std::vector<size_t> batch_;
  TempDirCleanup cleanup_;
  Vocab vocab_;

  // Compress, write, and re-open a vocabulary with the given basename.
  Vocab buildVocabulary(const std::string& basename) {
    Vocab vocab;
    {
      auto writerPtr = vocab.makeDiskWriterPtr(basename);
      for (const auto& word : words_) {
        (*writerPtr)(word, false);
      }
      writerPtr->finish();
    }
    vocab.open(basename);
    return vocab;
  }

 public:
  CompressedVocabLookupBatchEndToEndBenchmark() {
    constexpr size_t numWords = 50'000;
    constexpr std::string_view alphabet{
        "abcdefghijklmnopqrstuvwxyz0123456789_:/.-#"};
    words_.reserve(numWords);
    for (size_t i = 0; i < numWords; ++i) {
      std::string word{"http://www.wikidata.org/entity/Q"};
      word += std::to_string(i);
      word += '/';
      for (size_t character = 0; character < 32; ++character) {
        word += alphabet[(i * 17 + character * 31) % alphabet.size()];
      }
      words_.push_back(std::move(word));
    }
    std::sort(words_.begin(), words_.end());

    constexpr size_t batchSize = 100'000;
    batch_.reserve(batchSize);
    for (size_t i = 0; i < batchSize; ++i) {
      batch_.push_back((i * 2654435761u) % words_.size());
    }

    cleanup_.dir_ = std::filesystem::temp_directory_path() /
                    "compressedVocabLookupBatchEndToEnd";
    std::error_code ec;
    std::filesystem::remove_all(cleanup_.dir_, ec);
    std::filesystem::create_directories(cleanup_.dir_, ec);
    vocab_ = buildVocabulary((cleanup_.dir_ / "vocab").string());
  }

  std::string name() const final {
    return "CompressedVocabulary::lookupBatch (end to end)";
  }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results;
    auto& group = results.addGroup(
        "End to end: 50,000 words in 13 decoder blocks, "
        "100,000 lookups per batch");
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
    constexpr size_t maxRepetitions = 100;
    const size_t repetitions = parseEnvironmentSize(
        std::getenv("COMPRESSED_VOCAB_E2E_REPETITIONS"), 5);
    AD_CONTRACT_CHECK(repetitions > 0);
    AD_CONTRACT_CHECK(repetitions <= maxRepetitions);

    size_t rebuildCounter = 0;
    group.addMeasurement("build: compress + write + open", [&] {
      size_t totalBytes = 0;
      for (size_t repetition = 0; repetition < repetitions; ++repetition) {
        const std::string basename =
            (cleanup_.dir_ / ("rebuild" + std::to_string(rebuildCounter++)))
                .string();
        Vocab rebuilt = buildVocabulary(basename);
        // Consume the result so the build cannot be optimized away.
        for (size_t i = 0; i < words_.size(); i += 1000) {
          totalBytes += rebuilt[i].size();
        }
      }
      return totalBytes;
    });
    group.addMeasurement("sequential operator[] over batch", [&] {
      size_t totalBytes = 0;
      for (size_t repetition = 0; repetition < repetitions; ++repetition) {
        for (size_t idx : batch_) {
          totalBytes += vocab_[idx].size();
        }
      }
      return totalBytes;
    });
    group.addMeasurement("lookupBatch over batch (PMR arena)", [&] {
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

AD_REGISTER_BENCHMARK(CompressedVocabLookupBatchEndToEndBenchmark);

}  // namespace
}  // namespace ad_benchmark
