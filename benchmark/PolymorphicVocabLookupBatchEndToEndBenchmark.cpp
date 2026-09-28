// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// Full end-to-end benchmark for the polymorphic `lookupBatch` dispatch (PR
// marvin7122/qlever#79): build a realistically sized compressed vocabulary
// behind a `PolymorphicVocabulary` through the actual write-to-disk path,
// read it back, and resolve a large batch of lookups in pseudo-random order
// with duplicates, comparing sequential per-word `operator[]` lookups against
// a single `lookupBatch` call and against the arena-based
// `lookupBatch(indices, builder)` overload. This is complemented by
// `PolymorphicVocabLookupBatchMicroBenchmark`, which measures per-batch costs
// on tiny synthetic batches.

#include <cerrno>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <string_view>
#include <vector>

#include "../benchmark/infrastructure/Benchmark.h"
#include "absl/strings/str_cat.h"
#include "backports/span.h"
#include "index/vocabulary/PolymorphicVocabulary.h"
#include "index/vocabulary/VocabularyType.h"
#include "index/vocabulary/VocabularyTypes.h"
#include "util/Exception.h"

namespace ad_benchmark {
namespace {

using PolymorphicVocabulary;

class PolymorphicVocabLookupBatchEndToEndBenchmark : public BenchmarkInterface {
 private:
  // Remove a whole directory tree. Best effort: failures are ignored.
  struct TempDirCleanup {
    std::filesystem::path dir_;
    ~TempDirCleanup() {
      std::error_code ec;
      std::filesystem::remove_all(dir_, ec);
    }
  };

  // Large batch of vocabulary indices in pseudo-random order with duplicates
  // (every fifth entry repeats the previous index), mimicking the access
  // pattern of batch ID-to-word resolution during query evaluation.
  std::vector<size_t> batch_;
  TempDirCleanup cleanup_;
  PolymorphicVocabulary vocab_;

  // Write `numWords` deterministic Wikidata-like IRIs to disk and read them
  // back into a vocabulary with the given basename.
  PolymorphicVocabulary buildVocabulary(const std::string& basename,
                                        size_t numWords) {
    PolymorphicVocabulary vocab;
    ad_utility::VocabularyType type{
        ad_utility::VocabularyType::Enum::OnDiskCompressed};
    {
      auto writerPtr = PolymorphicVocabulary::makeDiskWriterPtr(basename, type);
      for (size_t i = 0; i < numWords; ++i) {
        // Fixed-width zero padding keeps the words sorted, as the vocabulary
        // writers require sorted input.
        std::string digits = std::to_string(i);
        constexpr size_t kWidth = 8;
        if (digits.size() < kWidth) {
          digits.insert(0, kWidth - digits.size(), '0');
        }
        (*writerPtr)(absl::StrCat("http://www.wikidata.org/entity/Q", digits,
                                  "_suffix_to_improve_prefix_compression"),
                     false);
      }
      writerPtr->finish();
    }
    vocab.open(basename, type);
    return vocab;
  }

 public:
  PolymorphicVocabLookupBatchEndToEndBenchmark() {
    constexpr size_t numWords = 50'000;
    cleanup_.dir_ =
        std::filesystem::temp_directory_path() / "polyVocabLookupBatchEndToEnd";
    std::error_code ec;
    std::filesystem::remove_all(cleanup_.dir_, ec);
    std::filesystem::create_directories(cleanup_.dir_, ec);
    vocab_ = buildVocabulary((cleanup_.dir_ / "vocab").string(), numWords);

    constexpr size_t batchSize = 100'000;
    batch_.reserve(batchSize);
    for (size_t i = 0; i < batchSize; ++i) {
      batch_.push_back((i % 5 == 0 && i > 0)
                           ? ((i - 1) * 2654435761u) % numWords
                           : (i * 2654435761u) % numWords);
    }
  }

  std::string name() const final {
    return "PolymorphicVocabulary::lookupBatch (end to end)";
  }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results;
    auto& group = results.addGroup(
        "End to end: 50,000 words (on-disk compressed), 100,000 lookups "
        "per batch");
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
    const size_t repetitions =
        parseEnvironmentSize(std::getenv("POLY_VOCAB_E2E_REPETITIONS"), 5);
    AD_CONTRACT_CHECK(repetitions > 0);
    AD_CONTRACT_CHECK(repetitions <= maxRepetitions);

    group.addMeasurement("sequential operator[]", [&] {
      size_t totalBytes = 0;
      for (size_t repetition = 0; repetition < repetitions; ++repetition) {
        for (size_t index : batch_) {
          std::string word{vocab_[index]};
          totalBytes += word.size();
        }
      }
      return totalBytes;
    });
    group.addMeasurement("batched lookupBatch", [&] {
      size_t totalBytes = 0;
      for (size_t repetition = 0; repetition < repetitions; ++repetition) {
        auto result = vocab_.lookupBatch(batch_);
        for (const auto& word : result) {
          totalBytes += word.size();
        }
      }
      return totalBytes;
    });
    group.addMeasurement("batched lookupBatch with builder", [&] {
      size_t totalBytes = 0;
      for (size_t repetition = 0; repetition < repetitions; ++repetition) {
        ArenaVocabBatchBuilder builder(batch_.size());
        vocab_.lookupBatch(batch_, builder);
        auto result = std::move(builder).finalize();
        for (const auto& word : result) {
          totalBytes += word.size();
        }
      }
      return totalBytes;
    });
    return results;
  }
};

AD_REGISTER_BENCHMARK(PolymorphicVocabLookupBatchEndToEndBenchmark);

}  // namespace
}  // namespace ad_benchmark
