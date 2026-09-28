// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// Synthetic micro-benchmark for the polymorphic `lookupBatch` dispatch:
// small synthetic vocabularies behind a
// `PolymorphicVocabulary` and tiny batches, comparing sequential per-word
// `operator[]` lookups against a single `lookupBatch` call and against the
// arena-based `lookupBatch(indices, builder)` overload. The compressed
// vocabulary exercises the builder path (direct decode into the arena), the
// uncompressed vocabulary exercises the copy path (its result is copied into
// the builder). The "concrete" measurement opens the same files with the
// concrete vocabulary type (no `std::visit`), which separates the cost of the
// polymorphic dispatch from the cost of the underlying batched lookup. This
// is complemented by `PolymorphicVocabLookupBatchEndToEndBenchmark`, which
// measures the full write-open-batch pipeline at scale.

#include <cerrno>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "../benchmark/infrastructure/Benchmark.h"
#include "absl/strings/str_cat.h"
#include "backports/span.h"
#include "index/vocabulary/CompressedVocabulary.h"
#include "index/vocabulary/PolymorphicVocabulary.h"
#include "index/vocabulary/VocabularyInternalExternal.h"
#include "index/vocabulary/VocabularyType.h"
#include "index/vocabulary/VocabularyTypes.h"
#include "util/Exception.h"

namespace ad_benchmark {
namespace {

class PolymorphicVocabLookupBatchMicroBenchmark : public BenchmarkInterface {
 private:
  // Remove the files of a vocabulary basename: the base file itself plus the
  // auxiliary files of compressed vocabularies (see
  // `CompressedVocabulary::wordsSuffix` and `decodersSuffix`). Best effort:
  // failures (e.g. files that were never created) are ignored.
  struct TempFileCleanup {
    std::string basename_;
    ~TempFileCleanup() {
      std::error_code ec;
      std::filesystem::remove(basename_, ec);
      std::filesystem::remove(basename_ + ".words", ec);
      std::filesystem::remove(basename_ + ".codebooks", ec);
    }
  };

  // Batch of lookups in pseudo-random order with duplicates (every fifth
  // entry repeats the previous index).
  std::vector<size_t> batch_;
  PolymorphicVocabulary compressedVocab_;
  PolymorphicVocabulary uncompressedVocab_;
  // The same files, opened with the concrete types of the `OnDiskCompressed`
  // and `OnDiskUncompressed` alternatives of `PolymorphicVocabulary`.
  CompressedVocabulary<VocabularyInternalExternal> compressedConcrete_;
  VocabularyInternalExternal uncompressedConcrete_;
  TempFileCleanup compressedCleanup_;
  TempFileCleanup uncompressedCleanup_;

  static void buildVocabulary(PolymorphicVocabulary& vocab,
                              ad_utility::VocabularyType::Enum vocabType,
                              const std::string& basename, size_t numWords) {
    ad_utility::VocabularyType type{vocabType};
    auto writerPtr = PolymorphicVocabulary::makeDiskWriterPtr(basename, type);
    // Deterministic synthetic words with a long shared prefix (compresses
    // well). Fixed-width zero padding keeps them sorted, as the vocabulary
    // writers require sorted input.
    for (size_t i = 0; i < numWords; ++i) {
      std::string digits = std::to_string(i);
      constexpr size_t kWidth = 6;
      if (digits.size() < kWidth) {
        digits.insert(0, kWidth - digits.size(), '0');
      }
      (*writerPtr)(absl::StrCat("http://example.org/entity/", digits,
                                "_suffix_to_improve_prefix_compression"),
                   false);
    }
    writerPtr->finish();
    vocab.open(basename, type);
  }

 public:
  PolymorphicVocabLookupBatchMicroBenchmark() {
    constexpr size_t numWords = 2048;
    constexpr size_t batchSize = 2048;
    compressedCleanup_.basename_ =
        (std::filesystem::temp_directory_path() / "polyVocabLookupBatchMicroC")
            .string();
    uncompressedCleanup_.basename_ =
        (std::filesystem::temp_directory_path() / "polyVocabLookupBatchMicroU")
            .string();
    buildVocabulary(compressedVocab_,
                    ad_utility::VocabularyType::Enum::OnDiskCompressed,
                    compressedCleanup_.basename_, numWords);
    buildVocabulary(uncompressedVocab_,
                    ad_utility::VocabularyType::Enum::OnDiskUncompressed,
                    uncompressedCleanup_.basename_, numWords);
    compressedConcrete_.open(compressedCleanup_.basename_);
    uncompressedConcrete_.open(uncompressedCleanup_.basename_);
    batch_.reserve(batchSize);
    for (size_t i = 0; i < batchSize; ++i) {
      batch_.push_back((i % 5 == 0 && i > 0)
                           ? ((i - 1) * 2654435761u) % numWords
                           : (i * 2654435761u) % numWords);
    }
  }

  std::string name() const final {
    return "PolymorphicVocabulary::lookupBatch (synthetic micro)";
  }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results;
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
        parseEnvironmentSize(std::getenv("POLY_VOCAB_MICRO_REPETITIONS"), 50);
    AD_CONTRACT_CHECK(repetitions > 0);
    AD_CONTRACT_CHECK(repetitions <= maxRepetitions);
    // Optional filter for profiling a single measurement: if set, only the
    // measurements whose name contains this substring are run.
    const char* onlyMeasurement = std::getenv("POLY_VOCAB_ONLY_MEASUREMENT");
    const auto addMeasurement = [onlyMeasurement](auto& group,
                                                  const std::string& name,
                                                  auto measurement) {
      if (onlyMeasurement == nullptr ||
          name.find(onlyMeasurement) != std::string::npos) {
        group.addMeasurement(name, std::move(measurement));
      }
    };

    const auto runComparison = [&](auto& group,
                                   const PolymorphicVocabulary& vocab,
                                   const auto& concrete) {
      addMeasurement(group, "sequential operator[]", [&] {
        size_t totalBytes = 0;
        for (size_t repetition = 0; repetition < repetitions; ++repetition) {
          for (size_t index : batch_) {
            std::string word{vocab[index]};
            totalBytes += word.size();
          }
        }
        return totalBytes;
      });
      addMeasurement(group, "batched lookupBatch", [&] {
        size_t totalBytes = 0;
        for (size_t repetition = 0; repetition < repetitions; ++repetition) {
          auto result = vocab.lookupBatch(batch_);
          for (const auto& word : result) {
            totalBytes += word.size();
          }
        }
        return totalBytes;
      });
      addMeasurement(group, "batched lookupBatch with builder", [&] {
        size_t totalBytes = 0;
        for (size_t repetition = 0; repetition < repetitions; ++repetition) {
          ArenaVocabBatchBuilder builder(batch_.size());
          vocab.lookupBatch(batch_, builder);
          auto result = std::move(builder).finalize();
          for (const auto& word : result) {
            totalBytes += word.size();
          }
        }
        return totalBytes;
      });
      addMeasurement(group, "concrete lookupBatch (no dispatch)", [&] {
        size_t totalBytes = 0;
        for (size_t repetition = 0; repetition < repetitions; ++repetition) {
          auto result = concrete.lookupBatch(batch_);
          for (const auto& word : result) {
            totalBytes += word.size();
          }
        }
        return totalBytes;
      });
    };

    auto& compressedGroup = results.addGroup(
        "Synthetic micro-batches: 2,048 lookups into 2,048 words "
        "(on-disk compressed, builder path)");
    runComparison(compressedGroup, compressedVocab_, compressedConcrete_);
    auto& uncompressedGroup = results.addGroup(
        "Synthetic micro-batches: 2,048 lookups into 2,048 words "
        "(on-disk uncompressed, copy path)");
    runComparison(uncompressedGroup, uncompressedVocab_, uncompressedConcrete_);
    return results;
  }
};

AD_REGISTER_BENCHMARK(PolymorphicVocabLookupBatchMicroBenchmark);

}  // namespace
}  // namespace ad_benchmark
