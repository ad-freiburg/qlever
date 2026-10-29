// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// Benchmarks for the batch vocabulary lookup API introduced in
// `src/index/vocabulary/VocabularyTypes.h` (`VocabBatchLookupResult` and the
// contiguous/arena builders):
// * `VocabBatchLookupMicroBenchmark` (synthetic): assembling a batch with
//   `ContiguousVocabBatchBuilder` (from precomputed word sizes) or
//   `ArenaVocabBatchBuilder` (appending the words one by one) vs. the old
//   per-word `vector<string>`-by-value cost.
// * `VocabBatchLookupEndToEndBenchmark`: resolving a shuffled index batch
//   against a real `VocabularyInMemoryBinSearch` via a single `lookupBatch`
//   call vs. the old per-word `operator[]` loop. For this vocabulary,
//   `lookupBatch` is the sequential fallback, so the comparison measures the
//   overhead of the result type, not batched I/O.

#include <absl/strings/str_cat.h>

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <numeric>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include "../benchmark/infrastructure/Benchmark.h"
#include "backports/span.h"
#include "index/vocabulary/VocabularyInMemoryBinSearch.h"
#include "index/vocabulary/VocabularyTypes.h"
#include "util/Exception.h"
#include "util/File.h"

namespace ad_benchmark {
namespace {

// _____________________________________________________________________________
// Parse the unsigned integer in the environment variable value `value`.
// Return `defaultValue` if the variable is unset or empty, and fail on
// malformed or out-of-range input.
size_t parseEnvironmentSize(const char* value, size_t defaultValue) {
  if (value == nullptr || value[0] == '\0') {
    return defaultValue;
  }
  errno = 0;
  char* end = nullptr;
  const unsigned long parsed = std::strtoul(value, &end, 10);
  AD_CONTRACT_CHECK(end != value && *end == '\0' && errno != ERANGE);
  return static_cast<size_t>(parsed);
}

// _____________________________________________________________________________
// Deterministic synthetic vocabulary words of varying length: a third each are
// Wikidata entity IRIs, language-tagged literals, and property IRIs in angle
// brackets. Every word is unique because the zero-padded index is encoded into
// it (a real vocabulary has no duplicate words); only word 0 is empty, to cover
// the zero-size path.
std::string makeSyntheticWord(size_t index) {
  if (index == 0) {
    return "";
  }
  static constexpr std::string_view alphabet{
      "abcdefghijklmnopqrstuvwxyz0123456789_:/.-#"};
  std::string suffix = absl::StrCat(absl::Dec(index, absl::kZeroPad8));
  const size_t suffixLength = 8 + (index * 13) % 64;
  suffix.reserve(suffix.size() + suffixLength);
  for (size_t character = 0; character < suffixLength; ++character) {
    suffix += alphabet[(index * 17 + character * 31) % alphabet.size()];
  }
  if (index % 3 == 0) {
    return "http://www.wikidata.org/entity/Q" + suffix;
  }
  if (index % 3 == 1) {
    return "\"literal value " + suffix + "\"@en";
  }
  return "<http://example.org/property/" + suffix + ">";
}

// _____________________________________________________________________________
// Sink for the checksums of the measured lambdas. The benchmark framework
// discards the return value of a measured lambda, so every lambda stores its
// checksum here instead; the volatile store keeps the optimizer from
// eliminating the looked-up data.
volatile size_t checksumSink = 0;

// _____________________________________________________________________________
// Cheap checksum over the looked-up words. It folds only the size and the
// first and last byte of each word (XOR), so its cost does not dominate the
// timed region.
size_t foldWord(size_t hash, std::string_view word) {
  hash += word.size();
  if (!word.empty()) {
    hash ^=
        (static_cast<size_t>(static_cast<unsigned char>(word.front())) << 8) |
        static_cast<unsigned char>(word.back());
  }
  return hash;
}

// _____________________________________________________________________________
size_t checksumViews(ql::span<const std::string_view> views) {
  size_t hash = 0;
  for (std::string_view view : views) {
    hash = foldWord(hash, view);
  }
  return hash;
}

// _____________________________________________________________________________
size_t checksumStrings(ql::span<const std::string> words) {
  size_t hash = 0;
  for (const std::string& word : words) {
    hash = foldWord(hash, word);
  }
  return hash;
}

// _____________________________________________________________________________
// Synthetic micro-benchmark: build the same batch via the old per-word
// owning-copy path and via the new builders.
class VocabBatchLookupMicroBenchmark : public BenchmarkInterface {
 private:
  // Owns the word bytes for the lifetime of the views below.
  std::vector<std::string> wordsStorage_;
  std::vector<std::string_view> words_;
  std::vector<size_t> sizes_;

 public:
  VocabBatchLookupMicroBenchmark() {
    constexpr size_t numWords = 20'000;
    wordsStorage_.reserve(numWords);
    for (size_t i = 0; i < numWords; ++i) {
      wordsStorage_.push_back(makeSyntheticWord(i));
    }
    words_.assign(wordsStorage_.begin(), wordsStorage_.end());
    sizes_.reserve(words_.size());
    for (std::string_view word : words_) {
      sizes_.push_back(word.size());
    }
  }

  std::string name() const final {
    return "VocabBatchLookup synthetic micro-benchmark";
  }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results;
    auto& group =
        results.addGroup("Assemble 20,000-word batch (mean word length ~74)");
    // Bound the total work even when configured through the environment
    // (high enough for timed measurements of at least one second).
    constexpr size_t maxRepetitions = 100'000;
    const size_t repetitions = parseEnvironmentSize(
        std::getenv("VOCAB_BATCH_MICRO_INNER_REPETITIONS"), 5);
    AD_CONTRACT_CHECK(repetitions > 0 && repetitions <= maxRepetitions);
    // Profiling: run a single variant so a profiler attributes all samples
    // to one code path. The value is one of `baseline`, `contiguous`, `arena`.
    // If the variable is unset or empty, all three measurements run.
    const char* onlyVariant = std::getenv("VOCAB_BATCH_MICRO_ONLY");
    const auto runVariant = [onlyVariant](std::string_view variant) {
      return onlyVariant == nullptr || onlyVariant[0] == '\0' ||
             std::string_view{onlyVariant} == variant;
    };

    // Old API cost model: one owning `std::string` (heap allocation) per word.
    if (runVariant("baseline")) {
      group.addMeasurement("baseline: owning vector<string> per word", [&] {
        size_t checksum = 0;
        for (size_t repetition = 0; repetition < repetitions; ++repetition) {
          std::vector<std::string> copies;
          copies.reserve(words_.size());
          for (std::string_view word : words_) {
            copies.emplace_back(word.data(), word.size());
          }
          checksum += checksumStrings(copies);
        }
        checksumSink = checksum;
      });
    }  // if (runVariant("baseline"))

    // New API: single contiguous buffer, direct memcpy targets for batched I/O.
    if (runVariant("contiguous")) {
      group.addMeasurement("ContiguousVocabBatchBuilder + memcpy", [&] {
        size_t checksum = 0;
        for (size_t repetition = 0; repetition < repetitions; ++repetition) {
          ContiguousVocabBatchBuilder builder{sizes_};
          const auto targets = builder.targets();
          AD_CORRECTNESS_CHECK(targets.size() == words_.size());
          for (size_t i = 0; i < words_.size(); ++i) {
            std::memcpy(targets[i], words_[i].data(), words_[i].size());
          }
          const auto result = std::move(builder).finalize();
          checksum += checksumViews({result.data(), result.size()});
        }
        checksumSink = checksum;
      });
    }  // if (runVariant("contiguous"))

    // New API: PMR monotonic arena, appending the words one by one.
    if (runVariant("arena")) {
      group.addMeasurement("ArenaVocabBatchBuilder::appendWord", [&] {
        size_t checksum = 0;
        for (size_t repetition = 0; repetition < repetitions; ++repetition) {
          ArenaVocabBatchBuilder builder{words_.size()};
          for (std::string_view word : words_) {
            builder.appendWord(word);
          }
          const auto result = std::move(builder).finalize();
          checksum += checksumViews({result.data(), result.size()});
        }
        checksumSink = checksum;
      });
    }  // if (runVariant("arena"))

    return results;
  }
};

AD_REGISTER_BENCHMARK(VocabBatchLookupMicroBenchmark);

// _____________________________________________________________________________
// End-to-end benchmark: resolve a shuffled index batch against a real
// on-heap `VocabularyInMemoryBinSearch` (50,000 words), comparing the old
// per-word `operator[]` loop with a single `lookupBatch` call.
class VocabBatchLookupEndToEndBenchmark : public BenchmarkInterface {
 private:
  // The vocabulary file; it and its `.ids` companion are removed again in the
  // destructor.
  std::string filename_ = "VocabBatchLookupBenchmark.vocab.tmp";
  VocabularyInMemoryBinSearch vocabulary_;
  // Shuffled batch of vocabulary indices, resolved by every measurement.
  std::vector<size_t> batch_;
  bool isSetUp_ = false;

  // `AD_REGISTER_BENCHMARK` constructs the benchmark during static
  // initialization, so the file I/O happens lazily on the first run instead.
  void setUp() {
    if (isSetUp_) {
      return;
    }
    constexpr size_t numWords = 50'000;
    constexpr size_t batchSize = 4'096;
    ad_utility::deleteFile(filename_, false);
    ad_utility::deleteFile(
        absl::StrCat(filename_, VocabularyInMemoryBinSearch::idsSuffix), false);
    {
      // The synthetic words are not sorted. That is fine, because the
      // benchmark only performs lookups by index, never a binary search.
      VocabularyInMemoryBinSearch::WordWriter writer{filename_};
      for (size_t i = 0; i < numWords; ++i) {
        writer(makeSyntheticWord(i), i);
      }
      writer.finish();
    }
    vocabulary_.open(filename_);
    AD_CORRECTNESS_CHECK(vocabulary_.size() == numWords);

    batch_.resize(batchSize);
    std::iota(batch_.begin(), batch_.end(), 0);
    // Shuffle the indices deterministically with a fixed seed, then spread
    // them over the whole vocabulary by multiplying with a prime, so that the
    // batch does not only touch the first 4,096 words.
    std::mt19937_64 randomEngine{42};
    std::shuffle(batch_.begin(), batch_.end(), randomEngine);
    for (size_t& index : batch_) {
      index = (index * 7919) % numWords;
    }

    // Validate the benchmark once: batch lookup must agree with `operator[]`.
    const auto expected = vocabulary_.lookupBatch(
        ql::span<const size_t>{batch_.data(), batch_.size()});
    AD_CORRECTNESS_CHECK(expected.size() == batch_.size());
    for (size_t i = 0; i < batch_.size(); ++i) {
      const auto singleWord = vocabulary_[batch_[i]];
      AD_CORRECTNESS_CHECK(singleWord.has_value());
      AD_CORRECTNESS_CHECK(expected[i] == *singleWord);
    }
    isSetUp_ = true;
  }

 public:
  ~VocabBatchLookupEndToEndBenchmark() override {
    ad_utility::deleteFile(filename_, false);
    ad_utility::deleteFile(
        absl::StrCat(filename_, VocabularyInMemoryBinSearch::idsSuffix), false);
  }

  std::string name() const final {
    return "VocabBatchLookup end-to-end benchmark";
  }

  BenchmarkResults runAllBenchmarks() final {
    setUp();
    BenchmarkResults results;
    auto& group = results.addGroup(
        "Resolve 4,096-word batch from 50,000-word "
        "VocabularyInMemoryBinSearch");
    constexpr size_t maxRepetitions = 100'000;
    const size_t repetitions = parseEnvironmentSize(
        std::getenv("VOCAB_BATCH_E2E_INNER_REPETITIONS"), 10);
    AD_CONTRACT_CHECK(repetitions > 0 && repetitions <= maxRepetitions);
    const ql::span<const size_t> batch{batch_.data(), batch_.size()};

    // Old path: one `operator[]` (binary search + view) plus one owning copy
    // per word.
    group.addMeasurement("per-word operator[] loop", [&] {
      size_t checksum = 0;
      for (size_t repetition = 0; repetition < repetitions; ++repetition) {
        std::vector<std::string> copies;
        copies.reserve(batch.size());
        for (size_t index : batch) {
          const auto word = vocabulary_[index];
          AD_CORRECTNESS_CHECK(word.has_value());
          copies.emplace_back(*word);
        }
        checksum += checksumStrings(copies);
      }
      checksumSink = checksum;
    });

    // New path: a single batched lookup returning an owning result.
    group.addMeasurement("single lookupBatch", [&] {
      size_t checksum = 0;
      for (size_t repetition = 0; repetition < repetitions; ++repetition) {
        const auto result = vocabulary_.lookupBatch(batch);
        checksum += checksumViews({result.data(), result.size()});
      }
      checksumSink = checksum;
    });

    return results;
  }
};

AD_REGISTER_BENCHMARK(VocabBatchLookupEndToEndBenchmark);

}  // namespace
}  // namespace ad_benchmark
