// Copyright 2026, The QLever Authors, in particular:
//
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// Synthetic micro-benchmark for `lookupBatch` of `VocabularyOnDisk` and
// `VocabularyInternalExternal`: small vocabulary (4k words, files in the page
// cache), small shuffled batches. Compares single-word `operator[]` lookups
// with one `lookupBatch` call per batch.
//
// Every measurement repeats its batch until at least
// `VOCAB_LOOKUP_MIN_SECONDS` (default 10) seconds have passed; the reported
// time is that of the whole measurement, the time per word is in the
// measurement's metadata. So a measurement is never a single sub-millisecond
// sample. Environment knobs (all optional):
//   VOCAB_LOOKUP_MIN_SECONDS  minimum duration of one measurement (seconds).
//   VOCAB_LOOKUP_ONLY         run only the group with this id (`ondisk-128`,
//                             `hybrid-128`, `ondisk-2048`, `hybrid-2048`).
//   VOCAB_LOOKUP_ORDER        `batch-first` measures `lookupBatch` first.
// Every measurement also prints one tab-separated line
// `VOCAB_LOOKUP <group> <measurement> <ns/word> <batches> <seconds>` for
// scripts.

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <numeric>
#include <random>
#include <string>
#include <vector>

#include "../benchmark/infrastructure/Benchmark.h"
#include "../benchmark/infrastructure/BenchmarkMeasurementContainer.h"
#include "index/vocabulary/VocabularyInternalExternal.h"
#include "index/vocabulary/VocabularyOnDisk.h"

namespace ad_benchmark {
namespace {

// Create `numWords` synthetic words with varying lengths that resemble short
// IRIs/literals without requiring a real dataset.
std::vector<std::string> makeWords(size_t numWords) {
  std::vector<std::string> words;
  words.reserve(numWords);
  for (size_t i = 0; i < numWords; ++i) {
    std::string word = "<http://example.org/entity/" + std::to_string(i) + ">";
    // Vary the length so that batches cover small and large words.
    word.append(i % 53, 'x');
    words.push_back(std::move(word));
  }
  return words;
}

// Deterministic shuffled query ids in `[0, vocabSize)`.
std::vector<size_t> makeQueryIds(size_t vocabSize, size_t numQueries,
                                 uint32_t seed) {
  std::vector<size_t> ids(vocabSize);
  std::iota(ids.begin(), ids.end(), size_t{0});
  std::shuffle(ids.begin(), ids.end(), std::mt19937{seed});
  ids.resize(numQueries);
  return ids;
}

// Write `words` to a `VocabularyOnDisk` at `filename` and open it.
VocabularyOnDisk buildOnDiskVocabulary(const std::string& filename,
                                       const std::vector<std::string>& words) {
  {
    VocabularyOnDisk::WordWriter writer(filename);
    for (const auto& word : words) {
      writer(word, false);
    }
    writer.finish();
  }
  VocabularyOnDisk vocab;
  vocab.open(filename);
  return vocab;
}

// Write `words` to a hybrid `VocabularyInternalExternal` at `filename`: every
// second word is disk-only, the rest is additionally cached in RAM (plus the
// regular milestones).
VocabularyInternalExternal buildHybridVocabulary(
    const std::string& filename, const std::vector<std::string>& words) {
  {
    auto writerPtr = VocabularyInternalExternal::makeDiskWriterPtr(filename);
    for (size_t i = 0; i < words.size(); ++i) {
      (*writerPtr)(words[i], i % 2 == 0);
    }
    writerPtr->finish();
  }
  VocabularyInternalExternal vocab;
  vocab.open(filename);
  return vocab;
}

// Resolve all `ids` one word at a time via `operator[]` and return the total
// number of resolved bytes (so that the lookups cannot be optimized away).
template <typename Vocab>
size_t resolveSingly(const Vocab& vocab, const std::vector<size_t>& ids) {
  size_t totalBytes = 0;
  for (size_t idx : ids) {
    totalBytes += vocab[idx].size();
  }
  return totalBytes;
}

// Resolve all `ids` with a single `lookupBatch` call and return the total
// number of resolved bytes.
template <typename Vocab>
size_t resolveBatched(const Vocab& vocab, const std::vector<size_t>& ids) {
  size_t totalBytes = 0;
  auto result = vocab.lookupBatch(ids);
  for (std::string_view word : result) {
    totalBytes += word.size();
  }
  return totalBytes;
}

// Result of `measureLong`.
struct LongMeasurement {
  size_t numBatches = 0;
  double seconds = 0;
  double nsPerWord = 0;
};

// Repeat `resolveOneBatch` (which resolves `batchSize` words) until at least
// `minSeconds` have passed, and print the script line.
template <typename F>
LongMeasurement measureLong(const std::string& group,
                            const std::string& measurement, size_t batchSize,
                            double minSeconds, size_t& checksum,
                            const F& resolveOneBatch) {
  using Clock = std::chrono::steady_clock;
  LongMeasurement result;
  const auto start = Clock::now();
  do {
    checksum += resolveOneBatch();
    ++result.numBatches;
    result.seconds =
        std::chrono::duration<double>(Clock::now() - start).count();
  } while (result.seconds < minSeconds);
  result.nsPerWord =
      result.seconds * 1e9 / static_cast<double>(result.numBatches * batchSize);
  std::cout << "VOCAB_LOOKUP\t" << group << '\t' << measurement << '\t'
            << result.nsPerWord << '\t' << result.numBatches << '\t'
            << result.seconds << std::endl;
  return result;
}

// Minimum duration of one measurement: `VOCAB_LOOKUP_MIN_SECONDS`, default 10.
double minSecondsFromEnvironment() {
  const char* value = std::getenv("VOCAB_LOOKUP_MIN_SECONDS");
  return value != nullptr ? std::stod(value) : 10.0;
}
}  // namespace

class BMVocabBatchLookupMicro : public BenchmarkInterface {
 public:
  std::string name() const final {
    return "Vocabulary lookupBatch micro-benchmark";
  }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results{};
    const double minSeconds = minSecondsFromEnvironment();
    const char* onlyValue = std::getenv("VOCAB_LOOKUP_ONLY");
    const std::string only = onlyValue != nullptr ? onlyValue : "";
    const char* orderValue = std::getenv("VOCAB_LOOKUP_ORDER");
    const bool batchFirst =
        orderValue != nullptr && std::string{orderValue} == "batch-first";

    const auto benchmarkDir =
        std::filesystem::temp_directory_path() / "qleverVocabBatchMicro";
    std::filesystem::remove_all(benchmarkDir);
    std::filesystem::create_directories(benchmarkDir);

    const auto words = makeWords(4'096);
    auto onDiskVocab =
        buildOnDiskVocabulary((benchmarkDir / "micro.vocab").string(), words);
    auto hybridVocab =
        buildHybridVocabulary((benchmarkDir / "micro.hybrid").string(), words);

    size_t checksum = 0;
    // Add the two measurements (single lookups, `lookupBatch`) of one group.
    auto runGroup = [&](const std::string& id, const std::string& title,
                        const auto& vocab, const std::vector<size_t>& ids) {
      if (!only.empty() && only != id) {
        return;
      }
      auto& group = results.addGroup(title);
      group.metadata().addKeyValuePair("min-seconds-per-measurement",
                                       minSeconds);
      auto measure = [&](const std::string& name, const auto& resolve) {
        LongMeasurement m;
        auto& entry = group.addMeasurement(name, [&]() {
          m = measureLong(id, name, ids.size(), minSeconds, checksum, resolve);
        });
        entry.metadata().addKeyValuePair("ns-per-word", m.nsPerWord);
        entry.metadata().addKeyValuePair("batches", m.numBatches);
      };
      auto single = [&]() {
        measure("single lookups", [&]() { return resolveSingly(vocab, ids); });
      };
      auto batched = [&]() {
        measure("lookupBatch", [&]() { return resolveBatched(vocab, ids); });
      };
      if (batchFirst) {
        batched();
        single();
      } else {
        single();
        batched();
      }
    };
    for (size_t batchSize : {128u, 2'048u}) {
      const auto ids = makeQueryIds(words.size(), batchSize, 42);
      const auto size = std::to_string(batchSize);
      runGroup("ondisk-" + size, "VocabularyOnDisk, batch size " + size,
               onDiskVocab, ids);
      runGroup("hybrid-" + size,
               "VocabularyInternalExternal, batch size " + size, hybridVocab,
               ids);
    }
    // Print the checksum so that the measured lookups cannot be optimized
    // away (see the same pattern in `BenchmarkExamples.cpp`).
    std::cout << "micro-benchmark checksum: " << checksum << '\n';

    std::filesystem::remove_all(benchmarkDir);
    return results;
  }
};

AD_REGISTER_BENCHMARK(BMVocabBatchLookupMicro);
}  // namespace ad_benchmark
