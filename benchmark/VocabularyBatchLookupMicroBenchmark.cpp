// Copyright 2026, The QLever Authors, in particular:
//
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// Synthetic micro-benchmark for the vocabulary `lookupBatch` implementations
// added for `VocabularyOnDisk` and `VocabularyInternalExternal`: small
// vocabulary (4k words), small shuffled batches. Compares repeated
// single-word `operator[]` lookups against one `lookupBatch` call per batch
// size. To read the speedup of a batch size, divide the `single lookups` time
// by the `lookupBatch` time of the same group.

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

// Create `numWords` synthetic words with varying lengths (8 to 72 bytes) that
// resemble short IRIs/literals without requiring a real dataset.
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
// number of resolved bytes (returned, and ultimately printed, so that the
// lookups cannot be optimized away).
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
}  // namespace

class BMVocabBatchLookupMicro : public BenchmarkInterface {
 public:
  std::string name() const final {
    return "Vocabulary lookupBatch micro-benchmark";
  }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results{};
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
    for (size_t batchSize : {128u, 2'048u}) {
      const auto ids = makeQueryIds(words.size(), batchSize, 42);
      auto& onDiskGroup = results.addGroup("VocabularyOnDisk, batch size " +
                                           std::to_string(batchSize));
      onDiskGroup.addMeasurement("single lookups", [&]() {
        checksum += resolveSingly(onDiskVocab, ids);
      });
      onDiskGroup.addMeasurement("lookupBatch", [&]() {
        checksum += resolveBatched(onDiskVocab, ids);
      });
      auto& hybridGroup =
          results.addGroup("VocabularyInternalExternal, batch size " +
                           std::to_string(batchSize));
      hybridGroup.addMeasurement("single lookups", [&]() {
        checksum += resolveSingly(hybridVocab, ids);
      });
      hybridGroup.addMeasurement("lookupBatch", [&]() {
        checksum += resolveBatched(hybridVocab, ids);
      });
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
