// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// Full end-to-end benchmark for `SplitVocabulary::lookupBatch` (PR 78): build
// a realistically sized two-way split vocabulary through the actual
// write-to-disk path, read it back, and resolve a large batch of lookups with
// interleaved markers, duplicates, and reordered indices, comparing sequential
// per-word `operator[]` lookups against the partitioned batch lookup. This is
// complemented by `SplitVocabLookupBatchMicroBenchmark`, which measures
// per-batch costs on tiny synthetic batches.

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

struct EndToEndSplitFunc {
  uint8_t operator()(std::string_view word) const {
    return ql::starts_with(word, "\"a");
  }
};

// Vocabulary 0 is stored under the base filename, vocabulary 1 with suffix
// ".a".
constexpr std::array<std::string_view, 2> endToEndFilenameSuffixes{"", ".a"};

class SplitVocabLookupBatchEndToEndBenchmark : public BenchmarkInterface {
 private:
  using Vocab = SplitVocabulary<EndToEndSplitFunc, endToEndFilenameSuffixes,
                                VocabularyInMemory, VocabularyInMemory>;

  // Remove a whole directory tree. Best effort: failures are ignored.
  struct TempDirCleanup {
    std::filesystem::path dir_;
    ~TempDirCleanup() {
      std::error_code ec;
      std::filesystem::remove_all(dir_, ec);
    }
  };

  // Deterministic Wikidata-like IRIs, every third one routed to marker 1 via
  // the `"a` prefix. Sorted, as the vocabulary writers require sorted input.
  std::vector<std::string> words_;
  // Large batch of global (marker-encoded) indices in non-ascending order
  // with duplicates, mimicking the access pattern of batch ID-to-word
  // resolution during query evaluation.
  std::vector<size_t> batch_;
  TempDirCleanup cleanup_;
  Vocab vocab_;

  // Write the words to disk and read them back into a vocabulary with the
  // given basename.
  Vocab buildVocabulary(const std::string& basename) {
    Vocab vocab;
    {
      auto writerPtr = vocab.makeDiskWriterPtr(basename);
      for (const auto& word : words_) {
        (*writerPtr)(word, false);
      }
      writerPtr->finish();
    }
    vocab.readFromFile(basename);
    return vocab;
  }

 public:
  SplitVocabLookupBatchEndToEndBenchmark() {
    constexpr size_t numWords = 50'000;
    constexpr std::string_view alphabet{
        "abcdefghijklmnopqrstuvwxyz0123456789_:/.-#"};
    words_.reserve(numWords);
    for (size_t i = 0; i < numWords; ++i) {
      std::string word{i % 3 == 0 ? "\"ahttp://www.wikidata.org/entity/Q"
                                  : "\"http://www.wikidata.org/entity/Q"};
      word += std::to_string(i);
      word += '/';
      for (size_t character = 0; character < 32; ++character) {
        word += alphabet[(i * 17 + character * 31) % alphabet.size()];
      }
      word += '"';
      words_.push_back(std::move(word));
    }
    std::sort(words_.begin(), words_.end());

    cleanup_.dir_ = std::filesystem::temp_directory_path() /
                    "splitVocabLookupBatchEndToEnd";
    std::error_code ec;
    std::filesystem::remove_all(cleanup_.dir_, ec);
    std::filesystem::create_directories(cleanup_.dir_, ec);
    vocab_ = buildVocabulary((cleanup_.dir_ / "vocab").string());

    std::vector<size_t> marked;
    for (const IndexAndWord& indexAndWord : vocab_.scanAll()) {
      marked.push_back(static_cast<size_t>(indexAndWord.index_));
    }
    constexpr size_t batchSize = 100'000;
    batch_.reserve(batchSize);
    for (size_t i = 0; i < batchSize; ++i) {
      batch_.push_back(marked[(i * 2654435761u) % marked.size()]);
    }
  }

  std::string name() const final {
    return "SplitVocabulary::lookupBatch (end to end)";
  }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results;
    auto& group = results.addGroup(
        "End to end: 50,000 words in two markers, 100,000 lookups per batch");
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
        parseEnvironmentSize(std::getenv("SPLIT_VOCAB_E2E_REPETITIONS"), 5);
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
    // The generic fallback that `SplitVocabulary::lookupBatch` used before it
    // partitioned by marker (one owning `std::string` per word).
    group.addMeasurement("fallback sequentialLookupBatch", [&] {
      size_t totalBytes = 0;
      for (size_t repetition = 0; repetition < repetitions; ++repetition) {
        auto result =
            ad_utility::vocabulary::sequentialLookupBatch(vocab_, batch_);
        for (const auto& word : result) {
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
    return results;
  }
};

AD_REGISTER_BENCHMARK(SplitVocabLookupBatchEndToEndBenchmark);

}  // namespace
}  // namespace ad_benchmark
