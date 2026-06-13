// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <boost/program_options.hpp>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <random>

#include "backports/algorithm.h"
#include "global/Constants.h"
#include "index/Vocabulary.h"
#include "index/vocabulary/VocabularyType.h"
#include "index/vocabulary/VocabularyTypes.h"
#include "util/File.h"
#include "util/Iterators.h"
#include "util/json.h"

namespace po = boost::program_options;

int main(int argc, char** argv) {
  std::string indexBasename;
  size_t numLookups = 1'000'000;
  size_t seed = 42;
  size_t batchSize = 0;
  bool singleLookup = false;
  bool streamedLookup = false;
  bool sorted = false;

  po::options_description options("Vocabulary batch lookup benchmark");
  auto add = [&options](auto&&... args) {
    options.add_options()(AD_FWD(args)...);
  };
  add("help,h", "Show help message");
  add("index-basename,i", po::value<std::string>(&indexBasename)->required(),
      "Path to the index basename");
  add("num-lookups,n", po::value<size_t>(&numLookups)->default_value(1'000'000),
      "Number of random indices to look up");
  add("seed,s", po::value<size_t>(&seed)->default_value(42),
      "Deterministic seed for RNG");
  add("batch-size,b", po::value<size_t>(&batchSize)->default_value(0),
      "Size of each batch (0 = one big batch)");
  add("single", po::bool_switch(&singleLookup),
      "Look up each index individually via operator[] instead of batch lookup");
  add("streamed", po::bool_switch(&streamedLookup),
      "Use lookupBatchesStreamed (pipelined I/O) instead of per-batch "
      "lookupBatch");
  add("sorted", po::bool_switch(&sorted),
      "Sort the generated indices ascending before looking them up. This "
      "matches the production case, where the input to lookupBatch is always "
      "sorted (and unique). Without this flag the indices are uniform random "
      "and unsorted, which is a worst-case bound only.");

  po::positional_options_description positional;
  positional.add("index-basename", 1);

  po::variables_map optionsMap;
  try {
    po::store(po::command_line_parser(argc, argv)
                  .options(options)
                  .positional(positional)
                  .run(),
              optionsMap);
    if (optionsMap.count("help")) {
      std::cout << options << std::endl;
      return 0;
    }
    po::notify(optionsMap);
  } catch (const po::error& e) {
    std::cerr << "Error: " << e.what() << "\n\n" << options << std::endl;
    return 1;
  }

  if (batchSize == 0) {
    batchSize = numLookups;
  }

  // Read meta-data.json to get vocabulary type and locale.
  std::string configFile = absl::StrCat(indexBasename, CONFIGURATION_FILE);
  nlohmann::json configJson;
  {
    auto f = ad_utility::makeIfstream(configFile);
    f >> configJson;
  }

  // Extract locale settings.
  std::string lang = "en";
  std::string country = "US";
  bool ignorePunctuation = false;
  if (configJson.count("locale")) {
    lang = std::string{configJson["locale"]["language"]};
    country = std::string{configJson["locale"]["country"]};
    ignorePunctuation = bool{configJson["locale"]["ignore-punctuation"]};
  }

  // Extract vocabulary type.
  ad_utility::VocabularyType vocabType(
      ad_utility::VocabularyType::Enum::OnDiskCompressed);
  if (configJson.count("vocabulary-type")) {
    vocabType =
        static_cast<ad_utility::VocabularyType>(configJson["vocabulary-type"]);
  }

  // Initialize vocabulary.
  RdfsVocabulary vocab;
  vocab.resetToType(vocabType);
  vocab.setLocale(lang, country, ignorePunctuation);
  vocab.readFromFile(absl::StrCat(indexBasename, VOCAB_SUFFIX));

  std::cout << "Vocabulary size: " << vocab.size() << std::endl;
  std::string mode =
      singleLookup ? "single" : (streamedLookup ? "streamed" : "batch");
  std::cout << "Num lookups: " << numLookups << ", mode: " << mode
            << (singleLookup ? "" : absl::StrCat(", batch size: ", batchSize))
            << ", seed: " << seed << ", sorted: " << (sorted ? "yes" : "no")
            << std::endl;

  // Generate random indices.
  std::mt19937 rng(seed);
  std::uniform_int_distribution<size_t> dist(0, vocab.size() - 1);
  std::vector<size_t> allIndices(numLookups);
  for (auto& idx : allIndices) {
    idx = dist(rng);
  }

  // In production, each batch handed to `lookupBatch` is sorted ascending on
  // its own (the row-batch is sorted by VocabIndex before lookup), but
  // consecutive batches are independent and not globally ordered. The
  // `--sorted` flag reproduces this by sorting each batch-sized slice
  // separately; the default (unsorted, uniform random) is a worst-case bound
  // only.
  if (sorted) {
    for (size_t offset = 0; offset < allIndices.size(); offset += batchSize) {
      size_t currentBatchSize = std::min(batchSize, allIndices.size() - offset);
      ql::ranges::sort(
          ql::span<size_t>(allIndices.data() + offset, currentBatchSize));
    }
  }

  using Clock = std::chrono::high_resolution_clock;
  auto start = Clock::now();

  size_t totalStringBytes = 0;

  if (singleLookup) {
    // Look up each index individually via operator[].
    for (size_t idx : allIndices) {
      auto word = vocab[VocabIndex::make(idx)];
      totalStringBytes += word.size();
      // Prevent the compiler from optimizing away the lookup.
      asm volatile("" ::"r,m"(word.data()) : "memory");
    }
  } else if (streamedLookup) {
    // Streamed (pipelined) batch lookup. (Not implemented as a coroutine,
    // because coroutines are not available in the C++17 compatibility build.)
    auto makeBatches = ad_utility::InputRangeFromGetCallable(
        [&allIndices, batchSize,
         offset = size_t{0}]() mutable -> std::optional<std::vector<size_t>> {
          if (offset >= allIndices.size()) {
            return std::nullopt;
          }
          size_t currentBatchSize =
              std::min(batchSize, allIndices.size() - offset);
          std::vector<size_t> batch(
              allIndices.begin() + offset,
              allIndices.begin() + offset + currentBatchSize);
          offset += currentBatchSize;
          return batch;
        });
    auto results =
        vocab.lookupBatchesStreamed(VocabLookupInput{std::move(makeBatches)});
    for (auto& result : results) {
      for (const auto& sv : *result) {
        totalStringBytes += sv.size();
      }
    }
  } else {
    // Batch lookup.
    for (size_t offset = 0; offset < allIndices.size(); offset += batchSize) {
      size_t currentBatchSize = std::min(batchSize, allIndices.size() - offset);
      ql::span<const size_t> batch(allIndices.data() + offset,
                                   currentBatchSize);
      auto result = vocab.lookupBatch(batch);
      for (const auto& sv : *result) {
        totalStringBytes += sv.size();
      }
    }
  }

  auto end = Clock::now();
  double elapsedMs =
      std::chrono::duration<double, std::milli>(end - start).count();
  double elapsedS = elapsedMs / 1000.0;
  double throughput = static_cast<double>(numLookups) / elapsedS;
  double totalMB = static_cast<double>(totalStringBytes) / (1024.0 * 1024.0);
  double mbPerSec = totalMB / elapsedS;

  std::cout << "Total time: " << elapsedMs << " ms" << std::endl;
  std::cout << "Throughput: " << static_cast<size_t>(throughput)
            << " lookups/sec" << std::endl;
  std::cout << "Total string data: " << totalStringBytes << " bytes ("
            << std::fixed << std::setprecision(2) << totalMB << " MB)"
            << std::endl;
  std::cout << "Reading speed: " << std::fixed << std::setprecision(2)
            << mbPerSec << " MB/s" << std::endl;

  return 0;
}
