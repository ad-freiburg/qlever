// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "../benchmark/infrastructure/Benchmark.h"
#include "backports/span.h"
#include "index/vocabulary/PrefixCompressor.h"

namespace ad_benchmark {
namespace {

// Benchmark the in-place `PrefixCompressor::decompressInto` API against the
// allocating `PrefixCompressor::decompress` API. The micro group measures
// per-word decode latency; the end-to-end group measures batch vocabulary
// resolution of all benchmark words, either into owning strings or into a
// single preallocated arena (the pattern used by batch vocabulary lookups).
class PrefixCompressorBenchmark : public BenchmarkInterface {
 private:
  PrefixCompressor compressor_;
  // Owning storage for the compressed words; `compressed_` views into it.
  std::vector<std::string> compressedStorage_;
  std::vector<std::string_view> compressed_;
  // Maximum exact decompressed size across the benchmark inputs; sizes the
  // reused output buffer and the end-to-end arena.
  size_t outputCapacity_ = 0;
  // Sum of the exact decompressed sizes; sizes the end-to-end arena.
  size_t totalDecompressedSize_ = 0;

 public:
  PrefixCompressorBenchmark() {
    compressor_.buildCodebook(std::vector<std::string>{
        "http://www.wikidata.org/entity/",
        "http://www.wikidata.org/prop/direct/",
        "http://www.wikidata.org/value/", "http://schema.org/",
        "http://www.w3.org/2000/01/rdf-schema#",
        "<http://example.org/property/"});
    constexpr std::string_view alphabet{
        "abcdefghijklmnopqrstuvwxyz0123456789_:/.-#"};
    compressedStorage_.reserve(5'000);
    for (size_t i = 0; i < 5'000; ++i) {
      std::string suffix;
      suffix.reserve(45);
      for (size_t character = 0; character < 45; ++character) {
        suffix += alphabet[(i * 17 + character * 31) % alphabet.size()];
      }
      // Mix prefix-compressible IRIs with incompressible plain literals so
      // that both the prefix and the no-prefix paths are exercised.
      const std::string word =
          (i % 4 == 3) ? "literal-" + suffix
                       : "http://www.wikidata.org/entity/Q" + suffix;
      compressedStorage_.push_back(compressor_.compress(word));
    }
    compressed_.assign(compressedStorage_.begin(), compressedStorage_.end());
    for (const std::string_view& compressed : compressed_) {
      const size_t size = compressor_.maxDecompressedSize(compressed);
      outputCapacity_ = std::max(outputCapacity_, size);
      totalDecompressedSize_ += size;
    }
  }

  std::string name() const final {
    return "PrefixCompressor in-place decompression";
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
    // Bound the benchmark workload even when configured through the
    // environment.
    constexpr size_t maxRepetitions = 1'000'000;
    const size_t repetitions = parseEnvironmentSize(
        std::getenv("PREFIX_INPLACE_INNER_REPETITIONS"), 5);
    AD_CONTRACT_CHECK(repetitions > 0);
    AD_CONTRACT_CHECK(repetitions <= maxRepetitions);

    // Micro benchmark: per-word decode latency of the allocating API vs the
    // in-place API with a reused buffer.
    auto& micro =
        results.addGroup("Single-word PrefixCompressor decode (5,000 words)");
    micro.addMeasurement("decompress (allocating)", [&] {
      size_t totalBytes = 0;
      for (size_t repetition = 0; repetition < repetitions; ++repetition) {
        for (const auto& compressed : compressed_) {
          totalBytes += compressor_.decompress(compressed).size();
        }
      }
      return totalBytes;
    });
    micro.addMeasurement("decompressInto (reused buffer)", [&] {
      size_t totalBytes = 0;
      std::string output(outputCapacity_, '\0');
      for (size_t repetition = 0; repetition < repetitions; ++repetition) {
        for (const auto& compressed : compressed_) {
          totalBytes += compressor_.decompressInto(
              compressed, ql::span<char>{output.data(), output.size()});
        }
      }
      return totalBytes;
    });

    // End-to-end benchmark: batch resolution of all words, either into
    // owning strings or into a single preallocated arena.
    auto& endToEnd =
        results.addGroup("Batch PrefixCompressor resolution (5,000 words)");
    endToEnd.addMeasurement("batch decode into vector<string>", [&] {
      size_t totalBytes = 0;
      for (size_t repetition = 0; repetition < repetitions; ++repetition) {
        std::vector<std::string> decoded;
        decoded.reserve(compressed_.size());
        for (const auto& compressed : compressed_) {
          decoded.push_back(compressor_.decompress(compressed));
          totalBytes += decoded.back().size();
        }
      }
      return totalBytes;
    });
    endToEnd.addMeasurement("batch decodeInto contiguous arena", [&] {
      size_t totalBytes = 0;
      auto arena = std::make_unique<char[]>(totalDecompressedSize_);
      for (size_t repetition = 0; repetition < repetitions; ++repetition) {
        size_t offset = 0;
        for (const auto& compressed : compressed_) {
          const size_t size = compressor_.maxDecompressedSize(compressed);
          const size_t written = compressor_.decompressInto(
              compressed, ql::span<char>{arena.get() + offset,
                                         totalDecompressedSize_ - offset});
          AD_CORRECTNESS_CHECK(written == size);
          offset += written;
          totalBytes += written;
        }
        AD_CORRECTNESS_CHECK(offset == totalDecompressedSize_);
      }
      return totalBytes;
    });

    return results;
  }
};

AD_REGISTER_BENCHMARK(PrefixCompressorBenchmark);

}  // namespace
}  // namespace ad_benchmark
