// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/strings/str_cat.h>

#include <algorithm>
#include <array>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "../benchmark/infrastructure/Benchmark.h"
#include "backports/span.h"
#include "index/vocabulary/PrefixCompressor.h"

namespace ad_benchmark {
namespace {

// `measureTimeOfFunction` discards the return value of a measurement, so every
// measurement stores its result here. Every measurement additionally passes
// each decoded buffer through `keepDecodedBytes`, so the compiler cannot drop
// writes to buffers that are otherwise never read.
volatile size_t benchmarkSink = 0;

// Tell the compiler that the memory behind `decoded` may be read elsewhere,
// so that none of the decoded bytes can be optimized away (the sink only
// receives byte counts).
inline void keepDecodedBytes(const char* decoded) {
  asm volatile("" : : "g"(decoded) : "memory");
}

// Benchmark the in-place `PrefixCompressor::decompressInto` API against the
// allocating `PrefixCompressor::decompress` API. The micro group decodes the
// benchmark words one at a time, each into an owning string or into one reused
// buffer. The end-to-end group decodes all benchmark words in one batch, either
// into owning strings or into a single preallocated arena.
class PrefixCompressorBenchmark : public BenchmarkInterface {
 private:
  // The number of benchmark words and the number of passes over them.
  static constexpr size_t numWords_ = 5'000;
  static constexpr size_t numRepetitions_ = 5;

  PrefixCompressor compressor_;
  std::vector<std::string> compressed_;
  // Maximum exact decompressed size across the benchmark inputs; sizes the
  // reused output buffer of the micro group.
  size_t outputCapacity_ = 0;
  // Sum of the exact decompressed sizes; sizes the end-to-end arena.
  size_t totalDecompressedSize_ = 0;
  // Exact decompressed size of every word, computed once here so that the
  // timed arena loop does not recompute it per word.
  std::vector<size_t> decompressedSizes_;

 public:
  // The corpus mixes IRIs of all codebook prefixes with incompressible plain
  // literals (every fourth word), so that both the prefix and the no-prefix
  // paths are exercised and the decompressed sizes vary.
  PrefixCompressorBenchmark() {
    const std::array<std::string, 6> prefixes{
        "http://www.wikidata.org/entity/",
        "http://www.wikidata.org/prop/direct/",
        "http://www.wikidata.org/value/",
        "http://schema.org/",
        "http://www.w3.org/2000/01/rdf-schema#",
        "<http://example.org/property/"};
    compressor_.buildCodebook(prefixes);
    constexpr std::string_view alphabet{
        "abcdefghijklmnopqrstuvwxyz0123456789_:/.-#"};
    constexpr size_t suffixLength = 45;
    compressed_.reserve(numWords_);
    for (size_t i = 0; i < numWords_; ++i) {
      std::string suffix;
      suffix.reserve(suffixLength);
      for (size_t character = 0; character < suffixLength; ++character) {
        // The `i / alphabet.size()` term keeps the suffixes from repeating
        // with the period of the alphabet.
        suffix += alphabet[(i * 17 + character * 31 +
                            (i / alphabet.size()) * (character + 1)) %
                           alphabet.size()];
      }
      const std::string word = (i % 4 == 3)
                                   ? "literal-" + suffix
                                   : prefixes[i % prefixes.size()] + suffix;
      compressed_.push_back(compressor_.compress(word));
    }
    decompressedSizes_.reserve(compressed_.size());
    for (const std::string& compressed : compressed_) {
      const size_t decompressedSize =
          compressor_.maxDecompressedSize(compressed);
      decompressedSizes_.push_back(decompressedSize);
      outputCapacity_ = std::max(outputCapacity_, decompressedSize);
      totalDecompressedSize_ += decompressedSize;
    }
  }

  std::string name() const final {
    return "PrefixCompressor in-place decompression";
  }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results;
    const size_t repetitions = numRepetitions_;

    // Micro benchmark: the allocating API vs the in-place API with a reused
    // buffer, one word at a time.
    auto& micro = results.addGroup(absl::StrCat(
        "Single-word PrefixCompressor decode (", numWords_, " words)"));
    micro.addMeasurement("decompress (allocating)", [&] {
      size_t totalBytes = 0;
      for (size_t repetition = 0; repetition < repetitions; ++repetition) {
        for (const auto& compressed : compressed_) {
          const std::string decoded = compressor_.decompress(compressed);
          keepDecodedBytes(decoded.data());
          totalBytes += decoded.size();
        }
      }
      benchmarkSink = totalBytes;
      return totalBytes;
    });
    micro.addMeasurement("decompressInto (reused buffer)", [&] {
      size_t totalBytes = 0;
      std::string output(outputCapacity_, '\0');
      for (size_t repetition = 0; repetition < repetitions; ++repetition) {
        for (const auto& compressed : compressed_) {
          const size_t written = compressor_.decompressInto(
              compressed, ql::span<char>{output.data(), output.size()});
          keepDecodedBytes(output.data());
          totalBytes += written;
        }
      }
      benchmarkSink = totalBytes;
      return totalBytes;
    });

    // Validate the in-place API once, outside the timed regions: every word
    // decodes to the same bytes as `decompress` and to its exact size.
    {
      std::string output(outputCapacity_, '\0');
      for (size_t i = 0; i < compressed_.size(); ++i) {
        const size_t written = compressor_.decompressInto(
            compressed_[i], ql::span<char>{output.data(), output.size()});
        AD_CORRECTNESS_CHECK(written == decompressedSizes_[i]);
        AD_CORRECTNESS_CHECK(std::string_view(output.data(), written) ==
                             compressor_.decompress(compressed_[i]));
      }
    }

    // End-to-end benchmark: batch resolution of all words, either into
    // owning strings or into a single preallocated arena.
    auto& endToEnd = results.addGroup(absl::StrCat(
        "Batch PrefixCompressor resolution (", numWords_, " words)"));
    endToEnd.addMeasurement("batch decode into vector<string>", [&] {
      size_t totalBytes = 0;
      for (size_t repetition = 0; repetition < repetitions; ++repetition) {
        std::vector<std::string> decoded;
        decoded.reserve(compressed_.size());
        for (const auto& compressed : compressed_) {
          decoded.push_back(compressor_.decompress(compressed));
          keepDecodedBytes(decoded.back().data());
          totalBytes += decoded.back().size();
        }
      }
      benchmarkSink = totalBytes;
      return totalBytes;
    });
    endToEnd.addMeasurement("batch decodeInto contiguous arena", [&] {
      size_t totalBytes = 0;
      auto arena = std::make_unique<char[]>(totalDecompressedSize_);
      for (size_t repetition = 0; repetition < repetitions; ++repetition) {
        size_t offset = 0;
        for (size_t i = 0; i < compressed_.size(); ++i) {
          // The span is exactly the word's size, as a batch lookup that has
          // sized its arena from `maxDecompressedSize` would pass it.
          const size_t written = compressor_.decompressInto(
              compressed_[i],
              ql::span<char>{arena.get() + offset, decompressedSizes_[i]});
          keepDecodedBytes(arena.get() + offset);
          totalBytes += written;
          offset += written;
        }
        AD_CORRECTNESS_CHECK(offset == totalDecompressedSize_);
      }
      benchmarkSink = totalBytes;
      return totalBytes;
    });

    return results;
  }
};

AD_REGISTER_BENCHMARK(PrefixCompressorBenchmark);

}  // namespace
}  // namespace ad_benchmark
