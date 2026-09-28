// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// Compare the decode APIs of `FsstRepeatedDecoder` on the same workload: every
// arm decodes the same 5,000 compressed words through the same three FSST
// stages, `FSST_BENCH_REPETITIONS` times. The arms differ only in where the
// decoded bytes go (owning strings or caller-provided buffers) and how the
// scratch buffer is provided. Environment variables:
//   FSST_BENCH_ARM          run only this arm (0..6, default: all arms)
//   FSST_BENCH_REPETITIONS  passes over the 5,000 words (default 1)
// One untimed warm-up pass runs before each timed measurement. The
// constructor checks that all arms decode every word to the original bytes.

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "../benchmark/infrastructure/Benchmark.h"
#include "backports/span.h"
#include "util/FsstCompressor.h"

namespace ad_benchmark {
namespace {

constexpr size_t numberOfStages = 3;
using RepeatedDecoder = FsstRepeatedDecoder<numberOfStages>;
using Decoders = std::array<FsstDecoder, numberOfStages>;

// _____________________________________________________________________________
// The body of `FsstDecoder::decompress` before `decompressInto` existed: size
// an owning string to the 8x bound with `resize` (which zero-fills it),
// decode, shrink. `decompressInto` performs the same `fsst_decompress` call
// plus two bound checks.
std::string legacyDecompress(const FsstDecoder& decoder, std::string_view str) {
  std::string output;
  output.resize(FsstDecoder::maxExpansionFactor * str.size());
  const size_t size =
      decoder.decompressInto(str, ql::span<char>{output.data(), output.size()});
  output.resize(size);
  return output;
}

// _____________________________________________________________________________
// `FsstRepeatedDecoder::decompress` before `decompressInto` existed: one
// owning string per stage, each built by `legacyDecompress`.
std::string legacyRepeatedDecompress(const Decoders& decoders,
                                     std::string_view str) {
  std::string current = legacyDecompress(decoders[numberOfStages - 1], str);
  for (size_t stage = 1; stage < numberOfStages; ++stage) {
    current = legacyDecompress(decoders[numberOfStages - 1 - stage], current);
  }
  return current;
}

// _____________________________________________________________________________
// Decode through all stages into `output`, alternating with `scratch` (the
// parity scheme of `FsstRepeatedDecoder::decompressInto`), with caller-provided
// spans so that the scratch provisioning can be varied.
size_t decodeRepeatedIntoSpans(const Decoders& decoders,
                               std::string_view compressed,
                               ql::span<char> output, ql::span<char> scratch) {
  size_t destination = (numberOfStages % 2 == 0) ? 1 : 0;
  std::array<ql::span<char>, 2> buffers{output, scratch};
  std::string_view input = compressed;
  size_t bytesWritten = 0;
  for (size_t stage = 0; stage < numberOfStages; ++stage) {
    bytesWritten = decoders[numberOfStages - 1 - stage].decompressInto(
        input, buffers[destination]);
    input = {buffers[destination].data(), bytesWritten};
    destination ^= 1;
  }
  return bytesWritten;
}

// _____________________________________________________________________________
// Allocation statistics of the calling thread, read from jemalloc (which
// QLever links when it is installed) via `mallctl`. The symbol is declared
// weak, so without jemalloc `available()` is false and no statistics are
// recorded. All reads happen outside the timed region.
extern "C" int mallctl(const char* name, void* oldp, size_t* oldlenp,
                       void* newp, size_t newlen) __attribute__((weak));

class JemallocThreadStats {
 public:
  struct Snapshot {
    uint64_t requests = 0;  // Number of allocation requests (all threads).
    uint64_t allocatedBytes = 0;  // Bytes allocated by this thread.
  };

  static bool available() { return mallctl != nullptr; }

  // Flush this thread's cache so that the arena counters include all its
  // requests, then read the counters.
  static Snapshot read() {
    Snapshot snapshot;
    if (!available()) {
      return snapshot;
    }
    mallctl("thread.tcache.flush", nullptr, nullptr, nullptr, 0);
    uint64_t epoch = 1;
    size_t length = sizeof(epoch);
    mallctl("epoch", &epoch, &length, &epoch, length);
    snapshot.requests = readUint64("stats.arenas.4096.small.nrequests") +
                        readUint64("stats.arenas.4096.large.nrequests");
    snapshot.allocatedBytes = readUint64("thread.allocated");
    return snapshot;
  }

  // Reset and read the high-water mark of this thread's live heap bytes.
  static void resetPeak() {
    if (available()) {
      mallctl("thread.peak.reset", nullptr, nullptr, nullptr, 0);
    }
  }
  static uint64_t peakBytes() {
    return available() ? readUint64("thread.peak.read") : 0;
  }

 private:
  static uint64_t readUint64(const char* name) {
    uint64_t value = 0;
    size_t length = sizeof(value);
    return mallctl(name, &value, &length, nullptr, 0) == 0 ? value : 0;
  }
};

// _____________________________________________________________________________
size_t parseEnvironmentSize(const char* varName, size_t defaultValue) {
  const char* value = std::getenv(varName);
  if (value == nullptr) {
    return defaultValue;
  }
  errno = 0;
  char* end = nullptr;
  const unsigned long parsed = std::strtoul(value, &end, 10);
  AD_CONTRACT_CHECK(end != value && *end == '\0' && errno != ERANGE,
                    "Invalid value `", value, "` for environment variable ",
                    varName);
  return static_cast<size_t>(parsed);
}

class FsstScratchBufferBenchmark : public BenchmarkInterface {
 private:
  static constexpr size_t numberOfWords = 5'000;
  static constexpr size_t allArms = 7;
  // Views into the three times compressed words.
  std::vector<std::string_view> compressed_;
  // Owns the original words, which the first `compressAll` call reads.
  std::vector<std::string> wordsStorage_;
  Decoders decoders_;
  RepeatedDecoder repeatedDecoder_;
  // Owns the compressed strings behind the views in `compressed_`.
  std::vector<std::shared_ptr<std::string>> decoderStorage_;
  // Largest `RepeatedDecoder::maxDecompressedSize` over all words: sizes the
  // output buffer of the `decompressInto` arms.
  size_t outputCapacity_ = 0;
  // Scratch size of the stage-aware strategy: the largest intermediate stage
  // bound, `outputCapacity_ / 8`.
  size_t intermediateCapacity_ = 0;
  // Largest sum of the requested sizes of the owning strings that are alive
  // at the same time in the pre-API `decompress` (the previous and the
  // current stage's string), over all words and stages.
  size_t maxLiveOwnedStringBytes_ = 0;
  // Largest one-shot scratch that `decompressInto(out)` allocates per word.
  size_t maxOneShotScratchBytes_ = 0;

 public:
  FsstScratchBufferBenchmark() {
    // The `0` characters are literal characters of the synthetic alphabet,
    // not numeric values; the modulo below indexes into this string.
    constexpr std::string_view alphabet{
        "abcdefghijklmnopqrstuvwxyz0123456789_:/.-#"};
    wordsStorage_.reserve(numberOfWords);
    for (size_t i = 0; i < numberOfWords; ++i) {
      std::string suffix;
      suffix.reserve(45);
      for (size_t character = 0; character < 45; ++character) {
        suffix += alphabet[(i * 17 + character * 31) % alphabet.size()];
      }
      wordsStorage_.push_back("http://www.wikidata.org/entity/Q" + suffix);
    }

    compressed_.assign(wordsStorage_.begin(), wordsStorage_.end());
    decoderStorage_.reserve(numberOfStages);
    for (size_t stage = 0; stage < numberOfStages; ++stage) {
      auto [storage, compressed, decoder] =
          FsstEncoder::compressAll(compressed_);
      compressed_ = std::move(compressed);
      decoders_[stage] = std::move(decoder);
      decoderStorage_.push_back(std::move(storage));
    }
    repeatedDecoder_ = RepeatedDecoder{decoders_};

    for (const std::string_view& compressed : compressed_) {
      outputCapacity_ = std::max(
          outputCapacity_, RepeatedDecoder::maxDecompressedSize(compressed));
    }
    intermediateCapacity_ = outputCapacity_ / FsstDecoder::maxExpansionFactor;
    for (const std::string_view& compressed : compressed_) {
      constexpr size_t factor = FsstDecoder::maxExpansionFactor;
      size_t previousRequest = 0;
      std::string current{compressed};
      for (size_t stage = 0; stage < numberOfStages; ++stage) {
        const size_t request = factor * current.size();
        maxLiveOwnedStringBytes_ =
            std::max(maxLiveOwnedStringBytes_, previousRequest + request);
        current =
            legacyDecompress(decoders_[numberOfStages - 1 - stage], current);
        previousRequest = request;
      }
      maxOneShotScratchBytes_ =
          std::max(maxOneShotScratchBytes_,
                   RepeatedDecoder::maxDecompressedSize(compressed) / factor);
    }

    // All arms must reproduce the original words.
    std::string output(outputCapacity_, '\0');
    const ql::span<char> outputSpan{output.data(), output.size()};
    std::string scratch;
    for (size_t i = 0; i < numberOfWords; ++i) {
      const std::string& word = wordsStorage_[i];
      AD_CORRECTNESS_CHECK(
          legacyRepeatedDecompress(decoders_, compressed_[i]) == word);
      AD_CORRECTNESS_CHECK(repeatedDecoder_.decompress(compressed_[i]) == word);
      size_t size =
          repeatedDecoder_.decompressInto(compressed_[i], outputSpan, scratch);
      AD_CORRECTNESS_CHECK(std::string_view(output.data(), size) == word);
      size = repeatedDecoder_.decompressInto(compressed_[i], outputSpan);
      AD_CORRECTNESS_CHECK(std::string_view(output.data(), size) == word);
    }
  }

  std::string name() const final { return "FSST decode API and scratch"; }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results;
    const size_t selectedArm = parseEnvironmentSize("FSST_BENCH_ARM", allArms);
    const size_t repetitions =
        parseEnvironmentSize("FSST_BENCH_REPETITIONS", 1);
    AD_CONTRACT_CHECK(selectedArm <= allArms);
    AD_CONTRACT_CHECK(repetitions > 0 && repetitions <= 1'000'000);
    auto& group = results.addGroup(
        "Three-stage FSST decode of 5,000 words x FSST_BENCH_REPETITIONS");

    // Buffers of the span and `decompressInto` arms, provisioned once, outside
    // the timed region. `std::string(n, '\0')` zero-fills; `new char[n]` does
    // not.
    std::string reusedOutput(outputCapacity_, '\0');
    std::string reusedScratch;
    std::string stringScratch(outputCapacity_, '\0');
    std::unique_ptr<char[]> rawOutput{new char[outputCapacity_]};
    std::unique_ptr<char[]> rawScratch{new char[outputCapacity_]};
    std::unique_ptr<char[]> stagedScratch{new char[intermediateCapacity_]};
    const ql::span<char> reusedOutputSpan{reusedOutput.data(), outputCapacity_};
    const ql::span<char> rawOutputSpan{rawOutput.get(), outputCapacity_};

    // Every arm returns the number of decoded bytes (identical across arms),
    // which also keeps the decode from being optimized away.
    using Arm = std::pair<std::string, std::function<size_t(std::string_view)>>;
    const std::array<Arm, allArms> arms{{
        {"0 pre-API decompress: owning string per stage (resize zero-fills)",
         [&](std::string_view c) {
           return legacyRepeatedDecompress(decoders_, c).size();
         }},
        {"1 decompress (this PR): owning string per stage",
         [&](std::string_view c) {
           return repeatedDecoder_.decompress(c).size();
         }},
        {"2 decompressInto(out, scratch): both buffers reused",
         [&](std::string_view c) {
           return repeatedDecoder_.decompressInto(c, reusedOutputSpan,
                                                  reusedScratch);
         }},
        {"3 decompressInto(out): one-shot scratch per word",
         [&](std::string_view c) {
           return repeatedDecoder_.decompressInto(c, reusedOutputSpan);
         }},
        {"4 spans, full-size std::string scratch (allocated once)",
         [&](std::string_view c) {
           return decodeRepeatedIntoSpans(
               decoders_, c, rawOutputSpan,
               {stringScratch.data(), outputCapacity_});
         }},
        {"5 spans, full-size uninitialized scratch (allocated once)",
         [&](std::string_view c) {
           return decodeRepeatedIntoSpans(decoders_, c, rawOutputSpan,
                                          {rawScratch.get(), outputCapacity_});
         }},
        {"6 spans, stage-aware scratch outputCapacity/8 (allocated once)",
         [&](std::string_view c) {
           return decodeRepeatedIntoSpans(
               decoders_, c, rawOutputSpan,
               {stagedScratch.get(), intermediateCapacity_});
         }},
    }};

    for (size_t arm = 0; arm < arms.size(); ++arm) {
      if (selectedArm != allArms && arm != selectedArm) {
        continue;
      }
      const auto& decodeOne = arms[arm].second;
      auto run = [&](size_t passes) {
        size_t totalBytes = 0;
        for (size_t pass = 0; pass < passes; ++pass) {
          for (const auto& compressed : compressed_) {
            totalBytes += decodeOne(compressed);
          }
        }
        return totalBytes;
      };
      // Untimed warm-up pass.
      AD_CORRECTNESS_CHECK(run(1) > 0);
      const auto before = JemallocThreadStats::read();
      JemallocThreadStats::resetPeak();
      auto& entry = group.addMeasurement(arms[arm].first,
                                         [&]() { return run(repetitions); });
      const uint64_t peak = JemallocThreadStats::peakBytes();
      const auto after = JemallocThreadStats::read();
      if (JemallocThreadStats::available()) {
        // The counters include the benchmark infrastructure's own few
        // allocations while it times the lambda.
        entry.metadata().addKeyValuePair("allocationRequests",
                                         after.requests - before.requests);
        entry.metadata().addKeyValuePair(
            "allocatedBytes", after.allocatedBytes - before.allocatedBytes);
        entry.metadata().addKeyValuePair("peakLiveHeapBytesAboveStart", peak);
      }
      entry.metadata().addKeyValuePair("decodes", numberOfWords * repetitions);
      entry.metadata().addKeyValuePair("outputCapacity", outputCapacity_);
      entry.metadata().addKeyValuePair("intermediateCapacity",
                                       intermediateCapacity_);
      // Decode buffers by arm: owning strings per word (arms 0, 1), a
      // one-shot scratch per word (arm 3), or buffers provisioned once
      // before the timed region (arms 2, 4, 5, 6; arm 2's scratch grows to
      // `intermediateCapacity_` during the warm-up).
      const std::array<size_t, allArms> perWordBufferBytes{
          maxLiveOwnedStringBytes_,
          maxLiveOwnedStringBytes_,
          0,
          maxOneShotScratchBytes_,
          0,
          0,
          0};
      const std::array<size_t, allArms> provisionedBufferBytes{
          0,
          0,
          outputCapacity_ + intermediateCapacity_,
          outputCapacity_,
          2 * outputCapacity_,
          2 * outputCapacity_,
          outputCapacity_ + intermediateCapacity_};
      entry.metadata().addKeyValuePair("maxPerWordBufferBytes",
                                       perWordBufferBytes[arm]);
      entry.metadata().addKeyValuePair("provisionedBufferBytes",
                                       provisionedBufferBytes[arm]);
    }
    return results;
  }
};

AD_REGISTER_BENCHMARK(FsstScratchBufferBenchmark);

}  // namespace
}  // namespace ad_benchmark
