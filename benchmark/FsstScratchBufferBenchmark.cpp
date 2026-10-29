// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// Compare the decode APIs of `FsstRepeatedDecoder` on the same workload: every
// arm decodes the same 5,000 compressed words through the same three FSST
// stages, `FSST_BENCH_REPETITIONS` times. The arms differ in how they obtain
// their buffers: owning strings per word or stage (arms 0 and 1) or
// caller-provided buffers with different scratch provisioning (arms 2 to 6).
// Arms 4 to 6 use the local `decodeRepeatedIntoSpans`, not
// `FsstRepeatedDecoder`. Set the following environment variables to configure
// the run.
//   `FSST_BENCH_ARM`          Run only this arm (0..6, default: all arms).
//   `FSST_BENCH_REPETITIONS`  Set the passes over the 5,000 words (default 1).
// One untimed warm-up pass runs before each timed measurement. The
// constructor checks that every arm decodes every word to the original bytes.

#include <dlfcn.h>

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
// Decode `str` as `FsstDecoder::decompress` did before `decompressInto`
// existed: size an owning string to the 8x bound with `resize` (which
// zero-fills it), decode, shrink. Return the decoded word as a new string.
// `decompressInto` performs the same `fsst_decompress` call plus two bound
// checks.
std::string legacyDecompress(const FsstDecoder& decoder, std::string_view str) {
  std::string output;
  output.resize(FsstDecoder::maxDecompressedSize(str));
  const size_t size =
      decoder.decompressInto(str, ql::span<char>{output.data(), output.size()});
  output.resize(size);
  return output;
}

// _____________________________________________________________________________
// Decode `str` as `FsstRepeatedDecoder::decompress` did before `decompressInto`
// existed: one owning string per stage, each built by `legacyDecompress`.
// Return the decoded word as a new string.
std::string legacyRepeatedDecompress(const Decoders& decoders,
                                     std::string_view str) {
  std::string decoded = legacyDecompress(decoders[numberOfStages - 1], str);
  for (size_t stage = 1; stage < numberOfStages; ++stage) {
    decoded = legacyDecompress(decoders[numberOfStages - 1 - stage], decoded);
  }
  return decoded;
}

// _____________________________________________________________________________
// Decode through all stages into `output`, alternating with `scratch` (the
// parity scheme of `FsstRepeatedDecoder::decompressInto`), with caller-provided
// spans so that the scratch provisioning can be varied. `output` must hold
// `RepeatedDecoder::maxDecompressedSize(compressed)` bytes and `scratch` the
// largest intermediate bound, which is that size divided by the expansion
// factor. Return the number of decoded bytes written to `output`.
size_t decodeRepeatedIntoSpans(const Decoders& decoders,
                               std::string_view compressed,
                               ql::span<char> output, ql::span<char> scratch) {
  size_t targetBufferIdx = (numberOfStages % 2 == 0) ? 1 : 0;
  const std::array<ql::span<char>, 2> buffers{output, scratch};
  std::string_view input = compressed;
  size_t bytesWritten = 0;
  for (size_t stage = 0; stage < numberOfStages; ++stage) {
    bytesWritten = decoders[numberOfStages - 1 - stage].decompressInto(
        input, buffers[targetBufferIdx]);
    input = {buffers[targetBufferIdx].data(), bytesWritten};
    targetBufferIdx ^= 1;
  }
  return bytesWritten;
}

// _____________________________________________________________________________
// Read allocation statistics from jemalloc (which QLever links when it is
// installed) via `mallctl`. The request counters cover all threads, the other
// values the calling thread. The function is looked up at runtime, so the
// benchmark also links without jemalloc (e.g. on macOS, where a weak reference
// to an undefined symbol does not link); then `available()` is false and no
// statistics are recorded. All reads happen outside the timed region.
//
// Signature of `mallctl(name, oldp, oldlenp, newp, newlen)`: it reads the value
// of `name` into `oldp` (whose size is `*oldlenp`), or writes `newp` (of size
// `newlen`) to it, and returns 0 on success.
using MallctlFunction = int (*)(const char* name, void* oldp, size_t* oldlenp,
                                void* newp, size_t newlen);
// _____________________________________________________________________________
// Return the `mallctl` function of the linked jemalloc, or `nullptr` if there
// is none. The lookup happens once.
MallctlFunction mallctlFunction() {
  static const MallctlFunction mallctlSymbol =
      reinterpret_cast<MallctlFunction>(dlsym(RTLD_DEFAULT, "mallctl"));
  return mallctlSymbol;
}

class JemallocThreadStats {
 public:
  struct Snapshot {
    // Number of allocation requests of all threads.
    uint64_t requests = 0;
    // Bytes allocated by this thread.
    uint64_t allocatedBytes = 0;
  };

  // Return whether `mallctl` was found. Controls that are newer than
  // jemalloc 5.0, like `thread.peak.reset`, can still be missing.
  static bool available() { return mallctlFunction() != nullptr; }

  // ___________________________________________________________________________
  // Flush this thread's cache so that the arena counters include all its
  // requests, advance the statistics epoch, then read the counters. The arena
  // index 4096 is jemalloc's `MALLCTL_ARENAS_ALL`, which merges all arenas.
  static Snapshot read() {
    Snapshot stats;
    if (!available()) {
      return stats;
    }
    mallctlFunction()("thread.tcache.flush", nullptr, nullptr, nullptr, 0);
    uint64_t epoch = 1;
    size_t length = sizeof(epoch);
    mallctlFunction()("epoch", &epoch, &length, &epoch, length);
    stats.requests = readUint64("stats.arenas.4096.small.nrequests") +
                     readUint64("stats.arenas.4096.large.nrequests");
    stats.allocatedBytes = readUint64("thread.allocated");
    return stats;
  }

  // ___________________________________________________________________________
  // Reset the high-water mark of this thread's live heap bytes to the current
  // live bytes. Return whether the reset worked, which is not the case if
  // jemalloc is missing or older than 5.3. Only then is `peakBytes()` valid.
  [[nodiscard]] static bool resetPeak() {
    return available() && mallctlFunction()("thread.peak.reset", nullptr,
                                            nullptr, nullptr, 0) == 0;
  }

  // ___________________________________________________________________________
  // Return the high-water mark of this thread's live heap bytes since the last
  // successful `resetPeak()`. It is an absolute value, not relative to the
  // live bytes at the reset.
  static uint64_t peakBytes() {
    return available() ? readUint64("thread.peak.read") : 0;
  }

 private:
  // ___________________________________________________________________________
  // Return the `uint64_t` value of the `mallctl` control `name`, or 0 if the
  // control cannot be read.
  static uint64_t readUint64(const char* name) {
    uint64_t value = 0;
    size_t length = sizeof(value);
    return mallctlFunction()(name, &value, &length, nullptr, 0) == 0 ? value
                                                                     : 0;
  }
};

// _____________________________________________________________________________
// Return the unsigned integer in the environment variable `varName`, or
// `defaultValue` if it is not set. Throw a contract violation if the value is
// not a valid unsigned integer.
size_t parseEnvironmentCount(const char* varName, size_t defaultValue) {
  const char* rawValue = std::getenv(varName);
  if (rawValue == nullptr) {
    return defaultValue;
  }
  errno = 0;
  char* end = nullptr;
  const unsigned long parsed = std::strtoul(rawValue, &end, 10);
  AD_CONTRACT_CHECK(end != rawValue && *end == '\0' && errno != ERANGE,
                    "Invalid value `", rawValue, "` for environment variable ",
                    varName);
  return static_cast<size_t>(parsed);
}

class FsstScratchBufferBenchmark : public BenchmarkInterface {
 private:
  static constexpr size_t numberOfWords = 5'000;
  // The number of arms. As the value of `FSST_BENCH_ARM`, it selects all arms.
  static constexpr size_t allArms = 7;
  // The upper limit of `FSST_BENCH_REPETITIONS`.
  static constexpr size_t maxRepetitions = 1'000'000;
  // The length of the synthetic suffix of each word.
  static constexpr size_t suffixLength = 45;
  // The three times compressed words, as views into `compressedStorage_`.
  std::vector<std::string_view> compressed_;
  // Owns the original words, which the first `compressAll` call reads.
  std::vector<std::string> wordsStorage_;
  Decoders decoders_{};
  RepeatedDecoder repeatedDecoder_{};
  // Owns the compressed strings behind the views in `compressed_`.
  std::vector<std::shared_ptr<std::string>> compressedStorage_;
  // Largest word, which the final stage of `FsstRepeatedDecoder::decompress`
  // allocates as its only heap buffer.
  size_t maxWordBytes_ = 0;
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
  // Largest scratch that a per-word (one-shot) scratch string allocates.
  size_t maxOneShotScratchBytes_ = 0;

 public:
  FsstScratchBufferBenchmark() {
    // Characters of the synthetic alphabet; the modulo below indexes into this
    // string. The suffix depends on `i` only through `i * 17 mod 42`, and 17
    // and 42 are coprime, so the workload consists of 42 distinct words that
    // repeat.
    constexpr std::string_view alphabet{
        "abcdefghijklmnopqrstuvwxyz0123456789_:/.-#"};
    wordsStorage_.reserve(numberOfWords);
    for (size_t i = 0; i < numberOfWords; ++i) {
      std::string suffix;
      suffix.reserve(suffixLength);
      for (size_t character = 0; character < suffixLength; ++character) {
        suffix += alphabet[(i * 17 + character * 31) % alphabet.size()];
      }
      wordsStorage_.push_back("http://www.wikidata.org/entity/Q" + suffix);
    }

    compressed_.assign(wordsStorage_.begin(), wordsStorage_.end());
    compressedStorage_.reserve(numberOfStages);
    for (size_t stage = 0; stage < numberOfStages; ++stage) {
      auto [compressedBytes, compressed, decoder] =
          FsstEncoder::compressAll(compressed_);
      compressed_ = std::move(compressed);
      decoders_[stage] = std::move(decoder);
      compressedStorage_.push_back(std::move(compressedBytes));
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
      std::string stageOutput{compressed};
      for (size_t stage = 0; stage < numberOfStages; ++stage) {
        const size_t request = factor * stageOutput.size();
        maxLiveOwnedStringBytes_ =
            std::max(maxLiveOwnedStringBytes_, previousRequest + request);
        stageOutput = legacyDecompress(decoders_[numberOfStages - 1 - stage],
                                       stageOutput);
        previousRequest = request;
      }
      maxWordBytes_ = std::max(maxWordBytes_, stageOutput.size());
      maxOneShotScratchBytes_ =
          std::max(maxOneShotScratchBytes_,
                   RepeatedDecoder::maxDecompressedSize(compressed) / factor);
    }

    // All arms must reproduce the original words. Arms 4 to 6 differ from each
    // other only in the size and provisioning of the scratch span, so the
    // smallest one (arm 6) is checked for all of them.
    std::string output(outputCapacity_, '\0');
    const ql::span<char> outputSpan{output.data(), output.size()};
    std::string scratch;
    std::string spanScratch(intermediateCapacity_, '\0');
    for (size_t i = 0; i < numberOfWords; ++i) {
      const std::string& word = wordsStorage_[i];
      AD_CORRECTNESS_CHECK(
          legacyRepeatedDecompress(decoders_, compressed_[i]) == word);
      AD_CORRECTNESS_CHECK(repeatedDecoder_.decompress(compressed_[i]) == word);
      size_t size =
          repeatedDecoder_.decompressInto(compressed_[i], outputSpan, scratch);
      AD_CORRECTNESS_CHECK(std::string_view(output.data(), size) == word);
      std::string oneShotScratch;
      size = repeatedDecoder_.decompressInto(compressed_[i], outputSpan,
                                             oneShotScratch);
      AD_CORRECTNESS_CHECK(std::string_view(output.data(), size) == word);
      size = decodeRepeatedIntoSpans(
          decoders_, compressed_[i], outputSpan,
          ql::span<char>{spanScratch.data(), spanScratch.size()});
      AD_CORRECTNESS_CHECK(std::string_view(output.data(), size) == word);
    }
  }

  std::string name() const final { return "FSST decode API and scratch"; }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results;
    const size_t selectedArm = parseEnvironmentCount("FSST_BENCH_ARM", allArms);
    const size_t repetitions =
        parseEnvironmentCount("FSST_BENCH_REPETITIONS", 1);
    AD_CONTRACT_CHECK(selectedArm <= allArms);
    AD_CONTRACT_CHECK(repetitions > 0 && repetitions <= maxRepetitions);
    auto& group = results.addGroup(
        "Three-stage FSST decode of 5,000 words x FSST_BENCH_REPETITIONS");

    // Provision the buffers of the span and `decompressInto` arms once, outside
    // the timed region. `std::string(n, '\0')` zero-fills; `new char[n]` does
    // not. `reusedScratch` stays empty here and grows to
    // `intermediateCapacity_` during the warm-up pass of arm 2.
    std::string reusedOutput(outputCapacity_, '\0');
    std::string reusedScratch;
    std::string stringScratch(outputCapacity_, '\0');
    const std::unique_ptr<char[]> rawOutput{new char[outputCapacity_]};
    const std::unique_ptr<char[]> rawScratch{new char[outputCapacity_]};
    const std::unique_ptr<char[]> stagedScratch{
        new char[intermediateCapacity_]};
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
        {"1 decompress (this PR): stack buffers for the intermediate stages, "
         "one owning string",
         [&](std::string_view c) {
           return repeatedDecoder_.decompress(c).size();
         }},
        {"2 decompressInto(out, scratch): both buffers reused",
         [&](std::string_view c) {
           return repeatedDecoder_.decompressInto(c, reusedOutputSpan,
                                                  reusedScratch);
         }},
        {"3 decompressInto(out, scratch): one-shot scratch per word",
         [&](std::string_view c) {
           std::string oneShotScratch;
           return repeatedDecoder_.decompressInto(c, reusedOutputSpan,
                                                  oneShotScratch);
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
      const auto run = [&](size_t passes) {
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
      const auto statsBefore = JemallocThreadStats::read();
      const bool peakIsTracked = JemallocThreadStats::resetPeak();
      auto& entry = group.addMeasurement(arms[arm].first,
                                         [&]() { return run(repetitions); });
      const uint64_t peak = JemallocThreadStats::peakBytes();
      const auto statsAfter = JemallocThreadStats::read();
      if (JemallocThreadStats::available()) {
        // The counters include the benchmark infrastructure's own few
        // allocations while it times the lambda.
        entry.metadata().addKeyValuePair(
            "allocationRequests", statsAfter.requests - statsBefore.requests);
        entry.metadata().addKeyValuePair(
            "allocatedBytes",
            statsAfter.allocatedBytes - statsBefore.allocatedBytes);
      }
      if (peakIsTracked) {
        // `thread.peak.reset` starts the high-water mark at the thread's live
        // bytes at that moment, so this is an absolute peak of live heap
        // bytes, not the growth above the start.
        entry.metadata().addKeyValuePair("peakLiveHeapBytes", peak);
      }
      entry.metadata().addKeyValuePair("decodes", numberOfWords * repetitions);
      entry.metadata().addKeyValuePair("outputCapacity", outputCapacity_);
      entry.metadata().addKeyValuePair("intermediateCapacity",
                                       intermediateCapacity_);
      // Heap buffers by arm that are allocated per word: owning strings per
      // stage (arm 0), only the owning result string (arm 1), a one-shot
      // scratch per word (arm 3), or none because the buffers are provisioned
      // once before the timed region (arms 2, 4, 5, 6; arm 2's scratch grows
      // to `intermediateCapacity_` during the warm-up).
      const std::array<size_t, allArms> perWordBufferBytes{
          maxLiveOwnedStringBytes_,
          maxWordBytes_,
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
