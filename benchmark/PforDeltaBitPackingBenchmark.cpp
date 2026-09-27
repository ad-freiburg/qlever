// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "../benchmark/infrastructure/Benchmark.h"
#include "backports/algorithm.h"
#include "global/Id.h"
#include "index/PforDeltaBitPacking.h"
#include "util/Exception.h"

namespace ad_benchmark {

// Compression and decompression cost and size of `PforDeltaBitPacking` for
// 6.4 M sorted `Id`s with gap 2 (7 bits per value), against copying the flat
// column.
class PforDeltaBitPackingBenchmark : public BenchmarkInterface {
 public:
  std::string name() const override { return "PforDeltaBitPacking"; }

  BenchmarkResults runAllBenchmarks() override {
    using ql::index::compression::PforDeltaBitPacking;
    constexpr size_t BLOCK = PforDeltaBitPacking::BLOCK_SIZE;
    constexpr size_t NUM_BLOCKS = 100'000;
    constexpr size_t NUM_IDS = NUM_BLOCKS * BLOCK;
    BenchmarkResults results{};

    std::vector<Id> input;
    input.reserve(NUM_IDS);
    for (uint64_t i = 0; i < NUM_IDS; ++i) {
      input.push_back(Id::fromBits(1'000'000 + i * 2));
    }
    auto block = [&input](size_t b) {
      return ql::span<const Id>{input}.subspan(b * BLOCK, BLOCK);
    };

    auto& group = results.addGroup("6.4 M sorted Ids, gap 2");
    std::vector<Id> copy(NUM_IDS);
    group.addMeasurement("Copy flat column",
                         [&] { ql::ranges::copy(input, copy.begin()); });
    AD_CORRECTNESS_CHECK(copy == input);

    std::vector<PforDeltaBitPacking::CompressedBlock> compressed;
    group.addMeasurement("Compress", [&] {
      compressed.clear();
      compressed.reserve(NUM_BLOCKS);
      for (size_t b = 0; b < NUM_BLOCKS; ++b) {
        compressed.push_back(PforDeltaBitPacking::compressBlock(block(b)));
      }
    });

    std::vector<Id> decompressed(NUM_IDS);
    group.addMeasurement("Decompress", [&] {
      for (size_t b = 0; b < NUM_BLOCKS; ++b) {
        PforDeltaBitPacking::decompressBlock(
            compressed[b], ql::span<Id>{decompressed}.subspan(b * BLOCK));
      }
    });
    AD_CORRECTNESS_CHECK(decompressed == input);

    size_t compressedBytes = 0;
    for (const auto& c : compressed) {
      compressedBytes += sizeof(c) + c.packedWords_.size() * sizeof(uint64_t);
    }
    group.metadata().addKeyValuePair("flat bytes", NUM_IDS * sizeof(Id));
    group.metadata().addKeyValuePair("compressed bytes", compressedBytes);
    return results;
  }
};

AD_REGISTER_BENCHMARK(PforDeltaBitPackingBenchmark);

}  // namespace ad_benchmark
