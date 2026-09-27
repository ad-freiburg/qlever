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
#include "engine/RleVectorStream.h"
#include "global/Id.h"
#include "util/Exception.h"

namespace ad_benchmark {

// Build cost and size of a flat `std::vector<Id>` versus an `RleVectorStream`
// for a column with long runs, and the cost of materializing the stream into a
// preallocated buffer.
class RleVectorStreamBenchmark : public BenchmarkInterface {
 public:
  std::string name() const override { return "RleVectorStream"; }

  BenchmarkResults runAllBenchmarks() override {
    BenchmarkResults results{};
    // 10 M rows (80 MB as a flat column) in 1,000 runs of 10,000 rows.
    constexpr size_t NUM_RUNS = 1'000;
    constexpr uint32_t RUN_LENGTH = 10'000;
    constexpr size_t NUM_ROWS = NUM_RUNS * RUN_LENGTH;
    auto runValue = [](size_t run) {
      return Id::makeFromInt(static_cast<int64_t>(run));
    };

    auto& group = results.addGroup("10 M rows in 1,000 runs");
    group.metadata().addKeyValuePair("flat bytes", NUM_ROWS * sizeof(Id));
    group.metadata().addKeyValuePair(
        "rle bytes", NUM_RUNS * sizeof(ql::engine::rle::RleVectorStream::Run));

    std::vector<Id> flat;
    group.addMeasurement("Build flat column", [&] {
      flat.clear();
      flat.reserve(NUM_ROWS);
      for (size_t run = 0; run < NUM_RUNS; ++run) {
        flat.insert(flat.end(), RUN_LENGTH, runValue(run));
      }
    });
    AD_CORRECTNESS_CHECK(flat.size() == NUM_ROWS);

    ql::engine::rle::RleVectorStream stream;
    group.addMeasurement("Build RleVectorStream", [&] {
      stream = {};
      for (size_t run = 0; run < NUM_RUNS; ++run) {
        stream.append(runValue(run), RUN_LENGTH);
      }
    });
    AD_CORRECTNESS_CHECK(stream.totalRows() == NUM_ROWS);

    // The destination is allocated and touched outside the measurement.
    std::vector<Id> materialized(NUM_ROWS);
    group.addMeasurement("Materialize RleVectorStream",
                         [&] { stream.materialize(materialized); });
    // Observing the result keeps the materialization from being optimized
    // away and checks it.
    AD_CORRECTNESS_CHECK(materialized == flat);

    return results;
  }
};
AD_REGISTER_BENCHMARK(RleVectorStreamBenchmark);
}  // namespace ad_benchmark
