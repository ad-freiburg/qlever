//
// Created by kalmbacj on 9/8/23.
//

#include <cmath>

#include "../benchmark/infrastructure/Benchmark.h"
#include "../test/util/IdTableHelpers.h"
#include "util/Log.h"
#include "util/ParallelMultiwayMerge.h"

namespace ad_benchmark {

class IdTableCompressedWriterBenchmark : public BenchmarkInterface {
  std::string name() const final {
    return "Benchmarks for parallel multiway merging";
  }

  BenchmarkResults runAllBenchmarks() final {
    constexpr size_t numInputs = 20000;
    constexpr size_t numInputRows = 50'000;
    BenchmarkResults results{};

    auto generateRandomVec =
        [gen = ad_utility::FastRandomIntGenerator<uint64_t>{}]() mutable {
          std::vector<size_t> res;
          for ([[maybe_unused]] auto i :
               ad_utility::integerRange(numInputRows)) {
            res.push_back(gen());
          }
          std::ranges::sort(res);
          return res;
        };
    std::vector<std::vector<size_t>> inputs;
    inputs.resize(numInputs);
    std::ranges::generate(inputs, generateRandomVec);

    auto run = [&inputs]() {
      auto merger = ad_utility::parallelMultiwayMerge<size_t, false>(
          4_GB, inputs, std::less<>{});
      size_t result{};
      for (const auto& i : merger) {
        for (auto el : i) {
          result += el;
        }
      }
      LOG(INFO) << "result was " << result << std::endl;
    };

    results.addMeasurement("simple merge", run);
    return results;
  }
};
AD_REGISTER_BENCHMARK(IdTableCompressedWriterBenchmark);
}  // namespace ad_benchmark
