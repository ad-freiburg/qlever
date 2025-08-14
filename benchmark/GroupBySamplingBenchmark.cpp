#include <cmath>
#include <cstdlib>
#include <future>
#include <memory>
#include <numeric>
#include <string>
#include <vector>

#include "../benchmark/infrastructure/Benchmark.h"
#include "../benchmark/infrastructure/BenchmarkMeasurementContainer.h"
#include "engine/GroupByImpl.h"
#include "engine/GroupByStrategyChooser.h"
#include "engine/QueryExecutionContext.h"
#include "engine/QueryExecutionTree.h"
#include "engine/Result.h"
#include "engine/Values.h"
#include "engine/idTable/IdTable.h"
#include "global/RuntimeParameters.h"
#include "parser/GraphPatternOperation.h"
#include "util/Log.h"

// Shared testing helpers for sampling
#include "../test/engine/GroupByStrategyHelpers.h"

namespace ad_benchmark {

class GroupBySamplingBenchmark : public BenchmarkInterface {
 public:
  // Constructor: add sampling-time-log option to this benchmark only
  GroupBySamplingBenchmark() {
    auto& config = getConfigManager();
    config.addOption("detailed-time-log",
                     "Enable time logging for preparation and sampling",
                     &samplingTimeLog_, false);
  }

  std::string name() const final { return "GroupBySamplingBenchmark"; }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results;

    // Setup execution context and allocator
    auto qec = ad_utility::testing::getQec();
    ad_utility::AllocatorWithLimit<Id> allocator{
        ad_utility::testing::makeAllocator()};

    // Determine whether to enable timing logs via per-benchmark option
    const bool doTiming = samplingTimeLog_;
    if (!doTiming) {
      AD_LOG_INFO << "1 available option unused (use -o to see the options)"
                  << std::endl;
    }
    LogLevel logLevel = doTiming ? TIMING : INFO;
    auto to_ms = [](const auto d) {
      return std::chrono::duration_cast<std::chrono::milliseconds>(d);
    };
    // Test various table sizes
    std::vector<uint64_t> sizes = {10'000ULL, 1'000'000ULL, 100'000'000ULL,
                                   1'000'000'000ULL};
    // Fractions for distinct ratio
    std::vector<uint64_t> d_fracs;
    for (auto n : sizes) {
      // constant multiplier
      size_t k = RuntimeParameters().get<"group-by-sample-constant">();
      d_fracs = {1ULL, 5ULL, 100ULL, 1000ULL, n / 2, n};
      for (auto d_frac : d_fracs) {
        results.addMeasurement(
            "guard_call_" + std::to_string(n) + "_d" + std::to_string(d_frac),
            [n, k, d_frac, &allocator, qec, doTiming, logLevel, to_ms]() {
              // Create IdTable and GroupByImpl using shared helpers
              auto t0 = std::chrono::steady_clock::now();
              auto table = createIdTable(
                  n,
                  [d_frac](size_t i) {
                    return static_cast<int64_t>(i % d_frac);
                  },
                  allocator);
              auto t1 = std::chrono::steady_clock::now();
              auto groupBy = setupGroupBy(table, qec);
              auto t2 = std::chrono::steady_clock::now();
              if (doTiming) {
                auto idtableTime = to_ms(t1 - t0);
                auto setupTime = to_ms(t2 - t1);
                AD_LOG_INFO
                    << "Timing (ms): setup idtable=" << idtableTime.count()
                    << ", setup groupby=" << setupTime.count() << std::endl;
              }
              // Call under test with chosen logLevel
              GroupByStrategyChooser::shouldSkipHashMapGrouping(*groupBy, table,
                                                                logLevel);
            });
      }
    }
    return results;
  }

 private:
  // Per-benchmark flag for sampling timing logs
  bool samplingTimeLog_ = false;
};

AD_REGISTER_BENCHMARK(GroupBySamplingBenchmark)

}  // namespace ad_benchmark
