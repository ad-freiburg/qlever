// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Benke Hargitai <hargitab@cs.uni-freiburg.de>

// Benchmark infrastructure
#include "../benchmark/infrastructure/Benchmark.h"
// Test utilities
#include "../test/engine/ValuesForTesting.h"
#include "../test/util/AllocatorTestHelpers.h"
#include "../test/util/IdTestHelpers.h"
#include "../test/util/IndexTestHelpers.h"
// Core engine headers
#include "engine/GroupByImpl.h"
#include "engine/QueryExecutionContext.h"
#include "engine/QueryExecutionTree.h"
#include "engine/Result.h"
#include "engine/Values.h"
#include "engine/idTable/IdTable.h"
#include "global/RuntimeParameters.h"
// Utility
#include "util/Random.h"

namespace ad_benchmark {
// Bring in needed test utilities and helpers
using ad_utility::AllocatorWithLimit;
using ad_utility::makeExecutionTree;
using ad_utility::testing::getQec;
using ad_utility::testing::makeAllocator;
// ValuesForTesting is declared in the global namespace in test/engine
using ::ValuesForTesting;
using ad_utility::testing::IntId;

class GroupBySamplingBenchmark : public BenchmarkInterface {
 public:
  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results;
    QueryExecutionContext* qec = getQec();

    // choose a few table sizes and cardinalities, including small and unit
    // cardinalities
    for (size_t n : {1'000u, 10'000u, 100'000u, 1'000'000u}) {
      // test also a single-group scenario to see if sort wins on tiny outputs
      std::vector<size_t> cards = {1u, n / 10, n / 2, n};
      for (size_t card : cards) {
        // build an IdTable with one grouping column
        auto makeTable = [&]() {
          AllocatorWithLimit<Id> alloc{makeAllocator()};
          IdTable t(1, alloc);
          t.resize(n);
          for (size_t i = 0; i < n; ++i) {
            t(i, 0) = IntId(i % card);
          }
          return t;
        };

        auto table = makeTable();
        // wrap it into a Values subtree
        std::vector<std::optional<Variable>> varsOpt = {Variable{"?a"}};
        std::vector<ColumnIndex> sortedOn{};
        auto valuesOp = ad_utility::makeExecutionTree<ValuesForTesting>(
            qec, std::move(table), varsOpt, false, sortedOn);

        // descriptor for this parameter combination, and prepare vars for
        // GroupBy
        std::ostringstream desc;
        desc << "n=" << n << ", card=" << card;
        auto& group = results.addGroup(desc.str());
        // the actual variables for grouping
        std::vector<Variable> vars = {Variable{"?a"}};

        // hash‐path (hash-map enabled)
        group.addMeasurement("hash", [&] {
          RuntimeParameters().set<"group-by-hash-map-enabled">(true);
          auto gb = std::make_unique<GroupByImpl>(
              qec, vars, std::vector<Alias>{}, valuesOp);
          gb->getResult();  // run grouping
          qec->clearCacheUnpinnedOnly();
        });

        // sort‐path (force sort-based grouping)
        group.addMeasurement("sort", [&] {
          RuntimeParameters().set<"group-by-hash-map-enabled">(false);
          auto gb = std::make_unique<GroupByImpl>(
              qec, vars, std::vector<Alias>{}, valuesOp);
          gb->getResult();
          qec->clearCacheUnpinnedOnly();
        });
      }
    }

    return results;
  }

  std::string name() const final {
    return "GroupBy hash vs sort (sweep n/card)";
  }
};

AD_REGISTER_BENCHMARK(GroupBySamplingBenchmark);

}  // namespace ad_benchmark
