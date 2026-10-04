// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Pascal Keßler <kesslerp@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// Benchmarks for the new split-column `IdColumn`/`IdRef` storage machinery
// (see `engine/idTable/splitLayout/IdColumnVector.h`, `IdRef.h`).
// This machinery is not wired into the real `IdTable` yet,
// so these numbers establish a baseline for the standalone types before that
// switch, and can be re-run afterwards to check the switch didn't regress
// performance.

#include "../benchmark/infrastructure/Benchmark.h"
#include "../test/util/AllocatorTestHelpers.h"
#include "engine/idTable/splitLayout/IdColumnVector.h"
#include "engine/idTable/splitLayout/IdRef.h"
#include "global/Id.h"
#include "util/Random.h"
#include "util/UninitializedAllocator.h"

namespace ad_benchmark {
using namespace columnBasedIdTable::splitLayout;

namespace {
using TestAllocator =
    ad_utility::default_init_allocator<Id, ad_utility::AllocatorWithLimit<Id> >;

TestAllocator makeTestAllocator() {
  return TestAllocator{ad_utility::testing::makeAllocator()};
}

std::vector<Id> makeRandomIds(size_t numIds) {
  ad_utility::FastRandomIntGenerator<int64_t> gen;
  std::vector<Id> result;
  result.reserve(numIds);
  for (size_t i = 0; i < numIds; ++i) {
    result.push_back(Id::makeFromInt(gen()));
  }
  return result;
}

class IdColumnBenchmark : public BenchmarkInterface {
 public:
  [[nodiscard]] std::string name() const final;

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results{};
    static constexpr size_t numIds = 1'000'000;
    auto ids = makeRandomIds(numIds);

    // Growth via `push_back`, which is what `IdColumnVector`'s range
    // constructor and `IdTable::clone()` currently rely on.
    results.addMeasurement("push_back " + std::to_string(numIds) + " Ids",
                           [&ids] {
                             IdColumnVector vec{makeTestAllocator()};
                             for (const Id id : ids) {
                               vec.push_back(id);
                             }
                             AD_LOG_INFO << "size " << vec.size() << std::endl;
                           });

    // Growth via the range constructor directly (same underlying loop today,
    // but the natural place to notice if that constructor later gets a
    // `reserve()` fast path).
    results.addMeasurement(
        "range-construct " + std::to_string(numIds) + " Ids", [&ids] {
          const IdColumnVector vec{ids.begin(), ids.end(), makeTestAllocator()};
          AD_LOG_INFO << "size " << vec.size() << std::endl;
        });

    // Read-only iteration through the mutable view's iterator (proxy
    // dereference + implicit conversion to `Id` on every step).
    {
      IdColumnVector vec{ids.begin(), ids.end(), makeTestAllocator()};
      results.addMeasurement(
          "iterate " + std::to_string(numIds) + " Ids via IdColumnRef", [&vec] {
            int64_t sum = 0;
            for (const Id id : vec.asConstView()) {
              sum += id.getDatatype() == Datatype::Int ? id.getInt() : 0;
            }
            AD_LOG_INFO << "sum " << sum << std::endl;
          });
    }

    // Sorting through the mutable view/iterator: exercises `BasicIdRef`'s
    // `swap`/assignment-through-the-proxy on every element move.
    results.addMeasurement(
        "ranges::sort " + std::to_string(numIds) + " random Ids", [&ids] {
          IdColumnVector vec{ids.begin(), ids.end(), makeTestAllocator()};
          ql::ranges::sort(vec.asView(), {},
                           [](const Id id) { return getBitsCompat(id); });
          AD_LOG_INFO << "size " << vec.size() << std::endl;
        });
    return results;
  }
};
}  // namespace

std::string IdColumnBenchmark::name() const {
  return "Benchmarks for the split-column IdColumn/IdRef storage";
}

AD_REGISTER_BENCHMARK(IdColumnBenchmark);
}  // namespace ad_benchmark
