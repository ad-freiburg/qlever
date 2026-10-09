// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Pascal Keßler <kesslerp@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// Compare the lexicographic comparison of a row of an `IdTable` with a triple
// of `Id`s (as in `LocatedTriples.cpp`) when the entries are put into a
// `std::tie` (references) and when they are put into a `std::array<Id, 3>`
// (copies). The array is needed for column storages that return proxy objects
// instead of `Id&`, because `std::tie` cannot bind to them.
// NOTE: The `std::tie` variant only compiles if the element access of the
// `IdTable` returns references, so this benchmark only works for the legacy
// layout.

#include <array>
#include <string>
#include <tuple>
#include <vector>

#include "../benchmark/infrastructure/Benchmark.h"
#include "engine/idTable/IdTable.h"
#include "global/Id.h"
#include "util/AllocatorWithLimit.h"
#include "util/CompilerExtensions.h"
#include "util/Log.h"

namespace ad_benchmark {
namespace {
constexpr size_t numRows = 5'000'000;

using Triple = std::array<Id, 3>;

// A table and a vector of triples with `numRows` entries each. The row `i` is
// `(i / 4, i / 2, i)`. The triple `i` is equal to the row `i` for one of three
// positions, and one larger or smaller otherwise. Hence, the comparison of the
// two is decided on all three columns, and its result is `less`, `equal` and
// `greater` equally often.
struct Data {
  IdTable table{3, ad_utility::makeUnlimitedAllocator<Id>()};
  std::vector<Triple> triples;
};

Data makeData() {
  Data data;
  data.table.resize(numRows);
  data.triples.reserve(numRows);
  auto makeId = [](size_t value) {
    return Id::makeFromInt(static_cast<int64_t>(value));
  };
  for (size_t i = 0; i < numRows; ++i) {
    data.table(i, 0) = makeId(i / 4);
    data.table(i, 1) = makeId(i / 2);
    data.table(i, 2) = makeId(i);
    size_t j = i % 3 == 0 ? i : (i % 3 == 1 ? i + 1 : (i == 0 ? 0 : i - 1));
    data.triples.push_back({makeId(j / 4), makeId(j / 2), makeId(j)});
  }
  return data;
}

enum class Comparison { Less, Equal };

// Count the rows for which the triple compares `Less` (or `Equal`) to the row.
// The functions are not inlined, such that both variants are compiled in the
// same way as the functions in `LocatedTriples.cpp`.
template <Comparison comparison>
AD_NO_INLINE size_t countWithTie(const Data& data) {
  size_t count = 0;
  for (size_t i = 0; i < numRows; ++i) {
    auto row = data.table[i];
    const auto& triple = data.triples[i];
    auto lhs = std::tie(triple[0], triple[1], triple[2]);
    auto rhs = std::tie(row[0], row[1], row[2]);
    count += comparison == Comparison::Less ? lhs < rhs : lhs == rhs;
  }
  return count;
}

template <Comparison comparison>
AD_NO_INLINE size_t countWithArray(const Data& data) {
  size_t count = 0;
  for (size_t i = 0; i < numRows; ++i) {
    auto row = data.table[i];
    const auto& triple = data.triples[i];
    Triple lhs{triple[0], triple[1], triple[2]};
    Triple rhs{row[0], row[1], row[2]};
    count += comparison == Comparison::Less ? lhs < rhs : lhs == rhs;
  }
  return count;
}

class TieVsArrayBenchmark : public BenchmarkInterface {
 public:
  [[nodiscard]] std::string name() const final;

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results{};
    const auto data = makeData();

    // To ensure the first run is not influenced by cold caches.
    AD_LOG_INFO << "Warm up " << countWithTie<Comparison::Less>(data)
                << std::endl;

    auto& lessGroup = results.addGroup("triple < row");
    lessGroup.addMeasurement("std::tie (references)", [&data] {
      AD_LOG_INFO << "Less " << countWithTie<Comparison::Less>(data)
                  << std::endl;
    });
    lessGroup.addMeasurement("std::array<Id, 3> (copies)", [&data] {
      AD_LOG_INFO << "Less " << countWithArray<Comparison::Less>(data)
                  << std::endl;
    });

    auto& equalGroup = results.addGroup("triple == row");
    equalGroup.addMeasurement("std::tie (references)", [&data] {
      AD_LOG_INFO << "Equal " << countWithTie<Comparison::Equal>(data)
                  << std::endl;
    });
    equalGroup.addMeasurement("std::array<Id, 3> (copies)", [&data] {
      AD_LOG_INFO << "Equal " << countWithArray<Comparison::Equal>(data)
                  << std::endl;
    });
    return results;
  }
};
}  // namespace

std::string TieVsArrayBenchmark::name() const {
  return "Benchmarks for the comparison of rows via std::tie and std::array";
}

AD_REGISTER_BENCHMARK(TieVsArrayBenchmark);
}  // namespace ad_benchmark
