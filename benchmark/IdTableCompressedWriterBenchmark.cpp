//
// Created by kalmbacj on 9/8/23.
//

#include "../benchmark/infrastructure/Benchmark.h"
#include "../test/util/IdTableHelpers.h"
#include "engine/idTable/IdTableCompressedWriter.h"
#include "index/StxxlSortFunctors.h"
#include "util/BackgroundStxxlSorter.h"

namespace ad_benchmark {

using A = std::array<Id, 3>;
class IdTableCompressedWriterBenchmarks : public BenchmarkInterface {
  std::string name() const final {
    return "Benchmarks for external sorting and storage of IdTables";
  }

  BenchmarkResults runAllBenchmarks() final {
    constexpr size_t numCols = 3;
    const size_t numInputRows = 200'000'000;
    const size_t memForStxxl = 500'000'000;
    BenchmarkResults results{};

    auto generateRandomRow = [gen = FastRandomIntGenerator<uint64_t>{}]() mutable {
      std::array<Id, numCols> arr;
      for (auto& id : arr) {
        id = Id::fromBits(gen());
      }
      return arr;
    };

    ad_utility::BackgroundStxxlSorter<A, SortByPSO> sorter{memForStxxl};


    std::string filename = "idTableCompressedSorter.benchmark.dat";
    [[maybe_unused]] auto firstCol = [](const auto& a, const auto& b) {
      return a[0] < b[0];
    };
    ExternalIdTableSorter<SortByPSO, 3> writer{
        filename, 3, memForStxxl, ad_utility::testing::makeAllocator()};

    auto runPush = [&]() {
      for (auto i : std::views::iota(0u, numInputRows)) {
        writer.pushRow(generateRandomRow());
      }
    };
    auto run = [&]() {
      auto view = writer.sortedView();
      const auto& res = *view.begin();
      std::cout << res.size();
    };

    results.addMeasurement("SortingAndWritingBlocks", runPush);
    results.addMeasurement("ReadAndMerge", run);

    auto pushStxxl = [&]() {
      for (auto i : std::views::iota(0u, numInputRows)) {
        sorter.push(generateRandomRow());
      }
    };

    auto drainStxxl = [&]() {
        size_t i = 1;
        for (const auto& row : sorter.sortedView()) {
          ++i;
        }
        std::cout << i;
    };

    results.addMeasurement("Time using stxxl for push", pushStxxl);
    results.addMeasurement("Time using stxxl for pull", drainStxxl);

    std::cout << "\nNum blocks in stxxl" << sorter.numBlocks() << std::endl;

    return results;
  }
};
AD_REGISTER_BENCHMARK(IdTableCompressedWriterBenchmarks);
}  // namespace ad_benchmark
