//
// Created by kalmbacj on 9/8/23.
//

#include <cmath>

#include "../benchmark/infrastructure/Benchmark.h"
#include "../test/util/IdTableHelpers.h"
#include "engine/idTable/CompressedExternalIdTable.h"
#include "index/StxxlSortFunctors.h"
#include "util/BackgroundStxxlSorter.h"
#include "util/Log.h"

// On macOS on GitHub actions we currently have trouble with STXXL in this
// Benchmark.
#ifndef __APPLE__

namespace ad_benchmark {

using A = std::array<Id, 3>;
class IdTableCompressedWriterBenchmarks : public BenchmarkInterface {
  std::string name() const final {
    return "Benchmarks for external sorting and storage of IdTables";
  }

  BenchmarkResults runAllBenchmarks() final {
    constexpr size_t numCols = 3;
    const size_t numInputRows = 20'000'000'000;
    const size_t memForStxxl = 5'000'000'000;
    BenchmarkResults results{};

    auto generateRandomRow =
        [gen = ad_utility::FastRandomIntGenerator<uint64_t>{}]() mutable {
          std::array<Id, numCols> arr;
          for (auto& id : arr) {
            id = Id::fromBits(gen() >> 40);
          }
          return arr;
        };

    ad_utility::BackgroundStxxlSorter<A, SortByPSO> sorter{memForStxxl};

    std::string filename = "idTableCompressedSorter.benchmark.dat";
    [[maybe_unused]] auto firstCol = [](const auto& a, const auto& b) {
      return a[0] < b[0];
    };
    ad_utility::CompressedExternalIdTableSorter<SortByPSO, 3> writer{
        filename, 3, ad_utility::MemorySize::bytes(memForStxxl),
        ad_utility::testing::makeAllocator()};

    auto runPush = [&writer, &numInputRows, &generateRandomRow]() {
      for (auto i : std::views::iota(0UL, numInputRows)) {
        writer.push(generateRandomRow());
        if (i % 100'000'000 == 0) {
          LOG(INFO) << "Pushed " << i << " lines" << std::endl;
        }
      }
    };
    auto run = [&writer]() {
      auto view = writer.sortedView();
      double res = 0;
      size_t count = 0;
      for (const auto& block : view) {
        res += std::sqrt(static_cast<double>(
            block[0].getBits() + block[1].getBits() + block[2].getBits()));
        ++count;
        if (count % 100'000'000 == 0) {
          LOG(INFO) << "Merged " << count << " lines" << std::endl;
        }
      }
      std::cout << res << ' ' << count << std::endl;
    };

    results.addMeasurement("SortingAndWritingBlocks", runPush);
    std::cout << "Finish sorting" << std::endl;
    results.addMeasurement("ReadAndMerge", run);
    std::cout << "Finish merging" << std::endl;

    auto pushStxxl = [&sorter, &numInputRows, &generateRandomRow]() {
      for ([[maybe_unused]] auto i : std::views::iota(0UL, numInputRows)) {
        sorter.push(generateRandomRow());
      }
    };

    auto drainStxxl = [&sorter]() {
      double i = 0;
      size_t count = 0;
      for ([[maybe_unused]] const auto& row : sorter.sortedView()) {
        i += std::sqrt(static_cast<double>(row[0].getBits() + row[1].getBits() +
                                           row[2].getBits()));
        ++count;
      }
      std::cout << i << ' ' << count << std::endl;
    };

    results.addMeasurement("Time using stxxl for push", pushStxxl);
    std::cout << "Finished pushing stxxl" << std::endl;
    results.addMeasurement("Time using stxxl for pull", drainStxxl);
    std::cout << "Finished merging stxxl" << std::endl;

    std::cout << "\nNum blocks in stxxl" << sorter.numBlocks() << std::endl;

    return results;
  }
};
AD_REGISTER_BENCHMARK(IdTableCompressedWriterBenchmarks);
}  // namespace ad_benchmark
#endif
