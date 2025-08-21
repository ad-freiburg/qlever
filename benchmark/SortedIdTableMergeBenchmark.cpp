// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#include "../benchmark/infrastructure/Benchmark.h"
#include "index/SortedIdTableMerge.h"
#include "util/Random.h"

namespace ad_benchmark {

class SortedIdTableMergeBenchmark : public BenchmarkInterface {
  // This function tries to emulate how TextRecordIndex'es appear in a word scan
  // result.`It returns TextRecordIndex Ids in ascending order while sometimes
  // repeating Ids and skipping some.
  cppcoro::generator<Id> generateRandomAscendingTextRecordIds(
      size_t upperBound) {
    auto boolGen = ad_utility::RandomBoolGenerator(1, 3);
    auto howOftenGen = ad_utility::SlowRandomIntGenerator<size_t>(1, 6);
    for (size_t i = 0, j = 0; i < upperBound; ++i, ++j) {
      if (boolGen()) {
        ;
        size_t upTo = i + howOftenGen();
        for (; i < upperBound && i < upTo; ++i) {
          co_yield Id::makeFromTextRecordIndex(TextRecordIndex::make(j));
        }
      }
    }
  }

  cppcoro::generator<Id> generateRandomIds(size_t upperBound) {
    auto randomGen =
        ad_utility::SlowRandomIntGenerator<size_t>(0, ValueId::maxIndex);
    for (size_t i = 0; i < upperBound; ++i) {
      co_yield Id::makeFromInt(randomGen());
    }
  }

  static void fillColumnWithGenerator(IdTable& idTable, size_t colIndex,
                                      cppcoro::generator<Id>& generator) {
    constexpr size_t blockWriteSize = 10'000;
    auto col = idTable.getColumn(colIndex);
    auto out = col.begin();
    std::array<Id, blockWriteSize> buffer;
    size_t buffered = 0;
    for (auto&& id : generator) {
      buffer[buffered++] = std::move(id);
      if (buffer.size() >= blockWriteSize) {
        ql::ranges::move(buffer.begin(), buffer.end(), out);
        buffered = 0;
      }
    }
    if (buffered > 0) {
      ql::ranges::move(buffer.begin(), buffer.begin() + buffered, out);
    }
  }

  std::string name() const final { return "SortedIdTableMergeBenchmark"; }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results{};

    auto createRandomIdTable = [this](size_t numRows) {
      auto allocator = ad_utility::makeUnlimitedAllocator<Id>();
      IdTable idTable{3, allocator};
      idTable.resize(numRows);

      // Fill first col
      auto firstColGen = generateRandomAscendingTextRecordIds(idTable.size());
      fillColumnWithGenerator(idTable, 0, firstColGen);

      // Fill second and third col
      auto secondColGen = generateRandomIds(idTable.size());
      fillColumnWithGenerator(idTable, 1, secondColGen);
      auto thirdColGen = generateRandomIds(idTable.size());
      fillColumnWithGenerator(idTable, 2, thirdColGen);

      return idTable;
    };

    auto createVectorOfRandomIdTables = [this, &createRandomIdTable](
                                            size_t numTables, size_t numRows) {
      std::vector<IdTable> idTables;
      idTables.reserve(numTables);
      for (size_t i = 0; i < numTables; ++i) {
        idTables.emplace_back(createRandomIdTable(numRows));
      }
      return idTables;
    };

    // Creating 4 Tables to merge
    auto idTables1 = createVectorOfRandomIdTables(4, 100'000);
    auto moveIdTables1 = std::move(idTables1);

    auto idTables2 = createVectorOfRandomIdTables(4, 100'000);
    auto moveIdTables2 = std::move(idTables2);

    auto myMerge = [](std::vector<IdTable> idTables) {
      return SortedIdTableMerge::mergeIdTables(
          std::move(idTables), ad_utility::makeUnlimitedAllocator<Id>());
    };

    auto appendAndSort = [](std::vector<IdTable> idTables) {
      IdTable result{3, ad_utility::makeUnlimitedAllocator<Id>()};
      result.reserve(
          std::accumulate(idTables.begin(), idTables.end(), size_t{0},
                          [](size_t acc, const IdTable& partialResult) {
                            return acc + partialResult.numRows();
                          }));
      for (const auto& partialResult : idTables) {
        result.insertAtEnd(partialResult);
      }
      auto toSort = std::move(result).toStatic<3>();
      // Sort the table
      ql::ranges::sort(toSort, [](const auto& a, const auto& b) {
        return ql::ranges::lexicographical_compare(
            std::begin(a), std::end(a), std::begin(b), std::end(b),
            [](const Id& x, const Id& y) {
              return x.compareWithoutLocalVocab(y) < 0;
            });
      });
      return std::move(toSort).toDynamic<>();
    };

    results.addMeasurement(
        "Merge 4 tables using SortedIdTableMerge",
        [&myMerge, &moveIdTables1]() { myMerge(std::move(moveIdTables1)); });

    results.addMeasurement("Merge 4 tables using append and sort",
                           [&appendAndSort, &moveIdTables2]() {
                             appendAndSort(std::move(moveIdTables2));
                           });

    return results;
  }
};

// Registering the benchmarks
AD_REGISTER_BENCHMARK(SortedIdTableMergeBenchmark);

}  // namespace ad_benchmark
