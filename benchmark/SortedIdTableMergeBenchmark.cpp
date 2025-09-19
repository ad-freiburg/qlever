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

  static void fillColumnWithGenerator(IdTableStatic<3>& idTable,
                                      size_t colIndex,
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
    constexpr size_t numTablesToMerge = 100;
    constexpr size_t numRowsPerTable = 100'000;
    BenchmarkResults results{};

    auto createRandomIdTable = [this](size_t numRows) {
      auto allocator = ad_utility::makeUnlimitedAllocator<Id>();
      IdTableStatic<3> idTable{3, allocator};
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

    auto createVectorOfRandomIdTables = [&createRandomIdTable](size_t numTables,
                                                               size_t numRows) {
      std::vector<IdTable> idTables;
      idTables.reserve(numTables);
      for (size_t i = 0; i < numTables; ++i) {
        idTables.emplace_back(
            std::move(createRandomIdTable(numRows)).toDynamic());
      }
      return idTables;
    };

    auto myMerge = [](const std::vector<IdTable>& idTables) {
      return sortedIdTableMerge::mergeIdTables<3, 1>(
          idTables, ad_utility::makeUnlimitedAllocator<Id>(), {0},
          sortedIdTableMerge::DirectComparator{});
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

    auto idTables1 =
        createVectorOfRandomIdTables(numTablesToMerge, numRowsPerTable);
    auto moveIdTables1 = std::move(idTables1);
    results.addMeasurement(
        absl::StrCat("Merge ", numTablesToMerge, " tables with ",
                     numRowsPerTable, " rows using append and sort"),
        [&appendAndSort, &moveIdTables1]() {
          appendAndSort(std::move(moveIdTables1));
        });
    auto idTables2 =
        createVectorOfRandomIdTables(numTablesToMerge, numRowsPerTable);
    results.addMeasurement(
        absl::StrCat("Merge ", numTablesToMerge, " tables with ",
                     numRowsPerTable, " rows using second SortedIdTableMerge"),
        [&myMerge, &idTables2]() { myMerge(idTables2); });

    return results;
  }
};

// Registering the benchmarks
AD_REGISTER_BENCHMARK(SortedIdTableMergeBenchmark);

}  // namespace ad_benchmark
