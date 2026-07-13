// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// Standalone microbenchmarks for the three core algorithms that are most
// sensitive to the underlying `Id` storage format: the merge join on a
// single join column without UNDEF values, the in-memory sort of a fully
// materialized `IdTable` by a single column, and the GROUP BY on sorted
// input. Each kernel exercises the actual production code path.
//
// This is deliberately a plain command-line binary (and not part of the
// macro-based benchmark infrastructure), s.t. a single kernel can be
// isolated cleanly for `perf record`/`perf stat`.
//
// Note: This file deliberately only uses interfaces that exist both on
// `master` (packed 64-bit IDs) and on the `64-bit-payloads` branch (split
// payload/datatype storage), s.t. the identical file can be compiled on
// both branches to obtain a true baseline comparison.
//
// Usage examples:
//   IdKernelBenchmarks join    --rows 10000000 --cols 2 --types 3 --reps 5
//   IdKernelBenchmarks sort    --rows 10000000 --cols 5 --types 3 --reps 5
//   IdKernelBenchmarks groupby --rows 10000000 --groupsize 100 --reps 5

#include <chrono>
#include <cstdint>
#include <deque>
#include <iostream>
#include <random>
#include <string>
#include <vector>

#include "../test/engine/ValuesForTesting.h"
#include "../test/util/IndexTestHelpers.h"
#include "engine/AddCombinedRowToTable.h"
#include "engine/Distinct.h"
#include "engine/ExistsJoin.h"
#include "engine/Filter.h"
#include "engine/GroupBy.h"
#include "engine/QueryExecutionTree.h"
#include "engine/sparqlExpressions/AggregateExpression.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/RelationalExpressions.h"
#include "global/Id.h"
#include "index/IdTableUtils.h"
#include "util/AllocatorWithLimit.h"
#include "util/JoinAlgorithms/JoinAlgorithms.h"
#include "util/JoinAlgorithms/JoinColumnMapping.h"

namespace {

using namespace std::chrono;

// Command-line options with defaults.
struct Options {
  std::string kernel_;
  size_t numRows_ = 10'000'000;
  // Number of payload columns (in addition to the join/sort/group column).
  size_t numPayloadCols_ = 2;
  // Number of distinct datatypes in the key column (1 = only `VocabIndex`,
  // up to 3 = `VocabIndex`, `Int`, `Double`).
  size_t numTypes_ = 1;
  // Average number of occurrences of each distinct key (controls the number
  // of groups for `groupby` and the match density for `join`).
  size_t groupSize_ = 10;
  size_t reps_ = 5;
  uint64_t seed_ = 42;
  // Number of rows per block for the `join-blocks` kernel.
  size_t blockSize_ = 100'000;
  // Per-mille of `LocalVocabIndex` IDs in the key column.
  size_t localVocabPerMille_ = 0;
};

// _____________________________________________________________________________
Options parseOptions(int argc, char** argv) {
  Options opts;
  if (argc < 2) {
    std::cerr << "Usage: " << argv[0]
              << " <join|sort|groupby> [--rows N] [--cols C] [--types K] "
                 "[--groupsize G] [--reps R] [--seed S]\n";
    std::exit(1);
  }
  opts.kernel_ = argv[1];
  for (int i = 2; i + 1 < argc; i += 2) {
    std::string flag = argv[i];
    uint64_t value = std::stoull(argv[i + 1]);
    if (flag == "--rows") {
      opts.numRows_ = value;
    } else if (flag == "--cols") {
      opts.numPayloadCols_ = value;
    } else if (flag == "--types") {
      opts.numTypes_ = value;
    } else if (flag == "--groupsize") {
      opts.groupSize_ = value;
    } else if (flag == "--reps") {
      opts.reps_ = value;
    } else if (flag == "--seed") {
      opts.seed_ = value;
    } else if (flag == "--blocksize") {
      opts.blockSize_ = value;
    } else if (flag == "--lvpermille") {
      opts.localVocabPerMille_ = value;
    } else {
      std::cerr << "Unknown flag " << flag << '\n';
      std::exit(1);
    }
  }
  return opts;
}

// Make an `Id` of one of up to three datatypes from an integer key. The
// mapping is strictly monotonic in (`type`, `key`), so sorting a vector of
// keys and mapping them yields a sorted vector of `Id`s.
Id makeIdOfType(size_t type, uint64_t key) {
  switch (type) {
    case 0:
      return Id::makeFromVocabIndex(VocabIndex::make(key));
    case 1:
      return Id::makeFromInt(static_cast<int64_t>(key));
    default:
      return Id::makeFromDouble(static_cast<double>(key));
  }
}

// The datatype-major sort order of the types created by `makeIdOfType` (for
// nonnegative keys): `Int` < `Double` < `VocabIndex`.
constexpr std::array<size_t, 3> typeSortOrder{1, 2, 0};

ad_utility::AllocatorWithLimit<Id>& allocator() {
  static ad_utility::AllocatorWithLimit<Id> alloc{
      ad_utility::makeAllocationMemoryLeftThreadsafeObject(
          ad_utility::MemorySize::gigabytes(20))};
  return alloc;
}

// A pool that owns `LocalVocabEntry`s for the benchmark (the entries must
// outlive all tables that contain IDs pointing to them).
struct LocalVocabPool {
  std::deque<LocalVocabEntry> entries_;
  ad_utility::HashMap<uint64_t, const LocalVocabEntry*> byKey_;

  Id getId(uint64_t key, const LocalVocabContext& context) {
    auto it = byKey_.find(key);
    if (it == byKey_.end()) {
      std::string word = absl::StrFormat("\"lv%012d\"", key);
      entries_.push_back(
          LocalVocabEntry::fromStringRepresentation(std::move(word), context));
      it = byKey_.emplace(key, &entries_.back()).first;
    }
    return Id::makeFromLocalVocabIndex(it->second);
  }
};

LocalVocabPool& localVocabPool() {
  static LocalVocabPool pool;
  return pool;
}

// Generate a table with `numRows` rows and `1 + numPayloadCols` columns.
// Column 0 contains keys drawn uniformly from `[0, numRows / groupSize)`,
// mapped to `numTypes` different datatypes; if `sorted`, column 0 is sorted
// (datatype-major, exactly as the engine would produce it). The payload
// columns contain random `Int` ids.
IdTable makeTable(const Options& opts, bool sorted, uint64_t seedOffset) {
  std::mt19937_64 rng(opts.seed_ + seedOffset);
  size_t numKeys = std::max<size_t>(1, opts.numRows_ / opts.groupSize_);
  std::uniform_int_distribution<uint64_t> keyDist(0, numKeys - 1);

  // Draw (type, key) pairs and sort them if requested.
  std::vector<std::pair<size_t, uint64_t>> keys;
  keys.reserve(opts.numRows_);
  for (size_t i = 0; i < opts.numRows_; ++i) {
    uint64_t key = keyDist(rng);
    size_t type =
        opts.numTypes_ <= 1
            ? 0
            : typeSortOrder[key % std::min<size_t>(opts.numTypes_, size_t{3})];
    keys.emplace_back(type, key);
  }
  if (sorted) {
    // Sort by the datatype-major order of the resulting `Id`s. Note: For a
    // single type this is simply sorting by key.
    std::sort(keys.begin(), keys.end(), [](const auto& a, const auto& b) {
      auto rank = [](const auto& p) {
        auto it =
            std::find(typeSortOrder.begin(), typeSortOrder.end(), p.first);
        return std::pair{it - typeSortOrder.begin(), p.second};
      };
      return rank(a) < rank(b);
    });
  }

  // Materialize the key ids (a fraction of them may be `LocalVocabIndex`
  // IDs). In the latter case the ids are sorted semantically afterwards.
  std::vector<Id> keyIds;
  keyIds.reserve(opts.numRows_);
  if (opts.localVocabPerMille_ > 0) {
    const auto& context = ad_utility::testing::getQec()->getLocalVocabContext();
    for (const auto& [type, key] : keys) {
      if (key % 1000 < opts.localVocabPerMille_) {
        keyIds.push_back(localVocabPool().getId(key, context));
      } else {
        keyIds.push_back(makeIdOfType(type, key));
      }
    }
    if (sorted) {
      std::sort(keyIds.begin(), keyIds.end());
    }
  } else {
    for (const auto& [type, key] : keys) {
      keyIds.push_back(makeIdOfType(type, key));
    }
  }

  IdTable table{1 + opts.numPayloadCols_, allocator()};
  table.resize(opts.numRows_);
  {
    decltype(auto) keyCol = table.getColumn(0);
    for (size_t i = 0; i < opts.numRows_; ++i) {
      keyCol[i] = keyIds[i];
    }
  }
  for (size_t c = 1; c <= opts.numPayloadCols_; ++c) {
    decltype(auto) col = table.getColumn(c);
    for (size_t i = 0; i < opts.numRows_; ++i) {
      col[i] = Id::makeFromInt(static_cast<int64_t>(rng() >> 5));
    }
  }
  return table;
}

// A simple cross-branch-stable checksum over a table (row count and a hash
// of the datatypes and row positions of the key column).
uint64_t checksum(const auto& table) {
  uint64_t result = table.numRows() * 31 + table.numColumns();
  decltype(auto) col = table.getColumn(0);
  size_t stride = std::max<size_t>(1, table.numRows() / 1000);
  for (size_t i = 0; i < table.numRows(); i += stride) {
    result = result * 1099511628211ull +
             static_cast<uint64_t>(static_cast<Id>(col[i]).getDatatype());
  }
  return result;
}

// Run `kernel` `reps` times and report the times and the median.
template <typename Setup, typename Kernel>
void runAndReport(const std::string& name, size_t reps, Setup setup,
                  Kernel kernel) {
  std::vector<double> times;
  for (size_t i = 0; i < reps; ++i) {
    auto state = setup();
    auto start = steady_clock::now();
    auto result = kernel(std::move(state));
    auto stop = steady_clock::now();
    double ms = duration_cast<microseconds>(stop - start).count() / 1000.0;
    times.push_back(ms);
    std::cout << name << " rep " << i << ": " << ms << " ms (checksum "
              << result << ")" << std::endl;
  }
  std::sort(times.begin(), times.end());
  std::cout << name << " MEDIAN: " << times[times.size() / 2] << " ms"
            << std::endl;
}

// _____________________________________________________________________________
// Kernel 1: The merge join on a single join column without UNDEF values.
// This replicates the exact code path of `JoinImpl::join` (zipper join +
// `AddCombinedRowToIdTable`).
void benchJoin(const Options& opts) {
  IdTable left = makeTable(opts, true, 0);
  IdTable right = makeTable(opts, true, 1);

  auto handle = std::make_shared<ad_utility::CancellationHandle<>>();

  auto setup = [&]() { return 0; };
  auto kernel = [&](int) {
    auto a = left.asStaticView<0>();
    auto b = right.asStaticView<0>();
    ad_utility::JoinColumnMapping joinColumnData{
        {{0, 0}}, a.numColumns(), b.numColumns()};
    auto joinColumnL = a.getColumn(0);
    auto joinColumnR = b.getColumn(0);
    auto aPermuted = a.asColumnSubsetView(joinColumnData.permutationLeft());
    auto bPermuted = b.asColumnSubsetView(joinColumnData.permutationRight());

    IdTable result{a.numColumns() + b.numColumns() - 1, allocator()};
    auto rowAdder = ad_utility::AddCombinedRowToIdTable(
        1, aPermuted, bPermuted, std::move(result), handle);
    auto addRow = [beginLeft = joinColumnL.begin(),
                   beginRight = joinColumnR.begin(),
                   &rowAdder](const auto& itLeft, const auto& itRight) {
      rowAdder.addRow(itLeft - beginLeft, itRight - beginRight);
    };
    [[maybe_unused]] auto numOutOfOrder = ad_utility::zipperJoinWithUndef(
        joinColumnL, joinColumnR, ql::ranges::less{}, addRow, ad_utility::noop,
        ad_utility::noop, {}, ad_utility::noop);
    auto resultTable = std::move(rowAdder).resultTable();
    resultTable.setColumnSubset(joinColumnData.permutationResult());
    return checksum(resultTable);
  };
  runAndReport("join", opts.reps_, setup, kernel);
}

// _____________________________________________________________________________
// Kernel 1b: The same merge join, but through the blockwise (lazy) zipper
// join that is used for joins of prefiltered index scans
// (`zipperJoinForBlocksWithoutUndef`).
void benchJoinBlocks(const Options& opts) {
  IdTable left = makeTable(opts, true, 0);
  IdTable right = makeTable(opts, true, 1);
  auto handle = std::make_shared<ad_utility::CancellationHandle<>>();

  // Split a table into blocks of `opts.blockSize_` rows.
  auto toBlocks = [&](const IdTable& table) {
    std::vector<ad_utility::IdTableAndFirstCols<1, IdTable>> blocks;
    for (size_t begin = 0; begin < table.numRows(); begin += opts.blockSize_) {
      size_t size = std::min(opts.blockSize_, table.numRows() - begin);
      IdTable block{table.numColumns(), allocator()};
      block.insertAtEnd(table, begin, begin + size);
      blocks.push_back(ad_utility::makeIdTableAndFirstCols<1>(std::move(block),
                                                              LocalVocab{}));
    }
    return blocks;
  };
  // Note: The blocks have to be recreated for every repetition, because
  // the blockwise join consumes them.
  auto setup = [&]() { return std::pair{toBlocks(left), toBlocks(right)}; };
  auto kernel = [&](auto blocks) {
    IdTable result{left.numColumns() + right.numColumns() - 1, allocator()};
    auto rowAdder =
        ad_utility::AddCombinedRowToIdTable(1, std::move(result), handle);
    ad_utility::zipperJoinForBlocksWithoutUndef(blocks.first, blocks.second,
                                                std::less{}, rowAdder);
    auto resultTable = std::move(rowAdder).resultTable();
    return checksum(resultTable);
  };
  runAndReport("join-blocks", opts.reps_, setup, kernel);
}

// _____________________________________________________________________________
// Kernel 2: Sort a fully materialized `IdTable` by a single column. This is
// the exact code path of `Sort::computeResultInMemory`.
void benchSort(const Options& opts) {
  IdTable input = makeTable(opts, false, 0);

  auto setup = [&]() { return input.clone(); };
  auto kernel = [&](IdTable table) {
    IdTableUtils::sort(table, std::vector<ColumnIndex>{0});
    return checksum(table);
  };
  runAndReport("sort", opts.reps_, setup, kernel);
}

// _____________________________________________________________________________
// Split a table into blocks of `blockSize` rows (as inputs for the lazy
// operation variants).
std::vector<Result::IdTableVocabPair> splitIntoBlocks(const IdTable& table,
                                                      size_t blockSize) {
  std::vector<Result::IdTableVocabPair> blocks;
  for (size_t begin = 0; begin < table.numRows(); begin += blockSize) {
    size_t size = std::min(blockSize, table.numRows() - begin);
    IdTable block{table.numColumns(), allocator()};
    block.insertAtEnd(table, begin, begin + size);
    blocks.emplace_back(std::move(block), LocalVocab{});
  }
  return blocks;
}

// Consume a `Result` (materialized or lazy) and return a checksum.
uint64_t consumeResult(std::shared_ptr<const Result> result) {
  if (result->isFullyMaterialized()) {
    return checksum(result->idTableView());
  }
  uint64_t sum = 1;
  for (auto& pair : result->idTables()) {
    sum = sum * 31 + checksum(pair.idTable_);
  }
  return sum;
}

// Make a `ValuesForTesting` tree, either fully materialized or as lazy
// blocks.
std::shared_ptr<QueryExecutionTree> makeInputTree(
    QueryExecutionContext* qec, const IdTable& table,
    std::vector<std::optional<Variable>> vars, bool lazy, bool sorted,
    size_t blockSize) {
  std::vector<ColumnIndex> sortedColumns =
      sorted ? std::vector<ColumnIndex>{0} : std::vector<ColumnIndex>{};
  if (lazy) {
    return ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, splitIntoBlocks(table, blockSize), std::move(vars), false,
        sortedColumns);
  }
  return ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, table.clone(), std::move(vars), false, sortedColumns, LocalVocab{},
      std::nullopt, true);
}

// _____________________________________________________________________________
// Kernel 4: FILTER with a conceptually simple expression (`?b < constant`
// with ~50% selectivity, where `?b` is a random `Int` column).
void benchFilter(const Options& opts, bool lazy) {
  using namespace sparqlExpression;
  auto qec = ad_utility::testing::getQec();
  IdTable input = makeTable(opts, false, 0);
  std::vector<std::optional<Variable>> vars;
  for (size_t i = 0; i < input.numColumns(); ++i) {
    vars.emplace_back(Variable{absl::StrCat("?v", i)});
  }
  // The payload columns contain uniform 59-bit random integers (which are
  // exactly representable on both ID layouts), so this threshold yields a
  // selectivity of ~50%.
  Id threshold = Id::makeFromInt(int64_t{1} << 58);

  auto setup = [&]() {
    qec->getQueryTreeCache().clearAll();
    auto tree = makeInputTree(qec, input, vars, lazy, false, opts.blockSize_);
    auto expression = std::make_unique<LessThanExpression>(
        std::array<SparqlExpression::Ptr, 2>{
            std::make_unique<VariableExpression>(Variable{"?v1"}),
            std::make_unique<IdExpression>(threshold)});
    return std::make_unique<Filter>(
        qec, std::move(tree),
        SparqlExpressionPimpl{std::move(expression), "?v1 < threshold"});
  };
  auto kernel = [&](std::unique_ptr<Filter> filter) {
    return consumeResult(
        filter->getResult(false, lazy ? ComputationMode::LAZY_IF_SUPPORTED
                                      : ComputationMode::FULLY_MATERIALIZED));
  };
  runAndReport(lazy ? "filter-lazy" : "filter", opts.reps_, setup, kernel);
}

// _____________________________________________________________________________
// Kernel 5: DISTINCT on the sorted key column (column 0).
void benchDistinct(const Options& opts, bool lazy) {
  auto qec = ad_utility::testing::getQec();
  IdTable input = makeTable(opts, true, 0);
  std::vector<std::optional<Variable>> vars;
  for (size_t i = 0; i < input.numColumns(); ++i) {
    vars.emplace_back(Variable{absl::StrCat("?v", i)});
  }

  auto setup = [&]() {
    qec->getQueryTreeCache().clearAll();
    auto tree = makeInputTree(qec, input, vars, lazy, true, opts.blockSize_);
    return std::make_unique<Distinct>(qec, std::move(tree),
                                      std::vector<ColumnIndex>{0});
  };
  auto kernel = [&](std::unique_ptr<Distinct> distinct) {
    return consumeResult(
        distinct->getResult(false, lazy ? ComputationMode::LAZY_IF_SUPPORTED
                                        : ComputationMode::FULLY_MATERIALIZED));
  };
  runAndReport(lazy ? "distinct-lazy" : "distinct", opts.reps_, setup, kernel);
}

// _____________________________________________________________________________
// Kernel 6: EXISTS join on the sorted key column.
void benchExists(const Options& opts, bool lazy) {
  auto qec = ad_utility::testing::getQec();
  IdTable left = makeTable(opts, true, 0);
  IdTable right = makeTable(opts, true, 1);
  std::vector<std::optional<Variable>> varsLeft;
  std::vector<std::optional<Variable>> varsRight;
  for (size_t i = 0; i < left.numColumns(); ++i) {
    varsLeft.emplace_back(Variable{absl::StrCat("?v", i)});
    varsRight.emplace_back(
        Variable{absl::StrCat(i == 0 ? "?v" : "?w", i == 0 ? 0 : i)});
  }

  auto setup = [&]() {
    qec->getQueryTreeCache().clearAll();
    auto leftTree =
        makeInputTree(qec, left, varsLeft, lazy, true, opts.blockSize_);
    auto rightTree =
        makeInputTree(qec, right, varsRight, lazy, true, opts.blockSize_);
    return std::make_unique<ExistsJoin>(
        qec, std::move(leftTree), std::move(rightTree), Variable{"?exists"});
  };
  auto kernel = [&](std::unique_ptr<ExistsJoin> exists) {
    return consumeResult(
        exists->getResult(false, lazy ? ComputationMode::LAZY_IF_SUPPORTED
                                      : ComputationMode::FULLY_MATERIALIZED));
  };
  runAndReport(lazy ? "exists-lazy" : "exists", opts.reps_, setup, kernel);
}

// _____________________________________________________________________________
// Kernel 3: GROUP BY on sorted input with a single group column and a
// `SUM(?b)` aggregate. This exercises the sorted (non-hash-map) path of
// `GroupByImpl` through the full operation.
void benchGroupBy(const Options& opts, bool lazy) {
  using namespace sparqlExpression;
  auto qec = ad_utility::testing::getQec();
  Variable varA{"?a"}, varB{"?b"};

  Options tableOpts = opts;
  tableOpts.numPayloadCols_ = 1;
  IdTable input = makeTable(tableOpts, true, 0);

  auto setup = [&]() {
    qec->getQueryTreeCache().clearAll();
    std::vector<std::optional<Variable>> vars{varA, varB};
    auto valuesTree = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, input.clone(), vars, false, std::vector<ColumnIndex>{0},
        LocalVocab{}, std::nullopt, !lazy);
    auto sumExpr = std::make_unique<SumExpression>(
        false, std::make_unique<VariableExpression>(varB));
    Alias alias{SparqlExpressionPimpl{std::move(sumExpr), "SUM(?b)"},
                Variable{"?sum"}};
    return std::make_unique<GroupBy>(qec, std::vector<Variable>{varA},
                                     std::vector<Alias>{std::move(alias)},
                                     std::move(valuesTree));
  };
  auto kernel = [&](std::unique_ptr<GroupBy> groupBy) {
    auto result = groupBy->getResult(false);
    return checksum(result->idTableView());
  };
  runAndReport(lazy ? "groupby-lazy" : "groupby", opts.reps_, setup, kernel);
}

}  // namespace

// _____________________________________________________________________________
int main(int argc, char** argv) {
  Options opts = parseOptions(argc, argv);
  std::cout << "kernel=" << opts.kernel_ << " rows=" << opts.numRows_
            << " payloadCols=" << opts.numPayloadCols_
            << " types=" << opts.numTypes_ << " groupsize=" << opts.groupSize_
            << " reps=" << opts.reps_ << std::endl;
  if (opts.kernel_ == "join") {
    benchJoin(opts);
  } else if (opts.kernel_ == "join-blocks") {
    benchJoinBlocks(opts);
  } else if (opts.kernel_ == "sort") {
    benchSort(opts);
  } else if (opts.kernel_ == "filter") {
    benchFilter(opts, false);
  } else if (opts.kernel_ == "filter-lazy") {
    benchFilter(opts, true);
  } else if (opts.kernel_ == "distinct") {
    benchDistinct(opts, false);
  } else if (opts.kernel_ == "distinct-lazy") {
    benchDistinct(opts, true);
  } else if (opts.kernel_ == "exists") {
    benchExists(opts, false);
  } else if (opts.kernel_ == "exists-lazy") {
    benchExists(opts, true);
  } else if (opts.kernel_ == "groupby") {
    benchGroupBy(opts, false);
  } else if (opts.kernel_ == "groupby-lazy") {
    benchGroupBy(opts, true);
  } else {
    std::cerr << "Unknown kernel " << opts.kernel_ << '\n';
    return 1;
  }
  return 0;
}
