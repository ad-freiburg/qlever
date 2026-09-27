// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include "engine/LeapfrogTriangleJoin.h"

#include <absl/strings/str_cat.h>

#include <algorithm>
#include <functional>

#include "engine/LeapfrogTriejoin.h"

using ql::engine::wcoj::KeyRun;
using ql::engine::wcoj::LeapfrogIterator;
using ql::engine::wcoj::LeapfrogJoin;

// _____________________________________________________________________________
LeapfrogTriangleJoin::LeapfrogTriangleJoin(
    QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> xy,
    std::shared_ptr<QueryExecutionTree> yz,
    std::shared_ptr<QueryExecutionTree> xz, Variable x, Variable y, Variable z)
    : Operation{qec},
      children_{std::move(xy), std::move(yz), std::move(xz)},
      variables_{std::move(x), std::move(y), std::move(z)} {
  AD_CONTRACT_CHECK(variables_[0] != variables_[1] &&
                    variables_[0] != variables_[2] &&
                    variables_[1] != variables_[2]);
  // The indices of the two variables of each input in `variables_`.
  static constexpr std::array<std::array<size_t, 2>, 3> edges{
      {{0, 1}, {1, 2}, {0, 2}}};
  for (size_t i = 0; i < 3; ++i) {
    auto& child = children_[i];
    AD_CONTRACT_CHECK(child->getResultWidth() == 2);
    const auto& varCols = child->getVariableColumns();
    std::array<ColumnIndex, 2> cols{};
    for (size_t j = 0; j < 2; ++j) {
      const auto& var = variables_[edges[i][j]];
      AD_CONTRACT_CHECK(varCols.contains(var));
      const auto& info = varCols.at(var);
      AD_CONTRACT_CHECK(info.mightContainUndef_ ==
                        ColumnIndexAndTypeInfo::UndefStatus::AlwaysDefined);
      cols[j] = info.columnIndex_;
    }
    child = QueryExecutionTree::createSortedTree(std::move(child),
                                                 {cols[0], cols[1]});
    columns_[i] = cols;
  }
}

// _____________________________________________________________________________
qlm::vector<QueryExecutionTree*> LeapfrogTriangleJoin::getChildrenImpl() const {
  return {{children_[XY].get(), children_[YZ].get(), children_[XZ].get()},
          allocator()};
}

// _____________________________________________________________________________
std::string LeapfrogTriangleJoin::getCacheKeyImpl() const {
  std::string key = "LEAPFROG_TRIANGLE_JOIN\n";
  for (size_t i = 0; i < 3; ++i) {
    absl::StrAppend(&key, children_[i]->getCacheKey(),
                    " columns: ", columns_[i][0], " ", columns_[i][1], "\n");
  }
  return key;
}

// _____________________________________________________________________________
std::string LeapfrogTriangleJoin::getDescriptor() const {
  return absl::StrCat("LeapfrogTriangleJoin on ", variables_[0].name(), " ",
                      variables_[1].name(), " ", variables_[2].name());
}

// _____________________________________________________________________________
size_t LeapfrogTriangleJoin::getResultWidth() const { return 3; }

// _____________________________________________________________________________
size_t LeapfrogTriangleJoin::getCostEstimate() {
  // Each input is read once; the result is written once.
  size_t cost = getSizeEstimateBeforeLimit();
  for (const auto& child : children_) {
    cost += child->getCostEstimate() + child->getSizeEstimate();
  }
  return cost;
}

// _____________________________________________________________________________
uint64_t LeapfrogTriangleJoin::getSizeEstimateBeforeLimit() {
  // A simple estimate: the size of the smallest input.
  uint64_t estimate = children_[0]->getSizeEstimate();
  for (const auto& child : children_) {
    estimate = std::min(estimate, child->getSizeEstimate());
  }
  return estimate;
}

// _____________________________________________________________________________
float LeapfrogTriangleJoin::getMultiplicity([[maybe_unused]] size_t col) {
  return 1.0f;
}

// _____________________________________________________________________________
bool LeapfrogTriangleJoin::knownEmptyResult() {
  return ql::ranges::any_of(
      children_, [](auto& child) { return child->knownEmptyResult(); });
}

// _____________________________________________________________________________
std::unique_ptr<Operation> LeapfrogTriangleJoin::cloneImpl() const {
  auto copy = std::make_unique<LeapfrogTriangleJoin>(*this);
  for (auto& child : copy->children_) {
    child = child->clone();
  }
  return copy;
}

// _____________________________________________________________________________
std::vector<ColumnIndex> LeapfrogTriangleJoin::resultSortedOn() const {
  return {0, 1, 2};
}

// _____________________________________________________________________________
VariableToColumnMap LeapfrogTriangleJoin::computeVariableToColumnMap() const {
  VariableToColumnMap map;
  for (size_t i = 0; i < 3; ++i) {
    map.emplace(variables_[i], makeAlwaysDefinedColumn(i));
  }
  return map;
}

// _____________________________________________________________________________
Result LeapfrogTriangleJoin::computeResult(
    [[maybe_unused]] bool requestLaziness) {
  std::array<std::shared_ptr<const Result>, 3> results;
  for (size_t i = 0; i < 3; ++i) {
    results[i] = children_[i]->getResult();
    checkCancellation();
  }
  // The two sorted columns of each input.
  auto column = [&](size_t input, size_t level) {
    return results[input]->idTableView().getColumn(columns_[input][level]);
  };
  auto xyX = column(XY, 0);
  auto xyY = column(XY, 1);
  auto yzY = column(YZ, 0);
  auto yzZ = column(YZ, 1);
  auto xzX = column(XZ, 0);
  auto xzZ = column(XZ, 1);
  auto subrange = [](auto span, const KeyRun& run) {
    return span.subspan(run.begin_, run.size());
  };

  IdTable result{3, allocator()};
  // Level `x`: the keys of the first level of the `xy` and `xz` tries.
  std::array xIterators{LeapfrogIterator{xyX}, LeapfrogIterator{xzX}};
  LeapfrogJoin::forEachCommonKey(xIterators, [&](Id x, const auto& xRuns) {
    checkCancellation();
    // Level `y`: the `y` values below `x` in the `xy` trie and the keys of the
    // first level of the `yz` trie.
    std::array yIterators{LeapfrogIterator{subrange(xyY, xRuns[0])},
                          LeapfrogIterator{yzY}};
    LeapfrogJoin::forEachCommonKey(yIterators, [&](Id y, const auto& yRuns) {
      // Level `z`: the `z` values below `y` in the `yz` trie and below `x` in
      // the `xz` trie.
      std::array zIterators{LeapfrogIterator{subrange(yzZ, yRuns[1])},
                            LeapfrogIterator{subrange(xzZ, xRuns[1])}};
      LeapfrogJoin::forEachCommonKey(zIterators, [&](Id z, const auto& zRuns) {
        size_t multiplicity =
            yRuns[0].size() * zRuns[0].size() * zRuns[1].size();
        for (size_t i = 0; i < multiplicity; ++i) {
          result.push_back({x, y, z});
        }
      });
    });
  });
  checkCancellation();

  return {std::move(result), resultSortedOn(),
          Result::getMergedLocalVocab(std::array{std::cref(*results[XY]),
                                                 std::cref(*results[YZ]),
                                                 std::cref(*results[XZ])})};
}
