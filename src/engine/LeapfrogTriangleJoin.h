// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#ifndef QLEVER_SRC_ENGINE_LEAPFROGTRIANGLEJOIN_H
#define QLEVER_SRC_ENGINE_LEAPFROGTRIANGLEJOIN_H

#include <array>
#include <memory>

#include "engine/Operation.h"
#include "engine/QueryExecutionTree.h"

// Join of three inputs that form a triangle over three variables `x`, `y`,
// `z`: the first input binds `(x, y)`, the second `(y, z)`, and the third
// `(x, z)`. The join is computed with the Leapfrog Triejoin algorithm
// (Veldhuizen, 2014): each input is sorted lexicographically on its two
// variables and read as a two-level trie, and the variables are bound one
// after the other in the order `x`, `y`, `z` by intersecting the sorted key
// ranges of the two inputs that contain the variable. Unlike a chain of two
// binary joins, no intermediate result of two inputs is materialized.
//
// The result has the columns `x`, `y`, `z` and is sorted on them. It has bag
// semantics: a result row appears `m1 * m2 * m3` times, where `mi` is the
// multiplicity of its projection in input `i`. The inputs must have exactly
// the two variables of their edge, and these must never be UNDEF.
class LeapfrogTriangleJoin : public Operation {
 public:
  // The positions of the three inputs in `children_`.
  static constexpr size_t XY = 0;
  static constexpr size_t YZ = 1;
  static constexpr size_t XZ = 2;

 private:
  // The inputs, each sorted on its two variables.
  std::array<std::shared_ptr<QueryExecutionTree>, 3> children_;
  // The variables `x`, `y`, `z`, which are also the result columns.
  std::array<Variable, 3> variables_;
  // For each input, the column of its first and second variable.
  std::array<std::array<ColumnIndex, 2>, 3> columns_;

 public:
  LeapfrogTriangleJoin(QueryExecutionContext* qec,
                       std::shared_ptr<QueryExecutionTree> xy,
                       std::shared_ptr<QueryExecutionTree> yz,
                       std::shared_ptr<QueryExecutionTree> xz, Variable x,
                       Variable y, Variable z);

  std::vector<QueryExecutionTree*> getChildren() override;
  std::string getCacheKeyImpl() const override;
  std::string getDescriptor() const override;
  size_t getResultWidth() const override;
  size_t getCostEstimate() override;

 private:
  uint64_t getSizeEstimateBeforeLimit() override;

 public:
  float getMultiplicity(size_t col) override;
  bool knownEmptyResult() override;

 private:
  [[nodiscard]] bool isDeterministicImpl() const override { return true; }
  std::unique_ptr<Operation> cloneImpl() const override;
  [[nodiscard]] std::vector<ColumnIndex> resultSortedOn() const override;
  Result computeResult(bool requestLaziness) override;
  VariableToColumnMap computeVariableToColumnMap() const override;
};

#endif  // QLEVER_SRC_ENGINE_LEAPFROGTRIANGLEJOIN_H
