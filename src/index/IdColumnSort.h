// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_IDCOLUMNSORT_H
#define QLEVER_SRC_INDEX_IDCOLUMNSORT_H

#include <array>
#include <cstdint>
#include <vector>

#include "backports/span.h"
#include "engine/idTable/IdTable.h"
#include "global/Id.h"

// `SingleKeySorter` sorts an `IdTable` by one column, exploiting the split
// storage layout of `Id` columns (see `engine/idTable/IdColumn.h`) to
// sort/permute the plain payload words instead of materialized `Id`s. It
// doesn't support the `LocalVocabIndex` datatype (which doesn't compare
// bitwise; see `ZipperJoiner` for how the analogous join handles it) -
// check `isSortable` first. Unlike `ZipperJoiner`, it's a plain
// (non-template) class, so its methods live in `IdColumnSort.cpp`.

namespace columnBasedIdTable {

// Sorts a table by one column in three steps (see the methods below).
// Construct once, check `isSortable()`, then call `sort()` at most once.
class SingleKeySorter {
 public:
  SingleKeySorter(::IdTable& table, size_t numThreads);

  // Whether `keyColumn` can be sorted with `sort()` (false if it contains
  // `LocalVocabIndex` IDs).
  static bool isSortable(const ::IdTable& table, ColumnIndex keyColumn);

  // Sorts `table` by `keyColumn`. Requires `isSortable(table, keyColumn)`.
  void sort(ColumnIndex keyColumn);

 private:
  // A payload paired with its original row index; sorting these yields a
  // permutation.
  struct PayloadAndIndex {
    uint64_t payload_;
    uint64_t index_;
  };

  // Step 1: Counting-sort the (payload, rowIndex) pairs by datatype. Sets
  // `singleDatatype_` if every row shares one datatype.
  void partitionByDatatype(ql::span<const uint8_t> keyTypes,
                           ql::span<const uint64_t> keyPayloads);

  // Step 2: Sort each datatype partition by its payload words.
  void sortPartitions();

  // Step 3: Apply `permutation_` to all columns, spread across up to
  // `numThreads_` worker threads (one disjoint subset of columns each).
  void applyPermutation();

  // Gather column `c` into `scratchPayloads`/`scratchTypes` by
  // `permutation_`, then copy back. Scratch buffers are caller-owned so a
  // worker thread can reuse them across its columns. Thread-safe to call
  // concurrently for different `c`.
  void applyPermutationToColumn(size_t c,
                                std::vector<uint64_t>& scratchPayloads,
                                std::vector<uint8_t>& scratchTypes);

  ::IdTable& table_;
  size_t numThreads_;
  size_t numRows_;
  std::array<size_t, 256> counts_{};
  std::array<size_t, 257> offsets_{};
  std::vector<PayloadAndIndex> permutation_;
  // Set by `partitionByDatatype`; see there and `sortPartitions`.
  bool singleDatatype_ = false;
};

}  // namespace columnBasedIdTable

#endif  // QLEVER_SRC_INDEX_IDCOLUMNSORT_H
