// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_ROWSCHECKSUM_H
#define QLEVER_SRC_INDEX_ROWSCHECKSUM_H

#include <absl/strings/str_cat.h>

#include <atomic>
#include <cstdint>
#include <string>
#include <vector>

#include "global/Id.h"

namespace qlever {

// A checksum of a multiset of rows of `Id`s, which doesn't depend on the order
// of the rows. It is used to check that the rows of a permutation pair (e.g.
// PSO and POS) arrive unchanged at all the stages of the writing of the twin
// permutation, see `PermutationWriter` in
// `CompressedRelationPermutationWriterImpl.h`.
struct RowsChecksum {
  uint64_t numRows_ = 0;
  // The sum (modulo 2^64) of a hash of each row. The hash of a row depends on
  // the order of its columns, so that the swapping of two values of the same
  // row is detected.
  uint64_t sum_ = 0;

  // Return the checksum of the rows of the `table` (an `IdTable` or a view of
  // it). If `swapColumns12` is `true`, then the columns 1 and 2 are hashed as
  // if they were swapped. The rows of a block of a permutation and the rows of
  // the same triples in its twin permutation (where columns 1 and 2 are
  // swapped) then have the same checksum.
  template <typename Table>
  static RowsChecksum of(const Table& table, bool swapColumns12 = false) {
    const size_t numColumns = table.numColumns();
    std::vector<decltype(table.getColumn(0))> columns;
    columns.reserve(numColumns);
    for (size_t col = 0; col < numColumns; ++col) {
      size_t sourceCol = col;
      if (swapColumns12 && (col == 1 || col == 2)) {
        sourceCol = 3 - col;
      }
      columns.push_back(table.getColumn(sourceCol));
    }
    RowsChecksum result;
    result.numRows_ = table.numRows();
    for (size_t row = 0; row < result.numRows_; ++row) {
      uint64_t hash = 0x9e3779b97f4a7c15;
      for (const auto& column : columns) {
        hash = mix(hash ^ column[row].getBits());
      }
      result.sum_ += hash;
    }
    return result;
  }

  // Add the rows of the `other` checksum to this one.
  void add(const RowsChecksum& other) {
    numRows_ += other.numRows_;
    sum_ += other.sum_;
  }

  bool operator==(const RowsChecksum&) const = default;

  // Return a human-readable representation for the messages of failed checks.
  std::string toString() const {
    return absl::StrCat("{numRows: ", numRows_, ", sum: ", sum_, "}");
  }

 private:
  // The finalizer of `SplitMix64`, a cheap function with a good avalanche
  // effect.
  static constexpr uint64_t mix(uint64_t x) {
    x ^= x >> 30;
    x *= 0xbf58476d1ce4e5b9;
    x ^= x >> 27;
    x *= 0x94d049bb133111eb;
    x ^= x >> 31;
    return x;
  }
};

// A `RowsChecksum` to which several threads may add concurrently.
class AtomicRowsChecksum {
 private:
  std::atomic<uint64_t> numRows_ = 0;
  std::atomic<uint64_t> sum_ = 0;

 public:
  // Add the rows of the `checksum`. Thread-safe.
  void add(const RowsChecksum& checksum) {
    numRows_.fetch_add(checksum.numRows_);
    sum_.fetch_add(checksum.sum_);
  }

  // Return the checksum of all the rows that have been added so far. Only
  // returns a consistent value if no `add` runs concurrently.
  RowsChecksum get() const { return {numRows_.load(), sum_.load()}; }
};

}  // namespace qlever

#endif  // QLEVER_SRC_INDEX_ROWSCHECKSUM_H
