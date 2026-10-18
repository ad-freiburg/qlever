// Copyright 2021 - 2025 The QLever Authors, in particular:
//
// 2021 - 2024 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2025        Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_COMPRESSEDRELATIONHELPERSIMPL_H_
#define QLEVER_SRC_INDEX_COMPRESSEDRELATIONHELPERSIMPL_H_

#include <array>
#include <functional>
#include <optional>

#include "backports/numeric.h"
#include "index/CompressedRelationWriter.h"
#include "util/ExceptionHandling.h"

namespace compressedRelationHelpers {

static constexpr size_t c0Idx = 0;
static constexpr size_t c1Idx = 1;
static constexpr size_t c2Idx = 2;

// Return the binary representation of the given `id` and make sure (using
// `AD_EXPENSIVE_CHECK`) that it can be compared bitwise, which should always be
// true for index building, because there are no `Id`s of a local vocabulary
// then. Comparing the bits is much cheaper than the general comparison of
// `Id`s, and (in contrast to the latter) it can be vectorized.
constexpr Id::T bitsOfIdWithoutLocalVocab(Id id) {
  AD_EXPENSIVE_CHECK(id.canBeComparedBitwise());
  return id.getBits();
}

// Return the binary representation (see `bitsOfIdWithoutLocalVocab` above) of
// the cells of the `row` with the given column indices `Cols`. The resulting
// `std::array`s compare lexicographically, which makes the comparison of rows
// by these columns really cheap. Note: In a micro benchmark (sorting 5M rows
// with various distributions of duplicates) materializing the arrays was as
// fast as a handwritten comparison of the bits with an early exit, and both
// were 20-35% faster than comparing the `Id`s themselves.
template <size_t... Cols>
inline constexpr auto pickBitsOfColumns =
    [](const auto& row) -> std::array<Id::T, sizeof...(Cols)> {
  return {bitsOfIdWithoutLocalVocab(row[Cols])...};
};

// Compares two rows based on the columns `c1Idx`, `c2Idx`, and
// `ADDITIONAL_COLUMN_GRAPH_ID` only (it ignores the column `c0Idx` as well as
// any payload columns). The comparison is performed on the bits of the `Id`s,
// see `pickBitsOfColumns` above.
struct ComparatorForConstCol0 {
  template <typename A, typename B>
  constexpr bool operator()(const A& a, const B& b) const {
    auto pickBits = pickBitsOfColumns<c1Idx, c2Idx, ADDITIONAL_COLUMN_GRAPH_ID>;
    return pickBits(a) < pickBits(b);
  }
};

// Helper function to make a row from `IdTable` easier to compare. This selects
// the binary representation of the cells of the given row in the columns
// `c0Idx`, `c1Idx`, and `c2Idx`, see `pickBitsOfColumns` above.
inline constexpr auto pickFirstThreeColumnsOfIdsWithoutLocalVocab =
    pickBitsOfColumns<c0Idx, c1Idx, c2Idx>;

// Collect elements of type `T` in batches of size 100'000 and apply the
// `Function` to each batch. For the last batch (which might be smaller)  the
// function is applied in the destructor.
CPP_template(typename T, typename Function)(
    requires ad_utility::InvocableWithExactReturnType<
        Function, void, std::vector<T>&&>) struct Batcher {
  Function function_;
  size_t blocksize_;
  std::vector<T> vec_;

  // ___________________________________________________________________________
  Batcher(Function function, size_t blocksize)
      : function_{std::move(function)}, blocksize_{blocksize} {
    vec_.reserve(blocksize_);
  }

  // ___________________________________________________________________________
  void operator()(T t) {
    vec_.push_back(std::move(t));
    if (vec_.size() >= blocksize_) {
      function_(std::move(vec_));
      vec_.clear();
      vec_.reserve(blocksize_);
    }
  }

  // ___________________________________________________________________________
  ~Batcher() {
    ad_utility::terminateIfThrows(
        [this]() {
          if (!vec_.empty()) {
            function_(std::move(vec_));
          }
        },
        "Batcher function threw an exception while processing the final "
        "(possibly incomplete) block.");
  }

  // No copy or move operations (neither needed nor easy to get right).
  Batcher(const Batcher&) = delete;
  Batcher& operator=(const Batcher&) = delete;
};

using MetadataCallback = CompressedRelationWriter::MetadataCallback;

// The `CompressedRelationMetadata` for a single permutation can be directly
// input blockwise to the `MetadataCallback` (collecting the blocks uses the
// `Batcher` helper from above).
using SingleMetadataWriter =
    Batcher<CompressedRelationMetadata, MetadataCallback>;

// A class that is called for all pairs of `CompressedRelationMetadata` for
// the same `col0Id` and the "twin permutations" (e.g. PSO and POS). The
// multiplicity of the last column is exchanged and then the metadata are
// passed on to the respective `MetadataCallback`.
class PairMetadataWriter {
 private:
  SingleMetadataWriter batcher1_;
  SingleMetadataWriter batcher2_;

 public:
  // ___________________________________________________________________________
  PairMetadataWriter(MetadataCallback callback1, MetadataCallback callback2,
                     size_t blocksize)
      : batcher1_{std::move(callback1), blocksize},
        batcher2_{std::move(callback2), blocksize} {}

  // ___________________________________________________________________________
  void operator()(CompressedRelationMetadata md1,
                  CompressedRelationMetadata md2) {
    md1.multiplicityCol2_ = md2.multiplicityCol1_;
    md2.multiplicityCol2_ = md1.multiplicityCol1_;
    batcher1_(md1);
    batcher2_(md2);
  }
};

// The number of distinct IDs in a sorted column of a single block, together
// with the first and the last ID of that column. The latter two are needed to
// correct the count at the boundary between two consecutive blocks, where an ID
// that ends the one block and starts the other must not be counted twice.
struct DistinctIdCountOfBlock {
  size_t count_ = 0;
  Id first_ = Id::makeUndefined();
  Id last_ = Id::makeUndefined();
};

// Return the `DistinctIdCountOfBlock` of the `column`, which has to be sorted
// and must not be empty.
inline DistinctIdCountOfBlock countDistinctIds(ql::span<const Id> column) {
  AD_CORRECTNESS_CHECK(!column.empty());
  // Note: The comparison of the bits (instead of the general comparison of
  // `Id`s) is not only much cheaper per element, it also makes this reduction
  // vectorizable, which matters because it touches every single ID.
  size_t count = ql::transform_reduce(
      column.begin() + 1, column.end(), column.begin(), size_t{1},
      std::plus<>{}, [](Id current, Id previous) {
        return static_cast<size_t>(bitsOfIdWithoutLocalVocab(current) !=
                                   bitsOfIdWithoutLocalVocab(previous));
      });
  return {count, column.front(), column.back()};
}

// Count the distinct IDs in a sorted sequence of IDs that is split into several
// consecutive blocks, which are added one after the other via `addBlock`.
class DistinctIdCounter {
  size_t count_ = 0;
  // The bits (see `bitsOfIdWithoutLocalVocab`) of the last ID of the previously
  // added block, which must not be counted again if the next block starts with
  // it.
  std::optional<Id::T> lastBitsOfPreviousBlock_;

 public:
  // Add the `column` of the next block, which has to be sorted, and whose
  // first ID must not be smaller than the last ID of the previous block. An
  // empty `column` is ignored.
  void addBlock(ql::span<const Id> column) {
    if (column.empty()) {
      return;
    }
    addCountOfBlock(countDistinctIds(column));
  }

  // Like `addBlock`, but for a block whose `DistinctIdCountOfBlock` has
  // already been computed (e.g. concurrently, see
  // `CompressedRelationWriter::addCompleteLargeRelation`).
  void addCountOfBlock(const DistinctIdCountOfBlock& countOfBlock) {
    count_ += countOfBlock.count_;
    if (lastBitsOfPreviousBlock_ ==
        bitsOfIdWithoutLocalVocab(countOfBlock.first_)) {
      --count_;
    }
    lastBitsOfPreviousBlock_ = bitsOfIdWithoutLocalVocab(countOfBlock.last_);
  }

  // Reset the counter, so that it can be reused for the next relation. Use
  // this instead of `getAndReset` if the count itself is not needed.
  void reset() {
    count_ = 0;
    lastBitsOfPreviousBlock_.reset();
  }

  // Return the number of distinct IDs in all the blocks that have been added
  // since the last reset, and reset the counter.
  size_t getAndReset() {
    auto count = count_;
    reset();
    return count;
  }
};

}  // namespace compressedRelationHelpers

#endif  // QLEVER_SRC_INDEX_COMPRESSEDRELATIONHELPERSIMPL_H_
