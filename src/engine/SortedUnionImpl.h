//   Copyright 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_SORTEDUNIONIMPL_H
#define QLEVER_SRC_ENGINE_SORTEDUNIONIMPL_H

#include <optional>

#include "backports/span.h"
#include "engine/Result.h"
#include "engine/Union.h"
#include "engine/idTable/IdTable.h"
#include "index/LocalVocab.h"

namespace sortedUnion {
// Helper struct that has the same layout as `Result::IdTableVocabPair` but
// doesn't own the data.
struct Wrapper {
  const qlever::IdTableView<0>& idTable_;
  const qlever::LocalVocab& localVocab_;
};

// Move the data from a real `Result::IdTableVocabPair`.
inline qlever::Result::IdTableVocabPair moveOrCopy(
    qlever::Result::IdTableVocabPair& element) {
  return std::move(element);
}

// Copy the data from a `Wrapper`.
inline qlever::Result::IdTableVocabPair moveOrCopy(const Wrapper& element) {
  return {element.idTable_.clone(), element.localVocab_.clone()};
}

// Represent one range of tables to be merged.
template <typename Range>
struct IterationData {
  std::shared_ptr<const qlever::Result> result_;
  Range range_;
  std::vector<qlever::ColumnIndex> permutation_;
  std::optional<typename Range::iterator> it_ = std::nullopt;
  size_t index_ = 0;

  // Call begin on the iterator if it hasn't been called yet.
  void initIfNotStarted() {
    if (!it_.has_value()) {
      it_ = range_.begin();
    }
  }

  // Fetch the next element from the range, make a copy if it's a `Wrapper`.
  // Otherwise move it. If the range is exhausted, return `std::nullopt`.
  template <typename T>
  std::optional<qlever::Result::IdTableVocabPair> passNext(T applyPermutation) {
    if (it_ == range_.end()) {
      return std::nullopt;
    }
    qlever::Result::IdTableVocabPair pair = moveOrCopy(*it_.value());
    pair.idTable_ = applyPermutation(std::move(pair.idTable_), permutation_);

    index_ = 0;
    ++it_.value();
    return pair;
  }

  // Append the remainder of the last partially consumed table to the current
  // result table and merge the local vocabs.
  void appendCurrent(qlever::IdTable& resultTable,
                     qlever::LocalVocab& localVocab) {
    resultTable.insertAtEnd(it_.value()->idTable_, index_, std::nullopt,
                            permutation_, qlever::Id::makeUndefined());
    localVocab.mergeWith(it_.value()->localVocab_);
    index_ = 0;
    ++it_.value();
  }

  // For the non-lazy case just append the remaining tables to the aggregated
  // result table until the range is exhausted.
  void appendRemaining(qlever::IdTable& resultTable,
                       qlever::LocalVocab& localVocab) {
    while (it_ != range_.end()) {
      appendCurrent(resultTable, localVocab);
    }
  }

  // Increment the iterator if the last table has been fully consumed.
  void advanceRangeIfConsumed() {
    if (index_ == it_.value()->idTable_.size()) {
      ++it_.value();
      index_ = 0;
    }
  }
};

template <typename Range>
IterationData(std::shared_ptr<const qlever::Result>, Range,
              std::vector<qlever::ColumnIndex>) -> IterationData<Range>;

// Range that performs a zipper merge of two sorted ranges.
template <size_t SPAN_SIZE, typename Range1, typename Range2, typename Func>
struct SortedUnionImpl
    : ad_utility::InputRangeFromGet<qlever::Result::IdTableVocabPair> {
  // Iterator and range storage.
  IterationData<Range1> data1_;
  IterationData<Range2> data2_;

  // Result storage.
  qlever::IdTable resultTable_;
  qlever::LocalVocab localVocab_{};

  // Metadata
  ad_utility::AllocatorWithLimit<qlever::Id> allocator_;
  bool requestLaziness_;
  std::vector<std::array<size_t, 2>> columnOrigins_;
  std::vector<std::array<size_t, 2>> targetOrder_;
  // Only being used when `requestLaziness_` is false.
  bool done_ = false;
  // Function forwarded from `Union`
  Func applyPermutation_;

  SortedUnionImpl(IterationData<Range1> data1, IterationData<Range2> data2,
                  bool requestLaziness,
                  const std::vector<std::array<size_t, 2>>& columnOrigins,
                  const ad_utility::AllocatorWithLimit<qlever::Id>& allocator,
                  ql::span<const qlever::ColumnIndex, SPAN_SIZE> comparatorView,
                  Func applyPermutation)
      : data1_{std::move(data1)},
        data2_{std::move(data2)},
        resultTable_{columnOrigins.size(), allocator},
        allocator_{allocator},
        requestLaziness_{requestLaziness},
        columnOrigins_{columnOrigins},
        applyPermutation_{std::move(applyPermutation)} {
    if (requestLaziness) {
      resultTable_.reserve(qlever::Union::chunkSize);
    }
    targetOrder_.reserve(comparatorView.size());
    for (auto& col : comparatorView) {
      targetOrder_.push_back(columnOrigins_.at(col));
    }
  }

  // Always inline makes makes a huge difference on large datasets.
  template <typename T1, typename T2>
  AD_ALWAYS_INLINE bool isSmaller(const T1& row1, const T2& row2) const {
    using StaticRange = ql::span<const std::array<size_t, 2>, SPAN_SIZE>;
    for (auto [index1, index2] : StaticRange{targetOrder_}) {
      if (index1 == qlever::Union::NO_COLUMN) {
        return true;
      }
      if (index2 == qlever::Union::NO_COLUMN) {
        return false;
      }
      if (row1[index1] != row2[index2]) {
        return row1[index1] < row2[index2];
      }
    }
    return false;
  }

  // Write a new row to the result table. The parameter `left` controls which
  // permutation is used to write the row to the result table.
  template <typename T>
  void pushRow(bool left, const T& row) {
    resultTable_.emplace_back();
    for (size_t column = 0; column < resultTable_.numColumns(); column++) {
      qlever::ColumnIndex origin = columnOrigins_.at(column).at(!left);
      resultTable_.at(resultTable_.size() - 1, column) =
          origin == qlever::Union::NO_COLUMN ? qlever::Id::makeUndefined()
                                             : row[origin];
    }
  }

  // Increment the iterators if the current id table is fully processed and
  // reset the index back to zero.
  void advanceRangeIfConsumed() {
    data1_.advanceRangeIfConsumed();
    data2_.advanceRangeIfConsumed();
  }

  // Retrieve the current result from `resultTable_` and `localVocab_` and reset
  // those members back to their initial value so the next operation can
  // continue adding values.
  qlever::Result::IdTableVocabPair popResult() {
    auto result = qlever::Result::IdTableVocabPair{std::move(resultTable_),
                                                   std::move(localVocab_)};
    resultTable_ = qlever::IdTable{resultTable_.numColumns(), allocator_};
    resultTable_.reserve(qlever::Union::chunkSize);
    localVocab_ = qlever::LocalVocab{};
    return result;
  }

  // ___________________________________________________________________________
  std::optional<qlever::Result::IdTableVocabPair> get() override {
    if (done_) {
      return std::nullopt;
    }
    data1_.initIfNotStarted();
    data2_.initIfNotStarted();
    while (data1_.it_ != data1_.range_.end() &&
           data2_.it_ != data2_.range_.end()) {
      auto& idTable1 = data1_.it_.value()->idTable_;
      auto& idTable2 = data2_.it_.value()->idTable_;
      localVocab_.mergeWith(data1_.it_.value()->localVocab_);
      localVocab_.mergeWith(data2_.it_.value()->localVocab_);
      size_t& index1 = data1_.index_;
      size_t& index2 = data2_.index_;
      while (index1 < idTable1.size() && index2 < idTable2.size()) {
        if (isSmaller(idTable1.at(index1), idTable2.at(index2))) {
          pushRow(true, idTable1.at(index1));
          index1++;
        } else {
          pushRow(false, idTable2.at(index2));
          index2++;
        }
        if (requestLaziness_ &&
            resultTable_.size() >= qlever::Union::chunkSize) {
          auto result = popResult();
          advanceRangeIfConsumed();
          return result;
        }
      }
      advanceRangeIfConsumed();
    }
    if (requestLaziness_) {
      if (data1_.index_ != 0) {
        data1_.appendCurrent(resultTable_, localVocab_);
      }
      if (data2_.index_ != 0) {
        data2_.appendCurrent(resultTable_, localVocab_);
      }
      if (!resultTable_.empty()) {
        return qlever::Result::IdTableVocabPair{std::move(resultTable_),
                                                std::move(localVocab_)};
      }
      auto leftTable = data1_.passNext(applyPermutation_);
      return leftTable.has_value() ? std::move(leftTable)
                                   : data2_.passNext(applyPermutation_);
    }
    data1_.appendRemaining(resultTable_, localVocab_);
    data2_.appendRemaining(resultTable_, localVocab_);
    done_ = true;
    return qlever::Result::IdTableVocabPair{std::move(resultTable_),
                                            std::move(localVocab_)};
  }
};

template <size_t SPAN_SIZE, typename Range1, typename Range2, typename Func>
SortedUnionImpl(IterationData<Range1>, IterationData<Range2>, bool,
                const std::vector<std::array<size_t, 2>>&,
                const ad_utility::AllocatorWithLimit<qlever::Id>&,
                ql::span<const qlever::ColumnIndex, SPAN_SIZE>,
                Func) -> SortedUnionImpl<SPAN_SIZE, Range1, Range2, Func>;
}  // namespace sortedUnion
#endif  // QLEVER_SRC_ENGINE_SORTEDUNIONIMPL_H
