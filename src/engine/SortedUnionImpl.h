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
#include "util/CancellationHandle.h"
#include "util/views/ChunkedIotaView.h"

namespace sortedUnion {
// Helper struct that has the same layout as `Result::IdTableVocabPair` but
// doesn't own the data.
struct Wrapper {
  const IdTableView<0>& idTable_;
  const LocalVocab& localVocab_;
};

// Move the data from a real `Result::IdTableVocabPair`.
template <typename CheckCancellation>
Result::IdTableVocabPair moveOrCopy(Result::IdTableVocabPair& element,
                                    const CheckCancellation&) {
  return std::move(element);
}

// Copy the data from a `Wrapper`. The table might be large, so
// `checkCancellation` is called regularly while copying.
template <typename CheckCancellation>
Result::IdTableVocabPair moveOrCopy(
    const Wrapper& element, const CheckCancellation& checkCancellation) {
  return {element.idTable_.clone(checkCancellation),
          element.localVocab_.clone()};
}

// Represent one range of tables to be merged.
template <typename Range>
struct IterationData {
  std::shared_ptr<const Result> result_;
  Range range_;
  std::vector<ColumnIndex> permutation_;
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
  template <typename T, typename CheckCancellation>
  std::optional<Result::IdTableVocabPair> passNext(
      T applyPermutation, const CheckCancellation& checkCancellation) {
    if (it_ == range_.end()) {
      return std::nullopt;
    }
    Result::IdTableVocabPair pair = moveOrCopy(*it_.value(), checkCancellation);
    pair.idTable_ = applyPermutation(std::move(pair.idTable_), permutation_);

    index_ = 0;
    ++it_.value();
    return pair;
  }

  // Append the remainder of the last partially consumed table to the current
  // result table and merge the local vocabs. The table might be large, so it
  // is appended in chunks, with a call to `checkCancellation` after each.
  template <typename CheckCancellation>
  void appendCurrent(IdTable& resultTable, LocalVocab& localVocab,
                     const CheckCancellation& checkCancellation) {
    const auto& table = it_.value()->idTable_;
    for (auto [begin, end] :
         ad_utility::chunkedIotaView(index_, table.size(), Union::chunkSize)) {
      resultTable.insertAtEnd(table, begin, end, permutation_,
                              Id::makeUndefined());
      checkCancellation();
    }
    localVocab.mergeWith(it_.value()->localVocab_);
    index_ = 0;
    ++it_.value();
  }

  // For the non-lazy case just append the remaining tables to the aggregated
  // result table until the range is exhausted.
  template <typename CheckCancellation>
  void appendRemaining(IdTable& resultTable, LocalVocab& localVocab,
                       const CheckCancellation& checkCancellation) {
    while (it_ != range_.end()) {
      appendCurrent(resultTable, localVocab, checkCancellation);
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
IterationData(std::shared_ptr<const Result>, Range,
              std::vector<ColumnIndex>) -> IterationData<Range>;

// Range that performs a zipper merge of two sorted ranges.
template <size_t SPAN_SIZE, typename Range1, typename Range2, typename Func>
struct SortedUnionImpl
    : ad_utility::InputRangeFromGet<Result::IdTableVocabPair> {
  // Iterator and range storage.
  IterationData<Range1> data1_;
  IterationData<Range2> data2_;

  // Result storage.
  IdTable resultTable_;
  LocalVocab localVocab_{};

  // Metadata
  ad_utility::AllocatorWithLimit<Id> allocator_;
  bool requestLaziness_;
  std::vector<std::array<size_t, 2>> columnOrigins_;
  std::vector<std::array<size_t, 2>> targetOrder_;
  // Only being used when `requestLaziness_` is false.
  bool done_ = false;
  // Function forwarded from `Union`
  Func applyPermutation_;
  ad_utility::SharedCancellationHandle cancellationHandle_;

  SortedUnionImpl(IterationData<Range1> data1, IterationData<Range2> data2,
                  bool requestLaziness,
                  const std::vector<std::array<size_t, 2>>& columnOrigins,
                  const ad_utility::AllocatorWithLimit<Id>& allocator,
                  ql::span<const ColumnIndex, SPAN_SIZE> comparatorView,
                  Func applyPermutation,
                  ad_utility::SharedCancellationHandle cancellationHandle)
      : data1_{std::move(data1)},
        data2_{std::move(data2)},
        resultTable_{columnOrigins.size(), allocator},
        allocator_{allocator},
        requestLaziness_{requestLaziness},
        columnOrigins_{columnOrigins},
        applyPermutation_{std::move(applyPermutation)},
        cancellationHandle_{std::move(cancellationHandle)} {
    AD_CONTRACT_CHECK(cancellationHandle_ != nullptr);
    if (requestLaziness) {
      resultTable_.reserve(Union::chunkSize);
    }
    targetOrder_.reserve(comparatorView.size());
    for (auto& col : comparatorView) {
      targetOrder_.push_back(columnOrigins_.at(col));
    }
  }

  // Always inline makes a huge difference on large datasets.
  template <typename T1, typename T2>
  AD_ALWAYS_INLINE bool isSmaller(const T1& row1, const T2& row2) const {
    using StaticRange = ql::span<const std::array<size_t, 2>, SPAN_SIZE>;
    for (auto [index1, index2] : StaticRange{targetOrder_}) {
      if (index1 == Union::NO_COLUMN) {
        return true;
      }
      if (index2 == Union::NO_COLUMN) {
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
      ColumnIndex origin = columnOrigins_.at(column).at(!left);
      resultTable_.at(resultTable_.size() - 1, column) =
          origin == Union::NO_COLUMN ? Id::makeUndefined() : row[origin];
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
  Result::IdTableVocabPair popResult() {
    auto result = Result::IdTableVocabPair{std::move(resultTable_),
                                           std::move(localVocab_)};
    resultTable_ = IdTable{resultTable_.numColumns(), allocator_};
    resultTable_.reserve(Union::chunkSize);
    localVocab_ = LocalVocab{};
    return result;
  }

  // ___________________________________________________________________________
  std::optional<Result::IdTableVocabPair> get() override {
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
      // In the lazy case, the result is yielded once per chunk. In the non-lazy
      // case, this loop merges the complete result, so check for cancellation
      // once per chunk instead.
      size_t chunkEnd = requestLaziness_
                            ? Union::chunkSize
                            : resultTable_.size() + Union::chunkSize;
      while (index1 < idTable1.size() && index2 < idTable2.size()) {
        if (isSmaller(idTable1.at(index1), idTable2.at(index2))) {
          pushRow(true, idTable1.at(index1));
          index1++;
        } else {
          pushRow(false, idTable2.at(index2));
          index2++;
        }
        if (resultTable_.size() >= chunkEnd) {
          if (requestLaziness_) {
            auto result = popResult();
            advanceRangeIfConsumed();
            return result;
          }
          cancellationHandle_->throwIfCancelled();
          chunkEnd += Union::chunkSize;
        }
      }
      advanceRangeIfConsumed();
    }
    auto check = cancellationHandle_->cancellationChecker();
    if (requestLaziness_) {
      if (data1_.index_ != 0) {
        data1_.appendCurrent(resultTable_, localVocab_, check);
      }
      if (data2_.index_ != 0) {
        data2_.appendCurrent(resultTable_, localVocab_, check);
      }
      if (!resultTable_.empty()) {
        return Result::IdTableVocabPair{std::move(resultTable_),
                                        std::move(localVocab_)};
      }
      auto leftTable = data1_.passNext(applyPermutation_, check);
      return leftTable.has_value() ? std::move(leftTable)
                                   : data2_.passNext(applyPermutation_, check);
    }
    data1_.appendRemaining(resultTable_, localVocab_, check);
    data2_.appendRemaining(resultTable_, localVocab_, check);
    done_ = true;
    return Result::IdTableVocabPair{std::move(resultTable_),
                                    std::move(localVocab_)};
  }
};

template <size_t SPAN_SIZE, typename Range1, typename Range2, typename Func>
SortedUnionImpl(IterationData<Range1>, IterationData<Range2>, bool,
                const std::vector<std::array<size_t, 2>>&,
                const ad_utility::AllocatorWithLimit<Id>&,
                ql::span<const ColumnIndex, SPAN_SIZE>,
                Func) -> SortedUnionImpl<SPAN_SIZE, Range1, Range2, Func>;
}  // namespace sortedUnion

#endif  // QLEVER_SRC_ENGINE_SORTEDUNIONIMPL_H
