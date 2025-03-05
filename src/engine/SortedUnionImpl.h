//   Copyright 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#pragma once

#include <optional>

#include "engine/LocalVocab.h"
#include "engine/Result.h"
#include "engine/Union.h"
#include "engine/idTable/IdTable.h"

// Helper struct that has the same layout as Result::IdTableVocabPair but
// doesn't own the data.
struct Wrapper {
  const IdTable& idTable_;
  const LocalVocab& localVocab_;
};

// Move the data from a real `Result::IdTableVocabPair`.
inline Result::IdTableVocabPair moveOrCopy(Result::IdTableVocabPair& element) {
  return std::move(element);
}

// Copy the data from a `Wrapper`.
inline Result::IdTableVocabPair moveOrCopy(const Wrapper& element) {
  return {element.idTable_.clone(), element.localVocab_.clone()};
}

// Range that performs a zipper merge of two sorted ranges.
template <size_t SPAN_SIZE, typename Range1, typename Range2, typename Func>
struct SortedUnionImpl
    : ad_utility::InputRangeFromGet<Result::IdTableVocabPair> {
  // Iterator and range storage.
  std::shared_ptr<const Result> result1_;
  std::shared_ptr<const Result> result2_;
  Range1 range1_;
  std::optional<typename Range1::iterator> it1_ = std::nullopt;
  size_t index1_ = 0;
  Range2 range2_;
  std::optional<typename Range2::iterator> it2_ = std::nullopt;
  size_t index2_ = 0;

  // Result storage.
  IdTable resultTable_;
  LocalVocab localVocab_{};

  // Metadata
  ad_utility::AllocatorWithLimit<Id> allocator_;
  bool requestLaziness_;
  std::vector<std::array<size_t, 2>> columnOrigins_;
  std::vector<ColumnIndex> leftPermutation_;
  std::vector<ColumnIndex> rightPermutation_;
  std::vector<std::array<size_t, 2>> targetOrder_;
  bool aggregatedTableReturned_ = false;
  // Function forwarded from `Union`
  Func applyPermutation_;

  SortedUnionImpl(std::shared_ptr<const Result> result1,
                  std::shared_ptr<const Result> result2, Range1 range1,
                  Range2 range2, bool requestLaziness,
                  const std::vector<std::array<size_t, 2>>& columnOrigins,
                  const ad_utility::AllocatorWithLimit<Id>& allocator,
                  std::vector<ColumnIndex> leftPermutation,
                  std::vector<ColumnIndex> rightPermutation,
                  std::span<const ColumnIndex, SPAN_SIZE> comparatorView,
                  Func applyPermutation)
      : result1_{std::move(result1)},
        result2_{std::move(result2)},
        range1_{std::move(range1)},
        range2_{std::move(range2)},
        resultTable_{columnOrigins.size(), allocator},
        allocator_{allocator},
        requestLaziness_{requestLaziness},
        columnOrigins_{columnOrigins},
        leftPermutation_{std::move(leftPermutation)},
        rightPermutation_{std::move(rightPermutation)},
        applyPermutation_{std::move(applyPermutation)} {
    if (requestLaziness) {
      resultTable_.reserve(Union::chunkSize);
    }
    targetOrder_.reserve(comparatorView.size());
    for (auto& col : comparatorView) {
      targetOrder_.push_back(columnOrigins_.at(col));
    }
  }

  // Always inline makes makes a huge difference on large datasets.
  AD_ALWAYS_INLINE bool isSmaller(const auto& row1, const auto& row2) const {
    using StaticRange = std::span<const std::array<size_t, 2>, SPAN_SIZE>;
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

  // Fetch the next element from the range, make a copy if it's a `Wrapper`.
  // Otherwise move it. If the range is exhausted, return `std::nullopt`.
  std::optional<Result::IdTableVocabPair> passNext(
      const std::vector<ColumnIndex>& permutation, auto& it, auto end,
      size_t& index) {
    if (it != end) {
      Result::IdTableVocabPair pair = moveOrCopy(*it);
      pair.idTable_ = applyPermutation_(std::move(pair.idTable_), permutation);

      index = 0;
      ++it;
      return pair;
    }
    return std::nullopt;
  }

  // Append the remainder of the last partially consumed table to the current
  // result table and merge the local vocabs.
  void appendCurrent(const std::vector<ColumnIndex>& permutation, auto& it,
                     size_t& index) {
    resultTable_.insertAtEnd(it->idTable_, index, std::nullopt, permutation,
                             Id::makeUndefined());
    localVocab_.mergeWith(std::span{&it->localVocab_, 1});
    index = 0;
    ++it;
  }

  // For the non-lazy case just append the remaining tables to the aggregated
  // result table until the range is exhausted.
  void appendRemaining(const std::vector<ColumnIndex>& permutation, auto& it,
                       auto end, size_t& index) {
    while (it != end) {
      appendCurrent(permutation, it, index);
    }
  }

  // Write a new row to the result table. The parameter `left` controls which
  // permutation is used to write the row to the result table.
  void pushRow(bool left, const auto& row) {
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
    if (index1_ == it1_.value()->idTable_.size()) {
      ++it1_.value();
      index1_ = 0;
    }
    if (index2_ == it2_.value()->idTable_.size()) {
      ++it2_.value();
      index2_ = 0;
    }
  }

  // Retreive the current result from `resultTable_` and `localVocab_` and reset
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
    if (aggregatedTableReturned_) {
      return std::nullopt;
    }
    if (!it1_.has_value()) {
      it1_ = range1_.begin();
    }
    if (!it2_.has_value()) {
      it2_ = range2_.begin();
    }
    while (it1_ != range1_.end() && it2_ != range2_.end()) {
      auto& idTable1 = it1_.value()->idTable_;
      auto& idTable2 = it2_.value()->idTable_;
      localVocab_.mergeWith(std::span{&it1_.value()->localVocab_, 1});
      localVocab_.mergeWith(std::span{&it2_.value()->localVocab_, 1});
      while (index1_ < idTable1.size() && index2_ < idTable2.size()) {
        if (isSmaller(idTable1.at(index1_), idTable2.at(index2_))) {
          pushRow(true, idTable1.at(index1_));
          index1_++;
        } else {
          pushRow(false, idTable2.at(index2_));
          index2_++;
        }
        if (requestLaziness_ && resultTable_.size() >= Union::chunkSize) {
          auto result = popResult();
          advanceRangeIfConsumed();
          return result;
        }
      }
      advanceRangeIfConsumed();
    }
    if (requestLaziness_) {
      if (index1_ != 0) {
        appendCurrent(leftPermutation_, it1_.value(), index1_);
      }
      if (index2_ != 0) {
        appendCurrent(rightPermutation_, it2_.value(), index2_);
      }
      if (!resultTable_.empty()) {
        return Result::IdTableVocabPair{std::move(resultTable_),
                                        std::move(localVocab_)};
      }
      auto leftTable =
          passNext(leftPermutation_, it1_.value(), range1_.end(), index1_);
      return leftTable.has_value() ? std::move(leftTable)
                                   : passNext(rightPermutation_, it2_.value(),
                                              range2_.end(), index2_);
    }
    appendRemaining(leftPermutation_, it1_.value(), range1_.end(), index1_);
    appendRemaining(rightPermutation_, it2_.value(), range2_.end(), index2_);
    aggregatedTableReturned_ = true;
    return Result::IdTableVocabPair{std::move(resultTable_),
                                    std::move(localVocab_)};
  }
};

template <size_t SPAN_SIZE, typename Range1, typename Range2, typename Func>
SortedUnionImpl(std::shared_ptr<const Result>, std::shared_ptr<const Result>,
                Range1, Range2, bool, const std::vector<std::array<size_t, 2>>&,
                const ad_utility::AllocatorWithLimit<Id>&,
                std::vector<ColumnIndex>, std::vector<ColumnIndex>,
                std::span<const ColumnIndex, SPAN_SIZE>, Func)
    -> SortedUnionImpl<SPAN_SIZE, Range1, Range2, Func>;
