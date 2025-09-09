// Copyright 2023-2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR/QL
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
// QL =  QLeverize AG

#ifndef QLEVER_SRC_ENGINE_ADDCOMBINEDROWTOTABLE_H
#define QLEVER_SRC_ENGINE_ADDCOMBINEDROWTOTABLE_H

#include <array>
#include <cstdint>
#include <vector>

#include "backports/concepts.h"
#include "engine/idTable/IdTable.h"
#include "engine/idTable/IdTableConcepts.h"
#include "global/Id.h"
#include "util/CancellationHandle.h"
#include "util/Exception.h"
#include "util/TransparentFunctors.h"

namespace ad_utility {

// This class handles the efficient writing of the results of a JOIN operation
// to a column-based `IdTable`. The underlying assumption is that in both inputs
// the join columns are the first columns. On each call to `addRow`, we only
// store the indices of the matching rows. When a certain buffer size
// (configurable, default value 100'000) is reached, the results are actually
// written to the table.
class AddCombinedRowToIdTable {
  std::vector<size_t> numUndefinedPerColumn_;
  size_t numJoinColumns_;
  // If set to false, then the join columns will not be written to the output.
  // The result table will also have no columns corresponding to the join
  // columns, but will only consist of the remaining payload columns.
  bool keepJoinColumns_ = true;
  std::optional<std::array<IdTableView<0>, 2>> inputLeftAndRight_;
  IdTable resultTable_;
  LocalVocab mergedVocab_{};
  std::array<const LocalVocab*, 2> currentVocabs_{nullptr, nullptr};

 public:
  // This struct stores the information, which row indices from the input are
  // combined into a given row index in the output, i.e. "To obtain the
  // `targetIndex_`-th row in the output, you have to combine
  // `inputLeft_[rowIndices_[0]]` and and `inputRight_[rowIndices_[1]]`.
  struct TargetIndexAndRowIndices {
    size_t targetIndex_;
    std::array<size_t, 2> rowIndices_;
  };
  // Store the indices that have not yet been written.
  std::vector<TargetIndexAndRowIndices> indexBuffer_;

  // Store the information, which row index from the left input is written to a
  // given index in the output. This is used for OPTIONAL joins where there are
  // rows that have no counterpart in the right input.
  struct TargetIndexAndRowIndex {
    size_t targetIndex_;
    size_t rowIndex_;
  };

  // Store the indices of OPTIONAL inputs that have not yet been written.
  std::vector<TargetIndexAndRowIndex> optionalIndexBuffer_;

  // The total number of optional and non-optional rows that are currently
  // buffered but not yet written to the result. The first row index in the
  // output for which a result has neither been written nor stored in one of the
  // buffers can be calculated by `result_.numRows() + nextIndex_`.
  size_t nextIndex_ = 0;

  // The number of rows for which the indices are buffered until they are
  // materialized and written to the result in one go.
  size_t bufferSize_ = 100'000;

  // This callback is called with the result as an argument each time `flush()`
  // is called. It can be used to consume parts of the result early, before the
  // complete operation has finished.
  using BlockwiseCallback = std::function<void(IdTable&, LocalVocab&)>;
  [[no_unique_address]] BlockwiseCallback blockwiseCallback_{ad_utility::noop};

  using CancellationHandle = ad_utility::SharedCancellationHandle;
  CancellationHandle cancellationHandle_;

 public:
  // Construct from the number of join columns, the two inputs, and the output.
  // The `bufferSize` can be configured for testing.
  explicit AddCombinedRowToIdTable(
      size_t numJoinColumns, IdTableView<0> input1, IdTableView<0> input2,
      IdTable output, CancellationHandle cancellationHandle,
      bool keepJoinColumns = true, size_t bufferSize = 100'000,
      BlockwiseCallback blockwiseCallback = ad_utility::noop)
      : numUndefinedPerColumn_(output.numColumns()),
        numJoinColumns_{numJoinColumns},
        keepJoinColumns_{keepJoinColumns},
        inputLeftAndRight_{std::array{input1, input2}},
        resultTable_{std::move(output)},
        bufferSize_{bufferSize},
        blockwiseCallback_{std::move(blockwiseCallback)},
        cancellationHandle_{std::move(cancellationHandle)} {
    AD_CONTRACT_CHECK(cancellationHandle_);
    checkNumColumns();
    indexBuffer_.reserve(bufferSize);
  }

  // Similar to the previous constructor, but the inputs are not given.
  // This means that the inputs have to be set to an explicit
  // call to `setInput` before adding rows. This is used for the lazy join
  // operations (see Join.cpp) where the input changes over time.
  explicit AddCombinedRowToIdTable(
      size_t numJoinColumns, IdTable output,
      CancellationHandle cancellationHandle, bool keepJoinColumns = true,
      size_t bufferSize = 100'000,
      BlockwiseCallback blockwiseCallback = ad_utility::noop)
      : numUndefinedPerColumn_(output.numColumns()),
        numJoinColumns_{numJoinColumns},
        keepJoinColumns_{keepJoinColumns},
        inputLeftAndRight_{std::nullopt},
        resultTable_{std::move(output)},
        bufferSize_{bufferSize},
        blockwiseCallback_{std::move(blockwiseCallback)},
        cancellationHandle_{std::move(cancellationHandle)} {
    AD_CONTRACT_CHECK(cancellationHandle_);
    indexBuffer_.reserve(bufferSize);
  }

  // Return the number of UNDEF values per column.
  const std::vector<size_t>& numUndefinedPerColumn() {
    flush();
    return numUndefinedPerColumn_;
  }

  // The next free row in the output will be created from
  // `inputLeft_[rowIndexA]` and `inputRight_[rowIndexB]`.
  void addRow(size_t rowIndexA, size_t rowIndexB) {
    AD_EXPENSIVE_CHECK(inputLeftAndRight_.has_value());
    indexBuffer_.push_back(
        TargetIndexAndRowIndices{nextIndex_, {rowIndexA, rowIndexB}});
    ++nextIndex_;
    if (nextIndex_ >= bufferSize_) {
      flush();
    }
  }

  // Concept for the `addRows` function below for a `sized_range` that has
  // unsigned integral values as its `value_type`.
  template <typename R>
  static constexpr bool sizedRangeOfUnsigned =
      ql::ranges::sized_range<R> &&
      ql::concepts::unsigned_integral<ql::ranges::range_value_t<R>>;

  // Same as calling `addRow` for each element in the Cartesian product of
  // `rowIndicesA` and `rowIndicesB` with an optimization for the special case
  // that the `resultTable` has zero columns.
  CPP_template(typename R1, typename R2)(
      requires sizedRangeOfUnsigned<R1> CPP_and
          sizedRangeOfUnsigned<R2>) void addRows(const R1& rowIndicesA,
                                                 const R2& rowIndicesB) {
    size_t total =
        ql::ranges::size(rowIndicesA) * ql::ranges::size(rowIndicesB);
    if (resultTable_.numColumns() == 0) {
      while (total > 0) {
        auto chunkSz = std::min(bufferSize_ - nextIndex_, total);
        nextIndex_ += chunkSz;
        total -= chunkSz;
        if (nextIndex_ >= bufferSize_) {
          flush();
        }
      }
    } else {
      for (auto a : rowIndicesA) {
        for (auto b : rowIndicesB) {
          addRow(a, b);
        }
      }
    }
  }

  // Merge the local vocab contained in `T` with the `mergedVocab_` and set the
  // passed pointer reference to that vocab.
  template <typename T>
  void mergeVocab(const T& table, const LocalVocab*& currentVocab) {
    detail::mergeVocabInto(table, currentVocab, mergedVocab_);
  }

  // Flush remaining pending entries before changing the input.
  void flushBeforeInputChange() {
    // Clear to avoid unnecessary merge.
    currentVocabs_ = {nullptr, nullptr};
    if (nextIndex_ != 0) {
      AD_CORRECTNESS_CHECK(inputLeftAndRight_.has_value());
      flush();
    } else if (resultTable_.empty()) {
      // Clear local vocab when no rows were written.
      //
      // TODO<joka921, robinTF> This is a conservative approach. We could
      // optimize this case (clear the local vocab more often, but still
      // correctly) by considering the situation after all the relevant inputs
      // have been processed.
      mergedVocab_ = LocalVocab{};
    }
  }

  // Set or reset the input. All following calls to `addRow` then refer to
  // indices in the new input. Before resetting, `flush()` is called, so all the
  // rows from the previous inputs get materialized before deleting the old
  // inputs. The arguments to `inputLeft` and `inputRight` can either be
  // `IdTable` or `IdTableView<0>`, or any other type that has a
  // `asStaticView<0>` method that returns an `IdTableView<0>`.
  template <typename L, typename R>
  void setInput(const L& inputLeft, const R& inputRight) {
    flushBeforeInputChange();
    mergeVocab(inputLeft, currentVocabs_.at(0));
    mergeVocab(inputRight, currentVocabs_.at(1));
    inputLeftAndRight_ =
        std::array{detail::toView(inputLeft), detail::toView(inputRight)};
    checkNumColumns();
  }

  // Only set the left input. After this it is only allowed to call
  // `addOptionalRow` and not `addRow` until `setInput` has been called again.
  template <typename L>
  void setOnlyLeftInputForOptionalJoin(const L& inputLeft) {
    flushBeforeInputChange();
    mergeVocab(inputLeft, currentVocabs_.at(0));
    // The right input will be empty, but with the correct number of columns.
    using namespace ad_utility::memory_literals;
    inputLeftAndRight_ = std::array{
        detail::toView(inputLeft),
        IdTableView<0>{
            resultTable_.numColumns() +
                (static_cast<size_t>(!keepJoinColumns_) * numJoinColumns_) -
                detail::toView(inputLeft).numColumns() + numJoinColumns_,
            ad_utility::makeAllocatorWithLimit<Id>(0_B)}};
  }

  // The next free row in the output will be created from
  // `inputLeft_[rowIndexA]`. The columns from `inputRight_` will all be set to
  // UNDEF
  void addOptionalRow(size_t rowIndexA) {
    AD_EXPENSIVE_CHECK(inputLeftAndRight_.has_value());
    optionalIndexBuffer_.push_back(
        TargetIndexAndRowIndex{nextIndex_, rowIndexA});
    ++nextIndex_;
    if (nextIndex_ >= bufferSize_) {
      flush();
    }
  }

  // Move the result out after the last write. The function ensures, that the
  // `flush()` is called before doing so.
  IdTable&& resultTable() && {
    flush();
    return std::move(resultTable_);
  }

  LocalVocab& localVocab() { return mergedVocab_; }

  // Disable copying and moving, it is currently not needed and makes it harder
  // to reason about
  AddCombinedRowToIdTable(const AddCombinedRowToIdTable&) = delete;
  AddCombinedRowToIdTable& operator=(const AddCombinedRowToIdTable&) = delete;
  AddCombinedRowToIdTable(AddCombinedRowToIdTable&&) = delete;
  AddCombinedRowToIdTable& operator=(AddCombinedRowToIdTable&&) = delete;

  // Write the result rows the indices of which have been stored in the buffers
  // since the last call to `flush()`. This function is automatically called by
  // the `addRow` functions if the buffers exceed a certain size, but you also
  // have to call it manually after adding the last row, else the destructor
  // will throw an exception.
  void flush() {
    cancellationHandle_->throwIfCancelled();
    auto& result = resultTable_;
    size_t oldSize = result.size();
    AD_CORRECTNESS_CHECK(nextIndex_ == indexBuffer_.size() +
                                           optionalIndexBuffer_.size() ||
                         result.numColumns() == 0);
    // Sometimes the left input and right input are not valid anymore, because
    // the `IdTable`s they point to have already been destroyed. This case is
    // okay, as long as there was a manual call to `flush` (after which
    // `nextIndex_ == 0`) before the inputs went out of scope. However, the call
    // to `resultTable()` will still unconditionally flush. The following check
    // makes this behavior defined.
    if (nextIndex_ == 0) {
      return;
    }
    AD_CORRECTNESS_CHECK(inputLeftAndRight_.has_value());
    result.resize(oldSize + nextIndex_);

    // Precondition: `a` and `b` compare equal or at least one of them is UNDEF
    // If exactly one of them is UNDEF, return the other one, else return any of
    // them (they are equal anyway).
    auto getJoinValue = [](const ValueId a, const ValueId b) {
      // NOTE: For localVocabIndices we might have different pointers that
      // compare equal because they point to the same word. Therefore we cannot
      // use a simple bitwise operation to handle the "one of them is UNDEF"
      // case as we previously did.
      if (a.isUndefined()) {
        return b;
      }
      return a;
    };

    // A lambda that writes the join column with the given `colIdx` to the
    // `nextResultColIdx`-th column of the result.
    auto writeJoinColumn = [&result, &getJoinValue, oldSize, this](
                               size_t colIdx, size_t resultColIdx) {
      const auto& colLeft = inputLeft().getColumn(colIdx);
      const auto& colRight = inputRight().getColumn(colIdx);
      // TODO<joka921> Implement prefetching.
      decltype(auto) resultCol = result.getColumn(resultColIdx);
      size_t& numUndef = numUndefinedPerColumn_.at(resultColIdx);

      // Write the matching rows.
      for (const auto& [targetIndex, sourceIndices] : indexBuffer_) {
        auto resultId =
            getJoinValue(colLeft[sourceIndices[0]], colRight[sourceIndices[1]]);
        numUndef += static_cast<size_t>(resultId.isUndefined());
        resultCol[oldSize + targetIndex] = resultId;
      }

      // Write the optional rows. For the right input those are always
      // undefined.
      for (const auto& [targetIndex, sourceIndex] : optionalIndexBuffer_) {
        Id id = colLeft[sourceIndex];
        resultCol[oldSize + targetIndex] = id;
        numUndef += static_cast<size_t>(id.isUndefined());
      }
    };

    // A lambda that writes the non-join-column `colIdx` to the
    // `nextResultColIdx`-th column of the result. the bool `isColFromLeft`
    // determines whether the input is taken from the left or the right input.
    // Note: There is quite some code duplication between this lambda and the
    // previous one. I have tried to unify them but this lead to template-heavy
    // code that was very hard to read for humans.
    auto writeNonJoinColumn = ad_utility::ApplyAsValueIdentity{
        [&result, oldSize, this](auto isColFromLeft, size_t colIdx,
                                 size_t resultColIdx) {
          decltype(auto) col = isColFromLeft ? inputLeft().getColumn(colIdx)
                                             : inputRight().getColumn(colIdx);
          // TODO<joka921> Implement prefetching.
          decltype(auto) resultCol = result.getColumn(resultColIdx);
          size_t& numUndef = numUndefinedPerColumn_.at(resultColIdx);

          // Write the matching rows.
          static constexpr size_t idx = isColFromLeft ? 0 : 1;
          for (const auto& [targetIndex, sourceIndices] : indexBuffer_) {
            auto resultId = col[sourceIndices[idx]];
            numUndef += static_cast<size_t>(resultId == Id::makeUndefined());
            resultCol[oldSize + targetIndex] = resultId;
          }

          // Write the optional rows. For the right input those are always
          // undefined.
          for (const auto& [targetIndex, sourceIndex] : optionalIndexBuffer_) {
            Id id = [&col, isColFromLeft, sourceIndex = sourceIndex]() {
              if constexpr (isColFromLeft) {
                return col[sourceIndex];
              } else {
                (void)col;
                (void)sourceIndex;
                return Id::makeUndefined();
              }
            }();
            resultCol[oldSize + targetIndex] = id;
            numUndef += static_cast<size_t>(id.isUndefined());
          }
        }};

    size_t nextResultColIdx = 0;
    // First write all the join columns.
    for (size_t col = 0; col < numJoinColumns_; col++) {
      if (keepJoinColumns_) {
        writeJoinColumn(col, nextResultColIdx);
        ++nextResultColIdx;
      }
    }

    // Then the remaining columns from the left input.
    for (size_t col = numJoinColumns_; col < inputLeft().numColumns(); ++col) {
      writeNonJoinColumn.template operator()<true>(col, nextResultColIdx);
      ++nextResultColIdx;
    }

    // Then the remaining columns from the right input.
    for (size_t col = numJoinColumns_; col < inputRight().numColumns(); col++) {
      writeNonJoinColumn.template operator()<false>(col, nextResultColIdx);
      ++nextResultColIdx;
    }

    indexBuffer_.clear();
    optionalIndexBuffer_.clear();
    nextIndex_ = 0;
    std::invoke(blockwiseCallback_, result, mergedVocab_);
    // The current `IdTable`s might still be active, so we have to merge the
    // local vocabs again if all other sets were moved-out.
    if (resultTable_.empty()) {
      // Make sure to reset `mergedVocab_` so it is in a valid state again.
      mergedVocab_ = LocalVocab{};
      // Only merge non-null vocabs.
      auto range = currentVocabs_ | ql::views::filter(toBool) |
                   ql::views::transform(dereference);
      mergedVocab_.mergeWith(ql::ranges::ref_view{range});
    }
  }
  const IdTableView<0>& inputLeft() const {
    return inputLeftAndRight_.value()[0];
  }

  const IdTableView<0>& inputRight() const {
    return inputLeftAndRight_.value()[1];
  }

  void checkNumColumns() const {
    AD_CONTRACT_CHECK(bufferSize_ > 0);
    AD_CONTRACT_CHECK(inputLeft().numColumns() >= numJoinColumns_);
    AD_CONTRACT_CHECK(inputRight().numColumns() >= numJoinColumns_);
    AD_CONTRACT_CHECK(
        resultTable_.numColumns() ==
        inputLeft().numColumns() + inputRight().numColumns() - numJoinColumns_ -
            numJoinColumns_ * static_cast<size_t>(!keepJoinColumns_));
  }
};
}  // namespace ad_utility

#endif  // QLEVER_SRC_ENGINE_ADDCOMBINEDROWTOTABLE_H
