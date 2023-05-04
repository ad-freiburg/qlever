//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include <array>
#include <cstdint>
#include <vector>

#include "engine/idTable/IdTable.h"
#include "global/Id.h"
#include "util/Exception.h"

namespace ad_utility {
// This class handles the efficient writing of the results of a JOIN operation
// to a column-based `IdTable`. The underlying assumption is that in both inputs
// the join columns are the first columns.
class AddCombinedRowToIdTable {
  std::vector<size_t> numUndefinedPerColumn_;
  size_t numJoinColumns_;
  IdTableView<0> inputLeft_;
  IdTableView<0> inputRight_;
  IdTable resultTable_;

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

  // Store the information, which row index from the first input is written to a
  // given index in the output. This is used for OPTIONAL joins where there are
  // rows that have no counterpart in the second input.
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

 public:
  // Construct from the number of join columns, the two inputs, and the output.
  // The `bufferSize` can be configured for testing.
  explicit AddCombinedRowToIdTable(size_t numJoinColumns,
                                   const IdTableView<0>& input1,
                                   const IdTableView<0>& input2, IdTable output,
                                   size_t bufferSize = 100'000)
      : numUndefinedPerColumn_(output.numColumns()),
        numJoinColumns_{numJoinColumns},
        inputLeft_{input1},
        inputRight_{input2},
        resultTable_{std::move(output)},
        bufferSize_{bufferSize} {
    AD_CORRECTNESS_CHECK(resultTable_.numColumns() == input1.numColumns() +
                                                          input2.numColumns() -
                                                          numJoinColumns);
    AD_CORRECTNESS_CHECK(input1.numColumns() >= numJoinColumns &&
                         input2.numColumns() >= numJoinColumns);
    AD_CORRECTNESS_CHECK(resultTable_.empty());
  }

  // Return the number of UNDEF values per column.
  const std::vector<size_t>& numUndefinedPerColumn() {
    flush();
    return numUndefinedPerColumn_;
  }

  // The next free row in the output will be created from
  // `inputLeft_[rowIndexA]` and `inputRight_[rowIndexB]`.
  void addRow(size_t rowIndexA, size_t rowIndexB) {
    indexBuffer_.push_back(
        TargetIndexAndRowIndices{nextIndex_, {rowIndexA, rowIndexB}});
    ++nextIndex_;
    if (nextIndex_ > bufferSize_) {
      flush();
    }
  }

  // The next free row in the output will be created from
  // `inputLeft_[rowIndexA]`. The columns from `inputRight_` will all be set to
  // UNDEF
  void addOptionalRow(size_t rowIndexA) {
    optionalIndexBuffer_.push_back(
        TargetIndexAndRowIndex{nextIndex_, rowIndexA});
    ++nextIndex_;
    if (nextIndex_ > bufferSize_) {
      flush();
    }
  }

  // Move the result out after the last write. The function ensures, that the
  // `flush()` is called before doing so.
  IdTable&& resultTable() && {
    flush();
    return std::move(resultTable_);
  }

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
    auto& result = resultTable_;
    size_t oldSize = result.size();
    AD_CORRECTNESS_CHECK(nextIndex_ ==
                         indexBuffer_.size() + optionalIndexBuffer_.size());
    result.resize(oldSize + nextIndex_);

    // Sometimes columns are combined where one value is UNDEF and the other one
    // is not. This function very efficiently returns the not-UNDEF value in
    // this case.
    // TODO<joka921> If we keep track of the information that one of the
    // involved columns contains no UNDEF values at all, we can omit this step
    // and simply copy the values from this column without looking at the other
    // input.
    auto mergeWithUndefined = [](const ValueId a, const ValueId b) {
      static_assert(ValueId::makeUndefined().getBits() == 0u);
      return ValueId::fromBits(a.getBits() | b.getBits());
    };

    // A lambda that writes the join column with the given `colIdx` to the
    // `nextResultColIdx`-th column of the result.
    auto writeJoinColumn = [&result, &mergeWithUndefined, oldSize, this](
                               size_t colIdx, size_t resultColIdx) {
      const auto& colLeft = inputLeft_.getColumn(colIdx);
      const auto& colRight = inputRight_.getColumn(colIdx);
      // TODO<joka921> Implement prefetching.
      decltype(auto) resultCol = result.getColumn(resultColIdx);
      size_t& numUndef = numUndefinedPerColumn_.at(resultColIdx);

      // Write the matching rows.
      for (const auto& [targetIndex, sourceIndices] : indexBuffer_) {
        auto resultId = mergeWithUndefined(colLeft[sourceIndices[0]],
                                           colRight[sourceIndices[1]]);
        numUndef += static_cast<size_t>(resultId.isUndefined());
        resultCol[oldSize + targetIndex] = resultId;
      }

      // Write the optional rows. For the second input those are always
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
    auto writeNonJoinColumn = [&result, oldSize, this]<bool isColFromLeft>(
                                  size_t colIdx, size_t resultColIdx) {
      decltype(auto) col = isColFromLeft ? inputLeft_.getColumn(colIdx)
                                         : inputRight_.getColumn(colIdx);
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

      // Write the optional rows. For the second input those are always
      // undefined.
      for (const auto& [targetIndex, sourceIndex] : optionalIndexBuffer_) {
        Id id = [&col, sourceIndex = sourceIndex]() {
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
    };

    size_t nextResultColIdx = 0;
    // First write all the join columns.
    for (size_t col = 0; col < numJoinColumns_; col++) {
      writeJoinColumn(col, nextResultColIdx);
      ++nextResultColIdx;
    }

    // Then the remaining columns from the first input.
    for (size_t col = numJoinColumns_; col < inputLeft_.numColumns(); ++col) {
      writeNonJoinColumn.operator()<true>(col, nextResultColIdx);
      ++nextResultColIdx;
    }

    // Then the remaining columns from the second input.
    for (size_t col = numJoinColumns_; col < inputRight_.numColumns(); col++) {
      writeNonJoinColumn.operator()<false>(col, nextResultColIdx);
      ++nextResultColIdx;
    }

    indexBuffer_.clear();
    optionalIndexBuffer_.clear();
    nextIndex_ = 0;
  }
};
}  // namespace ad_utility
