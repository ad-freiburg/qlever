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
  IdTableView<0> input1_;
  IdTableView<0> input2_;
  IdTable* resultTable_;

  // This struct stores the information, which row indices from the input are
  // combined into a given row index in the output, i.e. "To obtain the
  // `targetIndex`-th row in the output, you have to combine
  // `input1_[rowIndices[0]]` and and `input2_[rowIndices[1]]`.
  struct TargetIndexAndRowIndices {
    size_t targetIndex;
    std::array<size_t, 2> rowIndices;
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

  // The first row index in the output for which a result has neither been
  // written nor stored in one of the buffers.
  size_t nextIndex_ = 0ul;

  // The number of rows for which the indices are buffered until they are
  // materialized and written to the result in one go.
  size_t bufferSize_ = 100'000;

 public:
  // Construct from the number of join columns, the two inputs, and the output.
  explicit AddCombinedRowToIdTable(size_t numJoinColumns,
                                   const IdTableView<0>& input1,
                                   const IdTableView<0>& input2,
                                   IdTable* output)
      : numUndefinedPerColumn_(output->numColumns()),
        numJoinColumns_{numJoinColumns},
        input1_{input1},
        input2_{input2},
        resultTable_{output} {
    AD_CONTRACT_CHECK(output->numColumns() == input1.numColumns() +
                                                  input2.numColumns() -
                                                  numJoinColumns);
  }

  // Return the number of UNDEF values per column. The result is only valid
  // after all rows have been added and `flush()` has been called.
  const std::vector<size_t>& numUndefinedPerColumn() const {
    return numUndefinedPerColumn_;
  }

  // The next free row in the output will be created from `input1_[rowIndexA]`
  // and `input2_[rowIndexB]`.
  void addRow(size_t rowIndexA, size_t rowIndexB) {
    indexBuffer_.push_back(
        TargetIndexAndRowIndices{nextIndex_, {rowIndexA, rowIndexB}});
    ++nextIndex_;
    if (nextIndex_ > bufferSize_) {
      flush();
    }
  }

  // The next free row in the output will be created from `input1_[rowIndexA]`.
  // The columns from `input2_` will all be set to UNDEF
  void addOptionalRow(size_t rowIndexA) {
    optionalIndexBuffer_.push_back(
        TargetIndexAndRowIndex{nextIndex_, rowIndexA});
    ++nextIndex_;
    if (nextIndex_ > bufferSize_) {
      flush();
    }
  }

  // The destructor throws if `flush()` hasn't been called immediately before,
  // because that is very likely a bug, because the complete result is not
  // written until `flush()` is called.
  ~AddCombinedRowToIdTable() {
    // TODO<joka921> Change this to an
    // `OnDestructionDontThrowDuringStackUnwinding` to make it safe.
    if (!indexBuffer_.empty()) {
      AD_THROW(
          "Before destroying an object of type AddCombinedRowToIdTable, the "
          "`flush` method must be called. Please report this");
    }
  }

  // Write the result rows the indices of which have been stored in the buffers
  // since the last call to `flush()`. This function is automatically called by
  // the `addRow` functions if the buffers exceed a certain size, but you also
  // have to call it manually after adding the last row, else the destructor
  // will throw an exception.
  void flush() {
    auto& result = *resultTable_;
    size_t oldSize = result.size();
    AD_CORRECTNESS_CHECK(nextIndex_ ==
                         indexBuffer_.size() + optionalIndexBuffer_.size());
    result.resize(oldSize + nextIndex_);

    // Sometimes columns are combined where one value is UNDEF and the other one
    // is not. This function very efficiently returns the not-UNDEF value in
    // this case.
    auto mergeWithUndefined = [](const ValueId a, const ValueId b) {
      static_assert(ValueId::makeUndefined().getBits() == 0u);
      return ValueId::fromBits(a.getBits() | b.getBits());
    };

    size_t resultColIdx = 0;

    // Write the next column of the result. There are two ways to call this
    // function:
    // 1. For columns that appear in both inputs (join columns). In this case,
    // `input1` and `input2` must be specified as `inputs...` and
    // `idxOfSingleColumn` is ignored.
    // 2. only one of the inputs. Then `idxOfSingleColumn` must be `0` for
    // `input1` and `1` for `input2`.
    auto writeColumn = [&]<size_t idxOfSingleColumn =
                               std::numeric_limits<size_t>::max()>(
        size_t column, const std::same_as<IdTableView<0>> auto&... inputs) {
      static constexpr size_t numInputs = sizeof...(inputs);
      static_assert(numInputs == 1 || numInputs == 2);
      static_assert(numInputs == 2 || idxOfSingleColumn < 2);
      AD_CORRECTNESS_CHECK((numInputs == 1) || column < numJoinColumns_);
      std::tuple<decltype(inputs.getColumn(column))...> cols{
          inputs.getColumn(column)...};
      // TODO<joka921> Implement prefetching.
      decltype(auto) resultCol = result.getColumn(resultColIdx);
      size_t& numUndef = numUndefinedPerColumn_.at(resultColIdx);

      // Write the matching rows. For join columns (numInputs == 2) the inputs
      // are combined, for non-join-columns, the (single) input is just copied.
      for (auto [targetIndex, sourceIndices] : indexBuffer_) {
        auto resultId = [&]() -> Id {
          if constexpr (numInputs == 1) {
            return std::get<0>(
                cols)[std::get<idxOfSingleColumn>(sourceIndices)];
          } else {
            return mergeWithUndefined(std::get<0>(cols)[sourceIndices[0]],
                                      std::get<1>(cols)[sourceIndices[1]]);
          }
        }();
        numUndef += resultId == Id::makeUndefined();
        resultCol[oldSize + targetIndex] = resultId;
      }

      // Write the optional rows. For the second input those are always
      // undefined.
      for (const auto& [targetIndex, sourceIndex] : optionalIndexBuffer_) {
        if constexpr (idxOfSingleColumn == 0) {
          resultCol[oldSize + targetIndex] = std::get<0>(cols)[sourceIndex];
        } else {
          resultCol[oldSize + targetIndex] = Id::makeUndefined();
        }
      }
      ++resultColIdx;
    };

    // First write all the join columns.
    for (size_t col = 0; col < numJoinColumns_; col++) {
      writeColumn(col, input1_, input2_);
    }

    // Then the remaining columns from the first input.
    for (size_t col = numJoinColumns_; col < input1_.numColumns(); ++col) {
      writeColumn.template operator()<0>(col, input1_);
    }

    // Then the remaining columns from the second input.
    for (size_t col = numJoinColumns_; col < input2_.numColumns(); col++) {
      writeColumn.template operator()<1>(col, input2_);
    }

    indexBuffer_.clear();
    optionalIndexBuffer_.clear();
    nextIndex_ = 0ul;
  }
};
}  // namespace ad_utility