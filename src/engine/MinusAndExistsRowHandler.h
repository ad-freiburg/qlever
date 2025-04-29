//   Copyright 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef MINUSANDEXISTSROWHANDLER_H
#define MINUSANDEXISTSROWHANDLER_H

#include <cstdint>
#include <optional>
#include <vector>

#include "backports/concepts.h"
#include "engine/LocalVocab.h"
#include "engine/idTable/IdTable.h"
#include "util/CancellationHandle.h"
#include "util/ChunkedForLoop.h"
#include "util/TransparentFunctors.h"

namespace ad_utility {

// TODO<RobinTF> merge with AddCombinedRowToTable
namespace detail::concepts {
template <typename T>
CPP_requires(HasAsStaticView,
             requires(T& table)(table.template asStaticView<0>()));

template <typename T>
CPP_requires(HasGetLocalVocab, requires(T& table)(table.getLocalVocab()));
}  // namespace detail::concepts

namespace detail {

constexpr size_t CHUNK_SIZE = 100'000;

struct ExistsImpl {
  template <typename CancellationAction>
  static void handle(IdTable& idTable, const std::vector<size_t>&,
                     const std::vector<size_t>& nonMatchingIndices,
                     size_t startIndex, size_t endIndex,
                     const IdTableView<0>& inputTable,
                     const CancellationAction& action) {
    AD_CORRECTNESS_CHECK(idTable.numColumns() == inputTable.numColumns() + 1);
    size_t oldSize = idTable.size();
    idTable.resize(oldSize + (endIndex - startIndex));
    size_t col = 0;
    for (auto column : inputTable.getColumns()) {
      chunkedCopy(column.subspan(startIndex, endIndex - startIndex),
                  idTable.getColumn(col).begin() + oldSize, CHUNK_SIZE, action);
      col++;
    }
    auto lastColumn =
        idTable.getColumn(idTable.numColumns() - 1).subspan(oldSize);
    chunkedFill(lastColumn, Id::makeFromBool(true), CHUNK_SIZE, action);
    for (size_t index : nonMatchingIndices) {
      lastColumn[index - startIndex] = Id::makeFromBool(false);
    }
  }
};

struct MinusImpl {
  template <typename CancellationAction>
  static void handle(IdTable& idTable,
                     const std::vector<size_t>& matchingIndices,
                     const std::vector<size_t>&, size_t startIndex,
                     size_t endIndex, const IdTableView<0>& inputTable,
                     const CancellationAction& action) {
    AD_EXPENSIVE_CHECK(ql::ranges::is_sorted(matchingIndices));
    size_t oldSize = idTable.size();
    AD_CORRECTNESS_CHECK(endIndex - startIndex >= matchingIndices.size());
    idTable.resize(oldSize + (endIndex - startIndex - matchingIndices.size()));
    size_t col = 0;
    for (auto targetColumn : idTable.getColumns()) {
      auto inputColumn = inputTable.getColumn(col);
      auto columnIt = targetColumn.begin() + oldSize;
      size_t lastIndex = startIndex;
      for (size_t index : matchingIndices) {
        auto inputRange = inputColumn.subspan(lastIndex, index - lastIndex);
        chunkedCopy(inputRange, columnIt, CHUNK_SIZE, action);
        columnIt += inputRange.size();
        lastIndex = index + 1;
      }
      auto inputRange = inputColumn.subspan(lastIndex, endIndex - lastIndex);
      chunkedCopy(inputRange, columnIt, CHUNK_SIZE, action);
      AD_CORRECTNESS_CHECK(columnIt + inputRange.size() == targetColumn.end());
      col++;
    }
  }
};

template <typename Impl>
class MinusAndExistsRowHandler {
  std::vector<size_t> numUndefinedPerColumn_;
  size_t numJoinColumns_;
  std::optional<IdTableView<0>> inputLeft_;
  IdTable resultTable_;
  LocalVocab mergedVocab_{};
  const LocalVocab* currentVocab_ = nullptr;

  std::vector<size_t> indexBuffer_;
  // Store the indices of OPTIONAL inputs that have not yet been written.
  std::vector<size_t> optionalIndexBuffer_;

  std::optional<size_t> startIndex_ = std::nullopt;
  size_t endIndex_ = 0;
  // This callback is called with the result as an argument each time `flush()`
  // is called. It can be used to consume parts of the result early, before the
  // complete operation has finished.
  using BlockwiseCallback = std::function<void(IdTable&, LocalVocab&)>;
  [[no_unique_address]] BlockwiseCallback blockwiseCallback_;

  using CancellationHandle = ad_utility::SharedCancellationHandle;
  CancellationHandle cancellationHandle_;

 public:
  // Similar to the previous constructor, but the inputs are not given.
  // This means that the inputs have to be set to an explicit
  // call to `setInput` before adding rows. This is used for the lazy join
  // operations (see Join.cpp) where the input changes over time.
  explicit MinusAndExistsRowHandler(size_t numJoinColumns, IdTable output,
                                    CancellationHandle cancellationHandle,
                                    BlockwiseCallback blockwiseCallback)
      : numUndefinedPerColumn_(output.numColumns()),
        numJoinColumns_{numJoinColumns},
        inputLeft_{std::nullopt},
        resultTable_{std::move(output)},
        blockwiseCallback_{std::move(blockwiseCallback)},
        cancellationHandle_{std::move(cancellationHandle)} {
    AD_CONTRACT_CHECK(cancellationHandle_);
  }

  // Return the number of UNDEF values per column.
  const std::vector<size_t>& numUndefinedPerColumn() {
    flush();
    return numUndefinedPerColumn_;
  }

  void addRow(size_t index, size_t) {
    AD_EXPENSIVE_CHECK(inputLeft_.has_value());
    if (indexBuffer_.empty() || indexBuffer_.back() < index) {
      indexBuffer_.push_back(index);
    } else {
      // The indices must be strictly increasing (unless they are duplicates)
      AD_EXPENSIVE_CHECK(
          indexBuffer_.size() >= indexBuffer_.back() - index &&
              indexBuffer_.at(indexBuffer_.size() - 1 -
                              (indexBuffer_.back() - index)) == index,
          "Non-sequential value was not a duplicate!");
    }
    if (!startIndex_.has_value()) {
      startIndex_ = index;
    } else {
      AD_EXPENSIVE_CHECK(startIndex_.value() <= index);
    }
    AD_EXPENSIVE_CHECK(endIndex_ <= index + 1);
    endIndex_ = index + 1;
  }

  // Unwrap type `T` to get an `IdTableView<0>`, even if it's not an
  // `IdTableView<0>`. Identity for `IdTableView<0>`.
  template <typename T>
  static IdTableView<0> toView(const T& table) {
    if constexpr (CPP_requires_ref(detail::concepts::HasAsStaticView, T)) {
      return table.template asStaticView<0>();
    } else {
      return table;
    }
  }

  // Merge the local vocab contained in `T` with the `mergedVocab_` and set the
  // passed pointer reference to that vocab.
  template <typename T>
  void mergeVocab(const T& table, const LocalVocab*& currentVocab) {
    AD_CORRECTNESS_CHECK(currentVocab == nullptr);
    if constexpr (CPP_requires_ref(detail::concepts::HasGetLocalVocab, T)) {
      currentVocab = &table.getLocalVocab();
      mergedVocab_.mergeWith(table.getLocalVocab());
    }
  }

  // Flush remaining pending entries before changing the input.
  void flushBeforeInputChange() {
    // Clear to avoid unnecessary merge.
    currentVocab_ = nullptr;
    if (startIndex_.has_value()) {
      AD_CORRECTNESS_CHECK(inputLeft_.has_value());
      flush();
    } else if (resultTable_.empty()) {
      // Clear local vocab when no rows were written.
      //
      // TODO<joka921, RobinTF> This is a conservative approach. We could
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
  void setInput(const L& inputLeft, const R&) {
    flushBeforeInputChange();
    mergeVocab(inputLeft, currentVocab_);
    inputLeft_ = toView(inputLeft);
    AD_CONTRACT_CHECK(inputLeft_.value().numColumns() >= numJoinColumns_);
  }

  // Only set the left input. After this it is only allowed to call
  // `addOptionalRow` and not `addRow` until `setInput` has been called again.
  template <typename L>
  void setOnlyLeftInputForOptionalJoin(const L& inputLeft) {
    flushBeforeInputChange();
    mergeVocab(inputLeft, currentVocab_);
    // The right input will be empty, but with the correct number of columns.
    inputLeft_ = toView(inputLeft);
    AD_CONTRACT_CHECK(inputLeft_.value().numColumns() >= numJoinColumns_);
  }

  // The next free row in the output will be created from
  // `inputLeft_[rowIndexA]`. The columns from `inputRight_` will all be set to
  // UNDEF
  void addOptionalRow(size_t rowIndexA) {
    AD_EXPENSIVE_CHECK(inputLeft_.has_value());
    optionalIndexBuffer_.push_back(rowIndexA);
    if (!startIndex_.has_value()) {
      startIndex_ = rowIndexA;
    } else {
      AD_EXPENSIVE_CHECK(startIndex_.value() < rowIndexA);
    }
    AD_EXPENSIVE_CHECK(endIndex_ <= rowIndexA + 1);
    endIndex_ = rowIndexA + 1;
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
  MinusAndExistsRowHandler(const MinusAndExistsRowHandler&) = delete;
  MinusAndExistsRowHandler& operator=(const MinusAndExistsRowHandler&) = delete;
  MinusAndExistsRowHandler(MinusAndExistsRowHandler&&) = delete;
  MinusAndExistsRowHandler& operator=(MinusAndExistsRowHandler&&) = delete;

  // Write the result rows the indices of which have been stored in the buffers
  // since the last call to `flush()`. This function is automatically called by
  // the `addRow` functions if the buffers exceed a certain size, but you also
  // have to call it manually after adding the last row, else the destructor
  // will throw an exception.
  void flush() {
    cancellationHandle_->throwIfCancelled();
    auto& result = resultTable_;
    // Sometimes the left input and right input are not valid anymore, because
    // the `IdTable`s they point to have already been destroyed. This case is
    // okay, as long as there was a manual call to `flush` (after which
    // `!startIndex_.has_value()`) before the inputs went out of scope.
    if (!startIndex_.has_value()) {
      return;
    }
    AD_CORRECTNESS_CHECK(inputLeft_.has_value());

    Impl::handle(result, indexBuffer_, optionalIndexBuffer_,
                 startIndex_.value(), endIndex_, inputLeft(),
                 [this]() { cancellationHandle_->throwIfCancelled(); });

    indexBuffer_.clear();
    optionalIndexBuffer_.clear();
    startIndex_ = std::nullopt;
    endIndex_ = 0;
    std::invoke(blockwiseCallback_, result, mergedVocab_);
    // The current `IdTable`s might still be active, so we have to merge the
    // local vocabs again if all other sets were moved-out.
    if (resultTable_.empty()) {
      // Make sure to reset `mergedVocab_` so it is in a valid state again.
      mergedVocab_ = LocalVocab{};
      // Only merge non-null vocabs.
      if (currentVocab_) {
        mergedVocab_.mergeWith(*currentVocab_);
      }
    }
  }
  const IdTableView<0>& inputLeft() const { return inputLeft_.value(); }
};
}  // namespace detail

using MinusRowHandler = detail::MinusAndExistsRowHandler<detail::MinusImpl>;
using ExistsRowHandler = detail::MinusAndExistsRowHandler<detail::ExistsImpl>;
}  // namespace ad_utility

#endif  // MINUSANDEXISTSROWHANDLER_H
