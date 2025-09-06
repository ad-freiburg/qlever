//   Copyright 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef MINUSROWHANDLER_H
#define MINUSROWHANDLER_H

#include <cstdint>
#include <optional>
#include <vector>

#include "backports/concepts.h"
#include "engine/LocalVocab.h"
#include "engine/idTable/IdTable.h"
#include "engine/idTable/IdTableConcepts.h"
#include "util/CancellationHandle.h"
#include "util/ChunkedForLoop.h"

namespace ad_utility {

// Row handler for the `Minus` operation. Instead of materializing matching rows
// like `AddCombinedRowToIdTable` it only keeps non-matching rows and skips all
// other rows.
class MinusRowHandler {
  // Number of columns that are being joined. Currently always 1.
  size_t numJoinColumns_;
  // Store reference to left input table.
  std::optional<IdTableView<0>> inputLeft_ = std::nullopt;
  // Output `IdTable`.
  IdTable resultTable_;
  // Output `LocalVocab`.
  LocalVocab mergedVocab_{};
  // Pointer to the current `LocalVocab` of the left input.
  const LocalVocab* currentVocab_ = nullptr;
  // Non-matching indices of the left input, the ones to copy to the result.
  std::vector<size_t> indexBuffer_{};
  // This callback is called with the result as an argument each time `flush()`
  // is called. It can be used to consume parts of the result early, before the
  // complete operation has finished.
  using BlockwiseCallback = std::function<void(IdTable&, LocalVocab&)>;
  [[no_unique_address]] BlockwiseCallback blockwiseCallback_;

  using CancellationHandle = ad_utility::SharedCancellationHandle;
  CancellationHandle cancellationHandle_;

  // The number of rows to handle at once before checking the cancellation
  // handle during `handle()`.
  static constexpr size_t CHUNK_SIZE = 100'000;

 public:
  // Construct a `MinusRowHandler` from the number of join columns, the output
  // `IdTable` which is used to materialize the individual rows, the
  // `CancellationHandle` that is checked on every flush and the
  // `BlockwiseCallback` that is called whenever there are new materialized
  // values.
  explicit MinusRowHandler(size_t numJoinColumns, IdTable output,
                           CancellationHandle cancellationHandle,
                           BlockwiseCallback blockwiseCallback)
      : numJoinColumns_{numJoinColumns},
        resultTable_{std::move(output)},
        blockwiseCallback_{std::move(blockwiseCallback)},
        cancellationHandle_{std::move(cancellationHandle)} {
    AD_CONTRACT_CHECK(cancellationHandle_);
  }

  // No-op for `MINUS`.
  static void addRow(size_t, size_t) {
    // `BlockZipperJoinImpl` expects this interface.
  }

  // No-op for `MINUS`.
  template <typename R1, typename R2>
  static void addRows(const R1&, const R2&) {
    // `BlockZipperJoinImpl` expects this interface.
  }

  // Flush remaining pending entries before changing the input.
  void flushBeforeInputChange() {
    // Clear to avoid unnecessary merge.
    currentVocab_ = nullptr;
    if (!indexBuffer_.empty()) {
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
  // `asStaticView<0>` method that returns an `IdTableView<0>`. The right table
  // is completely ignored.
  template <typename L, typename R>
  void setInput(const L& inputLeft, const R&) {
    setOnlyLeftInputForOptionalJoin(inputLeft);
  }

  // Only set the left input. After this it is only allowed to call
  // `addOptionalRow` and not `addRow` until `setInput` has been called again.
  template <typename L>
  void setOnlyLeftInputForOptionalJoin(const L& inputLeft) {
    flushBeforeInputChange();
    detail::mergeVocabInto(inputLeft, currentVocab_, mergedVocab_);
    inputLeft_ = detail::toView(inputLeft);
    AD_CONTRACT_CHECK(inputLeft_.value().numColumns() >= numJoinColumns_);
  }

  // Store the next non-matching row to keep.
  void addOptionalRow(size_t rowIndexA) {
    AD_EXPENSIVE_CHECK(inputLeft_.has_value());
    indexBuffer_.push_back(rowIndexA);
  }

  // Move the result out after the last write. The function ensures, that the
  // `flush()` is called before doing so.
  IdTable&& resultTable() && {
    flush();
    return std::move(resultTable_);
  }

  // Get the output `LocalVocab`.
  LocalVocab& localVocab() { return mergedVocab_; }

  // Disable copying and moving, it is currently not needed and makes it harder
  // to reason about
  MinusRowHandler(const MinusRowHandler&) = delete;
  MinusRowHandler& operator=(const MinusRowHandler&) = delete;
  MinusRowHandler(MinusRowHandler&&) = delete;
  MinusRowHandler& operator=(MinusRowHandler&&) = delete;

  // Write the result rows the indices of which have been stored in the buffers
  // since the last call to `flush()`.
  void flush() {
    cancellationHandle_->throwIfCancelled();
    // Sometimes the left input and right input are not valid anymore, because
    // the `IdTable`s they point to have already been destroyed. This case is
    // okay, as long as there was a manual call to `flush` (after which
    // `indexBuffer_.empty()`) before the inputs went out of scope.
    if (indexBuffer_.empty()) {
      return;
    }
    AD_CORRECTNESS_CHECK(inputLeft_.has_value());

    handle();

    indexBuffer_.clear();
    std::invoke(blockwiseCallback_, resultTable_, mergedVocab_);
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
  // Process pending rows and materialize them into the actual table.
  void handle() {
    AD_EXPENSIVE_CHECK(ql::ranges::is_sorted(indexBuffer_));
    size_t oldSize = resultTable_.size();
    resultTable_.resize(oldSize + indexBuffer_.size());

    auto action = [this]() { cancellationHandle_->throwIfCancelled(); };
    for (const auto& [outputColumn, inputColumn] : ::ranges::zip_view(
             resultTable_.getColumns(), inputLeft_.value().getColumns())) {
      chunkedCopy(ql::views::transform(
                      indexBuffer_,
                      [&inputColumn](size_t row) { return inputColumn[row]; }),
                  outputColumn.begin() + oldSize, CHUNK_SIZE, action);
    }
  }
};

}  // namespace ad_utility

#endif  // MINUSROWHANDLER_H
