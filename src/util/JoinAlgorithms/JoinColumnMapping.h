//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_UTIL_JOINALGORITHMS_JOINCOLUMNMAPPING_H
#define QLEVER_SRC_UTIL_JOINALGORITHMS_JOINCOLUMNMAPPING_H

#include <array>
#include <cstdint>
#include <vector>

#include "engine/LocalVocab.h"
#include "engine/idTable/IdTable.h"
#include "global/Id.h"
#include "util/Algorithm.h"
#include "util/TransparentFunctors.h"

namespace ad_utility {
// The implementations of the join algorithms (merge/zipper join, galloping
// join) in `JoinAlgorithms.h` assume that their inputs only consist of the join
// columns. They also write a result where the join columns come first, then the
// non-join columns of the left input and then the non-join columns of the right
// input. This format is different from the formats that the join operations
// (`Join`, `OptionalJoin`, `MultiColumnJoin` etc.) provide and expect: The
// columns of the input are arbitrarily ordered and there is an explicit data
// structure that contains the information which columns are the join columns.
// The output must be first all columns from the left input (in the same order)
// and then all the columns in the right input, but without the join columns
// (also in the same order). The following struct stores the information, which
// permutations have to be applied to the input and the result to convert from
// one of those formats to the other.
// NOTE: This mapping always has to be consistent with the one created by
// `makeVarToColMapForJoinOperation` in `VariableToColumnMap.h.
class JoinColumnMapping {
 private:
  // For a documentation of those members, see the getter function with the same
  // name below.
  std::vector<ColumnIndex> jcsLeft_;
  std::vector<ColumnIndex> jcsRight_;
  std::vector<ColumnIndex> permutationLeft_;
  std::vector<ColumnIndex> permutationRight_;
  std::vector<ColumnIndex> permutationResult_;

 public:
  // The join columns in the left input. For example if `jcsLeft()` is `[3, 0]`
  // this means that the primary join column is the third column of the left
  // input, and the secondary join column is the 0-th column of the left input.
  const std::vector<ColumnIndex>& jcsLeft() const { return jcsLeft_; }
  // The same, but for the right input.
  const std::vector<ColumnIndex>& jcsRight() const { return jcsRight_; }

  // This permutation has to be applied to the left input to obtain the column
  // order expected by the join algorithms (see above for details on the
  // different orderings). For example `permutationLeft()[0]` is `jcsLeft_[0]`
  // and `permutationLeft()[numJoinColumns]` is the index of the first non-join
  // column in the left input.
  const std::vector<ColumnIndex>& permutationLeft() const {
    return permutationLeft_;
  };
  // The same, but for the right input.
  const std::vector<ColumnIndex>& permutationRight() const {
    return permutationRight_;
  }
  // This permutation has to be applied to the result of the join algorithms to
  // obtain the column order that the `Join` subclasses of `Operation` expect.
  // For example, `permutationResult[jcsLeft[0]] = 0`.
  const std::vector<ColumnIndex>& permutationResult() const {
    return permutationResult_;
  }

  // Construct the mapping from the `joinColumn` (given as pairs of
  // (leftColIndex, rightColIndex)`), and the total number of columns in the
  // left and right input respectively.
  JoinColumnMapping(const std::vector<std::array<ColumnIndex, 2>>& joinColumns,
                    size_t numColsLeft, size_t numColsRight,
                    bool keepJoinColumns = true) {
    permutationResult_.resize(numColsLeft + numColsRight - joinColumns.size());
    for (auto [colA, colB] : joinColumns) {
      permutationResult_.at(colA) = jcsLeft_.size();
      jcsLeft_.push_back(colA);
      jcsRight_.push_back(colB);
    }

    permutationLeft_ = jcsLeft_;
    permutationRight_ = jcsRight_;

    for (auto i = ColumnIndex{0}; i < numColsLeft; ++i) {
      if (!ad_utility::contains(jcsLeft_, i)) {
        permutationResult_.at(i) = permutationLeft_.size();
        permutationLeft_.push_back(i);
      }
    }

    size_t numSkippedJoinColumns = 0;
    for (auto i = ColumnIndex{0}; i < numColsRight; ++i) {
      if (!ad_utility::contains(jcsRight_, i)) {
        permutationResult_.at(i - numSkippedJoinColumns + numColsLeft) =
            i - numSkippedJoinColumns + numColsLeft;
        permutationRight_.push_back(i);
      } else {
        ++numSkippedJoinColumns;
      }
    }

    // If the join columns are not kept, the result permutation is just the
    // identity, because the original permutation preserves the order of the
    // non-join-columns.
    if (!keepJoinColumns) {
      permutationResult_.clear();
      ql::ranges::copy(ad_utility::integerRange(numColsLeft + numColsRight -
                                                2 * joinColumns.size()),
                       std::back_inserter(permutationResult_));
    }
  }
};

struct GetColsFromTable {
  template <size_t numCols, typename Table>
  decltype(auto) operator()(Table& table) {
    return [&table]<size_t... I>(std::index_sequence<I...>) {
      return ::ranges::views::zip(table.getColumn(I)...) |
             ::ranges::views::transform([](auto&& tuple) {
               return std::apply(
                   [](auto&... refs) { return std::array{refs...}; },
                   AD_FWD(tuple));
             });
    }(std::make_index_sequence<numCols>());
  }
};

// A class that stores a complete `IdTable`, but when being treated as a range
// via the `begin/end/operator[]` functions, then it only gives `const` access
// to the first `numCols`(via the `GetColsFromTable` struct above). This is very
// useful for the lazy join implementations (currently used in `Join.cpp` and
// `OptionalJoin.cpp`), where we need very efficient access to the join column
// for comparing rows, but also need to store the complete table to be able to
// write the other columns of a matching row to the result. This class is
// templated so we can use it for `IdTable` as well as for `IdTableView`.
// Note: The current implementation always copies the columns when they are
// accessed (as a `std::array<Id, numCols>`. The reason is, that we want
// something with a constant size that can be iterated via a runtime for-loop.
// `std::array` can't store references, and `std::tuple<Id&...>` can't be
// iterated.
// TODO<joka921> Implement an iterable tuple of the same types, but actually,
// for only two or three columns the full arrays (which can be optimized by the
// compiler) shouldn't be too bad..
template <size_t numCols, typename Table>
struct IdTableAndFirstCols {
 private:
  Table table_;
  LocalVocab localVocab_;

 public:
  // Typedef needed for generic interfaces.
  using ConstBaseIterator = ql::ranges::iterator_t<
      decltype(GetColsFromTable{}.template operator()<numCols>(
          std::declval<const Table&>()))>;
  using iterator = ConstBaseIterator;
  using const_iterator = ConstBaseIterator;
  // Get access to the first column.
  decltype(auto) cols() const {
    return GetColsFromTable{}.template operator()<numCols>(table_);
  }
  // Construct by taking ownership of the table.
  IdTableAndFirstCols(Table t, LocalVocab localVocab)
      : table_{std::move(t)}, localVocab_{std::move(localVocab)} {}

  // The following functions all refer to the same column.
  const_iterator begin() const { return cols().begin(); }
  const_iterator end() const { return cols().end(); }

  bool empty() const { return cols().empty(); }

  decltype(auto) operator[](size_t idx) const { return cols()[idx]; }
  decltype(auto) front() const { return cols().front(); }
  decltype(auto) back() const { return cols().back(); }

  size_t size() const { return cols().size(); }

  // Note: This function only refers to the exposed `numCols` column, not to all
  // the columns in the underlying `Table`. This interface is currently used by
  // the `specialOptionalJoin` function in `JoinAlgorithms.h`.
  constexpr size_t numColumns() const { return numCols; }
  decltype(auto) getColumn(size_t columnIndex) const {
    return table_.getColumn(columnIndex);
  }

  // This interface is required in `Join.cpp` by the `AddCombinedRowToTable`
  // class. Calling this function yields the same type, no matter if `Table` is
  // `IdTable` or `IdTableView`. In addition, it refers to the full underlying
  // table, not only to the first `numColumns` tables.
  template <size_t I = 0>
  IdTableView<I> asStaticView() const {
    return table_.template asStaticView<I>();
  }

  const LocalVocab& getLocalVocab() const { return localVocab_; }
};

// Specialization of `IdTableAndFirstCol` for only a single column where we
// don't need to copy into an `array`, but directly return single `Id&`.

// Note: this changes the interface (in particular the single rows can't be
// iterated over), but currently this is used by the `Join` class, which expects
// this interface.
template <typename Table>
struct IdTableAndFirstCols<1, Table> {
 private:
  Table table_;
  LocalVocab localVocab_;

 public:
  // Typedef needed for generic interfaces.
  using iterator = std::decay_t<decltype(table_.getColumn(0).begin())>;
  using const_iterator =
      std::decay_t<decltype(std::as_const(table_).getColumn(0).begin())>;

  // Construct by taking ownership of the table.
  IdTableAndFirstCols(Table t, LocalVocab localVocab)
      : table_{std::move(t)}, localVocab_{std::move(localVocab)} {}

  // Get access to the first column.
  decltype(auto) col() { return table_.getColumn(0); }
  decltype(auto) col() const { return table_.getColumn(0); }

  // The following functions all refer to the same column.
  auto begin() { return col().begin(); }
  auto end() { return col().end(); }
  auto begin() const { return col().begin(); }
  auto end() const { return col().end(); }

  bool empty() const { return col().empty(); }

  const Id& operator[](size_t idx) const { return col()[idx]; }
  const Id& front() const { return col().front(); }
  const Id& back() const { return col().back(); }

  size_t size() const { return col().size(); }

  // This interface is required in `Join.cpp` by the `AddCombinedRowToTable`
  // class. Calling this function yields the same type, no matter if `Table` is
  // `IdTable` or `IdTableView`. In addition, it refers to the full underlying
  // table, not only to the first `numColumns` tables.
  template <size_t I = 0>
  IdTableView<I> asStaticView() const {
    return table_.template asStaticView<I>();
  }

  const LocalVocab& getLocalVocab() const { return localVocab_; }
};

// Helper function to create an `IdTableAndFirstCols` with explicitly stated
// `numCols`, but deduce type of the table.
template <size_t NumCols, typename Table>
IdTableAndFirstCols<NumCols, std::decay_t<Table>> makeIdTableAndFirstCols(
    Table&& table, LocalVocab localVocab) {
  return {AD_FWD(table), std::move(localVocab)};
}

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_JOINALGORITHMS_JOINCOLUMNMAPPING_H
