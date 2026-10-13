// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#ifndef QLEVER_SRC_INDEX_IDTABLEUTILS_H
#define QLEVER_SRC_INDEX_IDTABLEUTILS_H

#include <algorithm>
#include <cstdint>
#include <limits>
#include <vector>

#include "backports/algorithm.h"
#include "backports/type_traits.h"
#include "engine/idTable/IdTable.h"
#include "global/RuntimeParameters.h"
#include "util/Executor.h"
#include "util/Log.h"
#include "util/blockSort/BlockIndirectSort.h"

class IdTableUtils {
  // The runtime parameter `sort-num-tasks-in-flight`, at least `1`.
  static uint32_t numSortTasksInFlight() {
    size_t numTasks =
        getRuntimeParameter<&RuntimeParameters::sortNumTasksInFlight_>();
    return static_cast<uint32_t>(
        std::clamp<size_t>(numTasks, 1, std::numeric_limits<uint32_t>::max()));
  }

 public:
  // The `sort` functions below sort in place (not stable) via
  // `blockIndirectSort` on the `executor`. They block the calling thread, which
  // therefore must not belong to the `executor`. The `executor` must not be a
  // strand.
  //
  // TODO<RobinTF> Pass the query's cancellation handle once
  // `blockIndirectSort` supports it.

  // Sort the rows of `tab` by the column `keyColumn`.
  template <size_t WIDTH>
  static void sort(IdTable* tab, const size_t keyColumn,
                   const qlever::Executor& executor) {
    sort<WIDTH>(
        tab,
        [keyColumn](const auto& a, const auto& b) {
          return a[keyColumn] < b[keyColumn];
        },
        executor);
  }

  // Sort the rows of `tab` by the comparator `comp`.
  //
  // The effect of the third template argument is that if C does not have
  // operator() with the specified arguments that returns bool, then this
  // template does not match (so that there is no confusion with the templated
  // function above). This is an instance of SFINAE.
  template <
      int WIDTH, typename C,
      typename = std::enable_if_t<std::is_same_v<
          bool, std::invoke_result_t<C, typename IdTableStatic<WIDTH>::row_type,
                                     typename IdTableStatic<WIDTH>::row_type>>>>
  static void sort(IdTable* tab, C comp, const qlever::Executor& executor) {
    AD_LOG_DEBUG << "Sorting " << tab->size() << " elements.\n";
    IdTableStatic<WIDTH> stab = std::move(*tab).toStatic<WIDTH>();
    ad_utility::blockSort::blockIndirectSort(stab, std::move(comp),
                                             numSortTasksInFlight(), executor);
    *tab = std::move(stab).toDynamic();
    AD_LOG_DEBUG << "Sort done.\n";
  }

  // Sort the rows of `idTable` lexicographically by the columns `sortCols`.
  static void sort(IdTable& idTable, const std::vector<ColumnIndex>& sortCols,
                   const qlever::Executor& executor);

  // Return the number of distinct rows in the `input`. The input must have all
  // duplicates adjacent to each other (e.g. by being sorted), otherwise the
  // behavior is undefined. `checkCancellation()` is invoked regularly and can
  // be used to implement a cancellation mechanism that throws on cancellation.
  static size_t countDistinct(const IdTable& input,
                              const std::function<void()>& checkCancellation);

  // Return true iff the `table` (an `IdTable` or a view of one) contains at
  // least one `Id` of type `Datatype::LocalVocabIndex`.
  template <typename Table>
  static bool containsLocalVocabIds(const Table& table) {
    return ql::ranges::any_of(table.getColumns(), [](const auto& column) {
      return ql::ranges::any_of(column, [](Id id) {
        return id.getDatatype() == Datatype::LocalVocabIndex;
      });
    });
  }
};

#endif  // QLEVER_SRC_INDEX_IDTABLEUTILS_H
