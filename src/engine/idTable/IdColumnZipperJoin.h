// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_IDTABLE_IDCOLUMNZIPPERJOIN_H
#define QLEVER_SRC_ENGINE_IDTABLE_IDCOLUMNZIPPERJOIN_H

#include "backports/concepts.h"
#include "engine/idTable/IdColumn.h"
#include "engine/idTable/ZipperJoiner.h"

namespace columnBasedIdTable {

// Merge/zipper join of two sorted columns without UNDEF values. Calls
// `addRow(leftIndex, rightIndex)` per match, `addRows(leftBegin, leftEnd,
// rightBegin, rightEnd)` for groups, and `notFoundAction(leftIndex)` for
// unmatched left elements (pass `ad_utility::noop` unless this is an
// OPTIONAL join or a MINUS). See `ZipperJoiner` for the algorithm.
CPP_template(typename AddRow, typename AddRows, typename NotFoundAction,
             typename CancelCallback)(
    requires ql::concepts::invocable<AddRow, size_t, size_t> CPP_and
        ql::concepts::invocable<AddRows, size_t, size_t, size_t, size_t>
            CPP_and ql::concepts::invocable<NotFoundAction, size_t>
                CPP_and ql::concepts::invocable<
                    CancelCallback>) void zipperJoinIdColumns(ConstIdColumn
                                                                  left,
                                                              ConstIdColumn
                                                                  right,
                                                              AddRow addRow,
                                                              AddRows addRows,
                                                              NotFoundAction
                                                                  notFoundAction,
                                                              CancelCallback
                                                                  cancelCallback) {
  ZipperJoiner<AddRow, AddRows, NotFoundAction, CancelCallback>{
      left, right, addRow, addRows, notFoundAction, cancelCallback}
      .run();
}

}  // namespace columnBasedIdTable

#endif  // QLEVER_SRC_ENGINE_IDTABLE_IDCOLUMNZIPPERJOIN_H
