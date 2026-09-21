#ifndef QLEVER_SRC_ENGINE_IDTABLE_IDCOLUMN_H
#define QLEVER_SRC_ENGINE_IDTABLE_IDCOLUMN_H

#include "backports/span.h"
#include "global/Id.h"

// Type aliases for the columns of an `IdTable`. Currently just aliases for
// `ql::span<Id>`/`ql::span<const Id>`; a later commit switches them to a
// storage-efficient split-column view, once `IdTable` stores the payload and
// datatype of each `Id` in separate arrays.
using IdColumn = ql::span<Id>;
using ConstIdColumn = ql::span<const Id>;

#endif  // QLEVER_SRC_ENGINE_IDTABLE_IDCOLUMN_H
