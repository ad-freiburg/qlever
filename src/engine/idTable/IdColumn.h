// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Pascal Keßler <kesslerp@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_IDTABLE_IDCOLUMN_H
#define QLEVER_SRC_ENGINE_IDTABLE_IDCOLUMN_H

#include "backports/span.h"
#include "global/Id.h"

// Type aliases for the columns of an `IdTable`. Currently just aliases for
// `ql::span<Id>`/`ql::span<const Id>`; a later commit switches them to a
// storage-efficient split-column view, once `IdTable` stores the payload and
// datatype of each `Id` in separate arrays.
using IdColumnRef = ql::span<Id>;
using ConstIdColumnRef = ql::span<const Id>;

#endif  // QLEVER_SRC_ENGINE_IDTABLE_IDCOLUMN_H
