// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Prashanth Premakumar <prashanthp0703@gmail.com>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_GLOBAL_TRIPLETERMINDEX_H
#define QLEVER_SRC_GLOBAL_TRIPLETERMINDEX_H

#include <cstdint>

#include "global/TypedIndex.h"

// TODO<Prashanth0703> Investigate a payload marker for query-local triple terms
// before adding persistent term storage, to avoid consuming another datatype.
// Account for both ID layouts, ownership and equality with index-stored terms.
inline constexpr ad_utility::IndexTag tripleTermIndexTag = "TripleTermIndex";
using TripleTermIndex = ad_utility::TypedIndex<uint64_t, tripleTermIndexTag>;

#endif  // QLEVER_SRC_GLOBAL_TRIPLETERMINDEX_H
