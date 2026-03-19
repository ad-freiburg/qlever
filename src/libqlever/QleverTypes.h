// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Christoph Ullinger <ullingec@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_LIBQLEVER_QLEVERTYPES_H
#define QLEVER_SRC_LIBQLEVER_QLEVERTYPES_H

#include <memory>
#include <tuple>

// This module contains type aliases from `libqlever`, which need to be in a
// separate file to break cyclic dependencies.

// Forward declarations.
class QueryExecutionTree;
class QueryExecutionContext;
class ParsedQuery;

namespace qlever {
using QueryPlan =
    std::tuple<std::shared_ptr<QueryExecutionTree>,
               std::shared_ptr<QueryExecutionContext>, ParsedQuery>;
}

#endif  // QLEVER_SRC_LIBQLEVER_QLEVERTYPES_H
