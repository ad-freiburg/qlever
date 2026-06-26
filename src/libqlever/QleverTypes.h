// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Christoph Ullinger <ullingec@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_LIBQLEVER_QLEVERTYPES_H
#define QLEVER_SRC_LIBQLEVER_QLEVERTYPES_H

#include <memory>
#include <tuple>

#include "engine/QueryExecutionContext.h"
#include "engine/QueryExecutionTree.h"
#include "parser/ParsedQuery.h"

// This module contains type aliases from `libqlever`, which need to be in a
// separate file to break cyclic dependencies.

// Forward declarations.
class QueryExecutionTree;
class QueryExecutionContext;
class ParsedQuery;

namespace qlever {

// Helper struct bundling a parsed query with a query execution tree.
// We store both `QueryExecutionTree` and `QueryExecutionContext` as
// `shared_ptr` to avoid lifetime issues especially in the asynchronous server
// code.
struct PlannedQuery {
 private:
  // NOTE: `qec_` must be declared before `queryExecutionTree_` so that it
  // is destroyed after it. The `QueryExecutionTree` holds operations with
  // raw `_executionContext` pointers to the QEC, and their lazy result
  // cleanup accesses the QEC via `signalQueryUpdate`. If `qec_` is the
  // last `shared_ptr` and is destroyed first, the QEC is freed while the
  // operations still reference it.
  std::shared_ptr<QueryExecutionContext> qec_;
  ParsedQuery parsedQuery_;
  std::shared_ptr<QueryExecutionTree> queryExecutionTree_;

 public:
  PlannedQuery(ParsedQuery pq, QueryExecutionTree qet,
               QueryExecutionContext& qec)
      : qec_{qec.shared_from_this()},
        parsedQuery_{std::move(pq)},
        queryExecutionTree_{
            std::make_shared<QueryExecutionTree>(std::move(qet))} {
    AD_CORRECTNESS_CHECK(qec_.get() == queryExecutionTree_->getQec());
  }

  const ParsedQuery& parsedQuery() const { return parsedQuery_; }
  ParsedQuery& parsedQuery() { return parsedQuery_; }

  const QueryExecutionTree& queryExecutionTree() const {
    return *queryExecutionTree_;
  }
  QueryExecutionTree& queryExecutionTree() { return *queryExecutionTree_; }
  std::shared_ptr<const QueryExecutionTree> sharedQueryExecutionTree() const {
    return queryExecutionTree_;
  }

  const QueryExecutionContext& queryExecutionContext() const { return *qec_; }
  QueryExecutionContext& queryExecutionContext() { return *qec_; }
  std::shared_ptr<const QueryExecutionContext> sharedQueryExecutionContext()
      const {
    return qec_;
  }
};
}  // namespace qlever

#endif  // QLEVER_SRC_LIBQLEVER_QLEVERTYPES_H
