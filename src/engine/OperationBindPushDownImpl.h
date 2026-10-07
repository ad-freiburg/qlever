// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_OPERATIONBINDPUSHDOWNIMPL_H_
#define QLEVER_SRC_ENGINE_OPERATIONBINDPUSHDOWNIMPL_H_

#include "engine/Operation.h"
#include "engine/QueryExecutionTree.h"

// _____________________________________________________________________________
CPP_template_def(typename MakeCloneWithNewChildren)(
    requires ad_utility::InvocableWithExactReturnType<
        MakeCloneWithNewChildren, std::shared_ptr<QueryExecutionTree>,
        std::vector<std::shared_ptr<QueryExecutionTree>>>)
    std::optional<std::shared_ptr<QueryExecutionTree>> Operation::
        pushDownBindToAnyChild(
            const parsedQuery::Bind& bind,
            std::vector<std::shared_ptr<QueryExecutionTree>> children,
            MakeCloneWithNewChildren makeCloneWithNewChildren) const {
  if (children.empty()) {
    return std::nullopt;
  }

  // For each child that can safely compute the `BIND` expression, check whether
  // the `BIND` can be pushed down into that child.
  bool anyChildRewritten = false;
  for (auto& child : children) {
    if (child == nullptr) {
      // This can happen for a `SpatialJoin` that doesn't have all of its
      // children attached yet.
      continue;
    }
    if (!canPushBindIntoChild(bind, *child, children) ||
        child->containsVariable(bind._target)) {
      continue;
    }
    auto result = QueryExecutionTree::makeTreeWithBindColumn(child, bind);
    if (result.has_value()) {
      child = result.value();
      anyChildRewritten = true;
      break;
    }
  }

  if (!anyChildRewritten) {
    return std::nullopt;
  }
  return makeCloneWithNewChildren(std::move(children));
}

// _____________________________________________________________________________
inline bool Operation::canPushBindIntoChild(
    const parsedQuery::Bind& bind, const QueryExecutionTree& child,
    const std::vector<std::shared_ptr<QueryExecutionTree>>& children) {
  return ql::ranges::all_of(
      bind._expression.containedVariables(), [&](const Variable* var) {
        if (child.getRootOperation()->isVariableAlwaysDefined(*var)) {
          return true;
        }
        return child.containsVariable(*var) &&
               ql::ranges::none_of(children, [&](const auto& other) {
                 return other != nullptr && other.get() != &child &&
                        other->containsVariable(*var);
               });
      });
}

#endif  // QLEVER_SRC_ENGINE_OPERATIONBINDPUSHDOWNIMPL_H_
