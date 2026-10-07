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

  // The target of a `BIND` must not be visible before the `BIND`. If a child
  // contains it anyway, it is hidden in this operation (e.g. by a subquery), so
  // the `BIND` must not be pushed down: the new operation built from the
  // children would also see the hidden column of that child.
  if (ql::ranges::any_of(children, [&bind](const auto& child) {
        return child != nullptr && child->containsVariable(bind._target);
      })) {
    return std::nullopt;
  }

  // Get the variables used in the bind expression (not the target).
  const auto& bindExpressionVars = bind._expression.containedVariables();

  // For each child that covers all expression variables, check whether the bind
  // can be pushed down into that child.
  bool anyChildRewritten = false;
  for (auto& child : children) {
    if (child == nullptr) {
      // This can happen for a `SpatialJoin` that doesn't have all of its
      // children attached yet.
      continue;
    }
    if (!child->getRootOperation()->areVariablesAlwaysDefined(
            bindExpressionVars)) {
      continue;
    }
    auto result = child->getRootOperation()->makeTreeWithBindColumn(bind);
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

#endif  // QLEVER_SRC_ENGINE_OPERATIONBINDPUSHDOWNIMPL_H_
