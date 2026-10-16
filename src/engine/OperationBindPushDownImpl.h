#ifndef QLEVER_SRC_ENGINE_OPERATIONBINDPUSHDOWNIMPL_H_
#define QLEVER_SRC_ENGINE_OPERATIONBINDPUSHDOWNIMPL_H_

#include "engine/Operation.h"
#include "engine/QueryExecutionTree.h"

CPP_template_def(typename MakeCloneWithNewChildren)(
    requires ad_utility::InvocableWithExactReturnType<
        MakeCloneWithNewChildren, qlm::shared_ptr<QueryExecutionTree>,
        qlm::vector<qlm::shared_ptr<QueryExecutionTree>>>)
qlm::optional<qlm::shared_ptr<QueryExecutionTree>> Operation::
    pushDownBindToAnyChild(
        const parsedQuery::Bind& bind,
        qlm::vector<qlm::shared_ptr<QueryExecutionTree>> children,
        MakeCloneWithNewChildren makeCloneWithNewChildren) const {
  if (children.empty()) {
    return std::nullopt;
  }

  if (getInternallyVisibleVariableColumns().contains(bind._target) ||
      ql::ranges::any_of(children, [&bind](const auto& child) {
        return child != nullptr && child->containsVariable(bind._target);
      })) {
    return std::nullopt;
  }

  const auto& bindExpressionVars = bind._expression.containedVariables();
  if (!areVariablesVisible(bindExpressionVars)) {
    return std::nullopt;
  }

  bool anyChildRewritten = false;
  for (auto& child : children) {
    if (child == nullptr ||
        !child->getRootOperation()->areVariablesAlwaysDefined(
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

  auto newTree = makeCloneWithNewChildren(std::move(children));
  keepHiddenVariablesHidden(*newTree->getRootOperation(), bind._target);
  return newTree;
}

#endif  // QLEVER_SRC_ENGINE_OPERATIONBINDPUSHDOWNIMPL_H_
