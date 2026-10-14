// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Anna Kaiser <anna.kaiser@uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_COLUMNSTRIPPINGHELPERS_H
#define QLEVER_SRC_ENGINE_COLUMNSTRIPPINGHELPERS_H

#include "engine/QueryExecutionTree.h"
#include "engine/StripColumns.h"
#include "rdfTypes/Variable.h"
#include "util/Algorithm.h"

// Helpers for the implementations of `Operation::makeTreeWithStrippedColumns`.
namespace columnStrippingHelpers {

// The set of variables that an operation requires from its subtree: the
// variables requested by the parent plus the variables that the operation
// itself needs (added via `add`). The requested set is copied only when `add`
// actually adds a variable, which optimizes the common case that the parent
// already requests all variables that the operation needs.
class VarsRequiredFromSubtree {
 private:
  // The copy of the requested variables with the added variables, only used
  // once a variable has actually been added.
  std::set<Variable> newVariables_;
  // Points to the requested variables, or to `newVariables_` once a variable
  // has been added.
  const std::set<Variable>* varsRequiredFromSubtree_;

 public:
  // The pointed-to set must outlive this object and must not be modified while
  // this object is in use.
  explicit VarsRequiredFromSubtree(
      const std::set<Variable>* varsRequestedByParent)
      : varsRequiredFromSubtree_{varsRequestedByParent} {
    AD_CORRECTNESS_CHECK(varsRequestedByParent != nullptr);
  }

  // Add a variable that the operation itself needs from its subtree.
  void add(const Variable& variable) {
    if (ad_utility::contains(*varsRequiredFromSubtree_, variable)) {
      return;
    }
    if (varsRequiredFromSubtree_ != &newVariables_) {
      newVariables_ = *varsRequiredFromSubtree_;
      varsRequiredFromSubtree_ = &newVariables_;
    }
    newVariables_.insert(variable);
  }

  // Return all variables that are required from the subtree.
  const std::set<Variable>& get() const { return *varsRequiredFromSubtree_; }
};

// Return true iff all variables of `qet` are contained in
// `variablesRequestedByParent`.
inline bool allVariablesAreRequired(
    const QueryExecutionTree& qet,
    const std::set<Variable>& variablesRequestedByParent) {
  return ql::ranges::all_of(
      qet.getVariableColumns() | ql::views::keys,
      [&variablesRequestedByParent](const Variable& variable) {
        return ad_utility::contains(variablesRequestedByParent, variable);
      });
}

// Create an execution tree with a new `Operation` (constructed from `qec` and
// `args`) as its root. If the operation produces variables that are not
// contained in `variablesRequestedByParent`, add a `StripColumns` operation on
// top that removes them.
//
// NOTE: Some operations always produce certain variables, even if the parent
// does not request them. For example, `Distinct` always produces the variables
// it compares on. TODO<joka921> It would be more efficient, but also more
// complicated, to tell such an operation directly not to export these
// variables.
template <typename Operation, typename... Args>
std::optional<std::shared_ptr<QueryExecutionTree>>
makeTreeWithOptionalStripOperation(
    QueryExecutionContext* qec,
    const std::set<Variable>& variablesRequestedByParent, Args&&... args) {
  auto tree = ad_utility::makeExecutionTree<Operation>(
      qec, std::forward<Args>(args)...);
  if (allVariablesAreRequired(*tree, variablesRequestedByParent)) {
    return tree;
  }
  return ad_utility::makeExecutionTree<StripColumns>(
      qec, std::move(tree), variablesRequestedByParent);
}
}  // namespace columnStrippingHelpers

#endif  // QLEVER_SRC_ENGINE_COLUMNSTRIPPINGHELPERS_H
