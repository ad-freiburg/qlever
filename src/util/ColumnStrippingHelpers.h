// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Anna Kaiser <anna.kaiser@uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef COLUMN_STRIPPING_HELPERS_H
#define COLUMN_STRIPPING_HELPERS_H

#include <set>

#include "engine/StripColumns.h"
#include "rdfTypes/Variable.h"
#include "util/Algorithm.h"

// A helper for the columns stripping of operations.
// It collects a set of variables (specified in the constructor and via the `add` function), together with the common optimization for the case that all variables are part of the set specified in the constructor.
// This optimizes the common case that the set of variables that is exported from an operation is a superset of the set of variables that this operation needs from its children.
// Please note, that the resulting variables that are required from the subtree
// can contain variables that the subtree does not provide. This is especially
// the case when an operation has several Subtrees (as for example the
// Join-Operation. In that case, the varsRequiredFromSubtree_ are the same for
// the left and the right subtree).
class VarsRequiredFromSubtree {
 private:
  // Buffer variable
  std::set<Variable> newVariables_;
  // The resulting variables that are required from the subtree.
  const std::set<Variable>* varsRequiredFromSubtree_;
  // Store the variables that are requested by the Parenttree.
  const std::set<Variable>& varsRequestedFromParentTree_;

 public:
  explicit VarsRequiredFromSubtree(
      const std::set<Variable>& varsRequestedFromParentTree)
      : varsRequiredFromSubtree_{&varsRequestedFromParentTree},
        varsRequestedFromParentTree_{varsRequestedFromParentTree} {}

  // The function add() has to be called whenever there are variables that are
  // needed by the operation itself to be executed. This function adds all these
  // variables to varsRequiredFromSubtree_ in case they are not already part of
  // varsRequiredFromSubtree_.
  void add(const Variable& varForOperation) {
    if (ad_utility::contains(*varsRequiredFromSubtree_, varForOperation)) {
      return;
    }
    if (varsRequiredFromSubtree_ == &varsRequestedFromParentTree_) {
      newVariables_ = varsRequestedFromParentTree_;
      varsRequiredFromSubtree_ = &newVariables_;
    }
    newVariables_.insert(varForOperation);
  }

  // Return all variables that are required form the subtree after having added
  // all relevant variables via add().
  const std::set<Variable>& get() const { return *varsRequiredFromSubtree_; }
};

// This function creates an execution tree which has the given Operation as
// root. There are some variables that are needed by the operation itself, but
// are not requested by the parent tree. In this case, an additional execution
// tree is generated, which inserts a StipColumn-Operation over the given
// Operation. This StripColumn-Operation strips the variables that were needed
// by the given operation but are not requested from the parent. If all
// variables, that are needed for the given operation to be executed are also
// requested from the parent, then the treeWithOperationAsRoot is returned and
// no additional StripColumns-Operation is inserted in the execution tree.
template <typename Operation, typename... Args>
std::optional<std::shared_ptr<QueryExecutionTree>>
makeTreeWithOptionalStripOperation(
    QueryExecutionContext* qec,
    const std::set<Variable>& variablesRequestedFromParent,
    std::vector<const Variable*> variablesNeededByOperation, Args&&... args) {
  // Create query execution tree with the given operation as root.
  auto treeWithOperationAsRoot = ad_utility::makeExecutionTree<Operation>(
      qec, std::forward<Args>(args)...);

  // check whether all variables needed for the given operation are also
  // requested from the parent. And either return the QueryExecutionTree with or
  // without an additional StripColumns-Operation.
  if (ql::ranges::all_of(
          variablesNeededByOperation,
          [&variablesRequestedFromParent](const Variable* varNeeded) {
            return ad_utility::contains(variablesRequestedFromParent,
                                        *varNeeded);
          })) {
    return treeWithOperationAsRoot;
  }
  return ad_utility::makeExecutionTree<StripColumns>(
      qec, std::move(treeWithOperationAsRoot), variablesRequestedFromParent);
}

#endif  // COLUMN_STRIPPING_HELPERS_H
