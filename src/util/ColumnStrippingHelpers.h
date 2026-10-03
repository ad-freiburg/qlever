// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Anna Kaiser <anna.kaiser@uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef COLUMN_STRIPPING_HELPERS_H
#define COLUMN_STRIPPING_HELPERS_H

#include <set>

#include "engine/StripColumns.h"
#include "rdfTypes/Variable.h"
#include "util/Algorithm.h"
#include "engine/QueryExecutionTree.h"

// A helper for the column stripping of operations.
// It collects a set of variables (specified in the constructor and via the
// `add` function), together with the common optimization for the case that all
// variables are part of the set specified in the constructor. This optimizes
// the common case that the set of variables that is exported from an operation
// is a superset of the set of variables that this operation needs from its
// children.
class VarsRequiredFromSubtree {
 private:
 // Is used so that assert is executed before dereferencation of pointer.
  static const std::set<Variable>& checkAndDereference(
      const std::set<Variable>* ptr) {
    AD_CORRECTNESS_CHECK(ptr != nullptr);
    return *ptr;
  }

 private:
  // Buffer variable
  std::set<Variable> newVariables_;
  // The resulting variables that are required from the subtree.
  const std::set<Variable>* varsRequiredFromSubtree_;
  // Store the variables that are requested by the Parenttree.
  const std::set<Variable> varsRequestedFromParentTree_;

 public:
  // `varsRequestedFromParentTree` must outlive this object, as its address
  // is stored in `varsRequiredFromSubtree_`.
  explicit VarsRequiredFromSubtree(
      const std::set<Variable>* varsRequestedFromParentTree)
      : varsRequiredFromSubtree_{varsRequestedFromParentTree},
        varsRequestedFromParentTree_{
            checkAndDereference(varsRequestedFromParentTree)} {}

  // The function add() has to be called whenever there are variables that are
  // needed by the operation itself to be executed. This function adds all these
  // variables to varsRequiredFromSubtree_ in case they are not already part of
  // varsRequiredFromSubtree_.
  void add(const Variable& varForOperation) {
    if (ad_utility::contains(*varsRequiredFromSubtree_, varForOperation)) {
      return;
    }
    if (varsRequiredFromSubtree_ != &newVariables_) {
      newVariables_ = varsRequestedFromParentTree_;
      varsRequiredFromSubtree_ = &newVariables_;
    }
    newVariables_.insert(varForOperation);
  }

  // Return all variables that are required form the subtree after having added
  // all relevant variables via add().
  const std::set<Variable>& get() const { return *varsRequiredFromSubtree_; }

  // FRIEND_TESTs
  FRIEND_TEST(VarsRequiredFromSubtree, add);
};

// A helper for the column stripping of operations.
// This function creates an execution tree with the given Operation as its root.
// Some operations need certain variables to perform their operation, even
// though these variables are not necessarily part of the result requested by
// the parent. (For example, the operation Sort needs the variables it sorts by,
// but the parent may request the sorted result without requesting those
// variables themselves.) If such a variable is needed by the operation but not
// requested by the parent, an additional StripColumns operation is inserted
// above the given operation to remove it after the operation has been
// executed. If all variables needed by the operation are also requested by
// the parent, the tree with the given Operation as root is returned
// unchanged and without an additional StripColumns operation.
  // TODO <joka921> It would be more efficient but more complicated to tell the
  // DISTINCT operation directly to not export some of its keepIndices_.
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
