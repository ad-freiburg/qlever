// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Anna Kaiser <anna.kaiser@uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_COLUMNSTRIPPINGHELPERS_H
#define QLEVER_SRC_UTIL_COLUMNSTRIPPINGHELPERS_H

#include "engine/QueryExecutionTree.h"
#include "engine/StripColumns.h"
#include "rdfTypes/Variable.h"
#include "util/Algorithm.h"

// A helper for the column stripping of operations.
// It collects a set of variables (specified in the constructor and via the
// `add` function), together with the common optimization for the case that all
// variables are part of the set specified in the constructor. This optimizes
// the common case that the set of variables that is exported from an operation
// is a superset of the set of variables that this operation needs from its
// children.
class VarsRequiredFromSubtree {
 private:
  // Stores the variables after a new variable has been added.
  std::set<Variable> newVariables_;
  // Points to the set of variables required from the subtree.
  const std::set<Variable>* varsRequiredFromSubtree_;

 public:
  // `varsRequestedFromParentTree` must outlive this object, as its address
  // is stored in `varsRequiredFromSubtree_`.
  // Important: Do not modify the pointed-to set `varsRequestedFromParentTree`
  // while this object is in use. Modifying it externally while this object is
  // in use will affect the behavior of the `add` function.

  explicit VarsRequiredFromSubtree(
      const std::set<Variable>* varsRequestedFromParentTree)
      : varsRequiredFromSubtree_{varsRequestedFromParentTree} {
    AD_CORRECTNESS_CHECK(varsRequestedFromParentTree != nullptr);
  }

  // The `add` function has to be called for every variable that is needed by
  // the operation itself to be executed. It adds the variable to
  // `varsRequiredFromSubtree_` if it is not already contained in the set.
  void add(const Variable& varForOperation) {
    if (ad_utility::contains(*varsRequiredFromSubtree_, varForOperation)) {
      return;
    }
    if (varsRequiredFromSubtree_ != &newVariables_) {
      newVariables_ = *varsRequiredFromSubtree_;
      varsRequiredFromSubtree_ = &newVariables_;
    }
    newVariables_.insert(varForOperation);
  }

  // Returns all variables that are required from the subtree after all relevant
  // variables have been added via the `add` function.
  const std::set<Variable>& get() const { return *varsRequiredFromSubtree_; }

  // FRIEND_TESTs
  FRIEND_TEST(VarsRequiredFromSubtree, add);
};

namespace columnStrippingHelpers {
// A helper for the column stripping of operations.
// It returns true when all variables provided by the `qet` are requested
// by the parent operation. Otherwise, it returns false.
inline bool allVariablesAreRequired(
    std::shared_ptr<QueryExecutionTree> qet,
    const std::set<Variable>& variablesRequestedFromParent) {
  return ql::ranges::all_of(
      qet->getVariableColumns() | ql::views::keys,
      [&variablesRequestedFromParent](const Variable& varNeeded) {
        return ad_utility::contains(variablesRequestedFromParent, varNeeded);
      });
}

// A helper for the column stripping of operations.
// This function creates an execution tree with the given Operation as its root.
// If any of the variables produced by the resulting operation are *not*
// contained in `variablesRequestedFromParent`, an additional `StripColumns`
// operation for those variables is added on top of the operation.
// Use case: Some operations currently produce certain variables even though
// these variables are not necessarily part of the result requested by the
// parent. (For example, the operation Sort always produces the variables it
// sorts by, but the parent may request the sorted result without requesting
// those variables themselves.) If all variables produced by the operation are
// also requested by the parent, the tree with the given operation as root is
// returned unchanged and without an additional `StripColumns` operation.
// TODO <joka921> It would be more efficient but more complicated to tell the
// operation directly not to export some of its produced or needed variables.
// (for example: The needed variables of the `Distinct` operation are contained
// in its `keepIndices_`)
template <typename Operation, typename... Args>
std::optional<std::shared_ptr<QueryExecutionTree>>
makeTreeWithOptionalStripOperation(
    QueryExecutionContext* qec,
    const std::set<Variable>& variablesRequestedFromParent, Args&&... args) {
  // Create query execution tree with the given operation as root.
  auto treeWithOperationAsRoot = ad_utility::makeExecutionTree<Operation>(
      qec, std::forward<Args>(args)...);

  // Check whether all variables needed for the given operation are also
  // requested from the parent and return the `QueryExecutionTree` with or
  // without an additional `StripColumns` operation.
  if (allVariablesAreRequired(treeWithOperationAsRoot,
                              variablesRequestedFromParent)) {
    return treeWithOperationAsRoot;
  }

  return ad_utility::makeExecutionTree<StripColumns>(
      qec, std::move(treeWithOperationAsRoot), variablesRequestedFromParent);
}
}  // namespace columnStrippingHelpers

#endif  // QLEVER_SRC_UTIL_COLUMNSTRIPPINGHELPERS_H
