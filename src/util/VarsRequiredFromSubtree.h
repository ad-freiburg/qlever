// Copyright 2026, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Anna Kaiser (anna.kaiser@uni-freiburg.de)

#ifndef VARS_REQUIRED_FROM_SUBTREE_H
#define VARS_REQUIRED_FROM_SUBTREE_H

#include <set>

#include "rdfTypes/Variable.h"
#include "util/Algorithm.h"

// This class collects all the variables that are required from the Subtree.
// The resulting variables are the ones that are requested from the parent tree
// and also the ones that are needed by the operation itself to be executed.
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
    if (!ad_utility::contains(*varsRequiredFromSubtree_, varForOperation)) {
      if (varsRequiredFromSubtree_ == &varsRequestedFromParentTree_) {
        newVariables_ = varsRequestedFromParentTree_;
        varsRequiredFromSubtree_ = &newVariables_;
      }
      newVariables_.insert(varForOperation);
    }
  }

  // Return all variables that are required form the subtree after having added
  // all relevant variables via add().
  const std::set<Variable>& get() const { return *varsRequiredFromSubtree_; }
};

#endif  // VARS_REQUIRED_FROM_SUBTREE_H
