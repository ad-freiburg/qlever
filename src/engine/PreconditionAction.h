//   Copyright 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef PRECONDITIONACTION_H
#define PRECONDITIONACTION_H

#include <memory>
#include <variant>

#include "util/Exception.h"

// forward declaration needed to break dependencies
class QueryExecutionTree;

// Represent how a precondition of an operation can be satisfied.
// `isImplicitlySatisfied()` will return true if the `Operation` in question is
// already naturally fulfilling the requested requirement.
// `mustBeSatisfiedExternally()` will return true if the `Operation` cannot
// fulfil the requirement on its own and needs external support.
// These two cases are mutually exclusive. If both options are false, the
// operation created a query execution tree that represents a modification of
// itself that can fulfil the requirement.
class PreconditionAction {
  class ImplicitlySatisfied {};
  class MustBeSatisfiedExternally {};

  // Hold the underlying state, or an alternative tree that satisfies the
  // condition.
  std::variant<ImplicitlySatisfied, MustBeSatisfiedExternally,
               std::shared_ptr<QueryExecutionTree>>
      value_;

  // Construct exclusively from the two tags.
  explicit PreconditionAction(ImplicitlySatisfied tag) : value_{tag} {}
  explicit PreconditionAction(MustBeSatisfiedExternally tag) : value_{tag} {}

 public:
  // Copy and move constructors and assignment operators are defaulted.
  PreconditionAction(PreconditionAction&& other) = default;
  PreconditionAction& operator=(PreconditionAction&& other) = default;
  PreconditionAction(const PreconditionAction&) = default;
  PreconditionAction& operator=(const PreconditionAction&) = default;

  // Publicly accessible tags for the precondition action.
  static const PreconditionAction IMPLICITLY_SATISFIED;
  static const PreconditionAction SATISFY_EXTERNALLY;

  // Construct a precondition action from a tree that satisfies the condition.
  explicit PreconditionAction(std::shared_ptr<QueryExecutionTree> tree)
      : value_{std::move(tree)} {}

  // Check if the precondition is already implicitly satisfied.
  bool isImplicitlySatisfied() const {
    return std::holds_alternative<ImplicitlySatisfied>(value_);
  }

  // Check if the precondition cannot be satisfied by the operation on its own.
  bool mustBeSatisfiedExternally() const {
    return std::holds_alternative<MustBeSatisfiedExternally>(value_);
  }

  // Get the tree that satisfies the precondition.
  std::shared_ptr<QueryExecutionTree> getTree() && {
    AD_CONTRACT_CHECK(
        std::holds_alternative<std::shared_ptr<QueryExecutionTree>>(value_));
    return std::move(std::get<std::shared_ptr<QueryExecutionTree>>(value_));
  }
};

const inline PreconditionAction PreconditionAction::IMPLICITLY_SATISFIED =
    PreconditionAction{ImplicitlySatisfied{}};
const inline PreconditionAction PreconditionAction::SATISFY_EXTERNALLY =
    PreconditionAction{MustBeSatisfiedExternally{}};

#endif  // PRECONDITIONACTION_H
