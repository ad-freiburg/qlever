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
class PreconditionAction {
  class AlreadySatisfied {};
  class NotSatisfiable {};

  // Hold the underlying state, or an alternative tree that satisfies the
  // condition.
  std::variant<AlreadySatisfied, NotSatisfiable,
               std::shared_ptr<QueryExecutionTree>>
      value_;

  // Construct exclusively from the two tags.
  explicit PreconditionAction(const AlreadySatisfied& tag) : value_{tag} {}
  explicit PreconditionAction(const NotSatisfiable& tag) : value_{tag} {}

 public:
  // Copy and move constructors and assignment operators are defaulted.
  PreconditionAction(PreconditionAction&& other) = default;
  PreconditionAction& operator=(PreconditionAction&& other) = default;
  PreconditionAction(const PreconditionAction&) = default;
  PreconditionAction& operator=(const PreconditionAction&) = default;

  // Publicly accessible tags for the precondition action.
  static const PreconditionAction ALREADY_SATISFIED;
  static const PreconditionAction NOT_SATISFIABLE;

  // Construct a precondition action from a tree that satisfies the condition.
  explicit PreconditionAction(std::shared_ptr<QueryExecutionTree> tree)
      : value_{std::move(tree)} {}

  // Check if the precondition is already satisfied.
  bool isAlreadySatisfied() const {
    return std::holds_alternative<AlreadySatisfied>(value_);
  }

  // Check if the precondition is not satisfiable.
  bool isNotSatisfiable() const {
    return std::holds_alternative<NotSatisfiable>(value_);
  }

  // Get the tree that satisfies the precondition.
  std::shared_ptr<QueryExecutionTree> getTree() && {
    AD_CONTRACT_CHECK(
        std::holds_alternative<std::shared_ptr<QueryExecutionTree>>(value_));
    return std::move(std::get<std::shared_ptr<QueryExecutionTree>>(value_));
  }
};

const inline PreconditionAction PreconditionAction::ALREADY_SATISFIED =
    PreconditionAction{AlreadySatisfied{}};
const inline PreconditionAction PreconditionAction::NOT_SATISFIABLE =
    PreconditionAction{NotSatisfiable{}};

#endif  // PRECONDITIONACTION_H
