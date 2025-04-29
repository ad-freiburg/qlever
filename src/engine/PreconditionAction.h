//   Copyright 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef PRECONDITIONACTION_H
#define PRECONDITIONACTION_H

#include <memory>
#include <variant>

#include "backports/concepts.h"
#include "util/Exception.h"
#include "util/TypeTraits.h"

// forward declaration needed to break dependencies
class QueryExecutionTree;

namespace qlever {
// Represent how a precondition of an operation can be satisfied.
// `isImplicitlySatisfied()` will return true if the `Operation` in question is
// already naturally fulfilling the requested requirement.
// `mustBeSatisfiedExternally()` will return true if the `Operation` cannot
// fulfil the requirement on its own and needs external support.
// These two cases are mutually exclusive. If both options are false, the
// operation created a query execution tree that represents a modification of
// itself that can fulfil the requirement.
class PreconditionAction {
 public:
  class ImplicitlySatisfied {};
  class MustBeSatisfiedExternally {};

 private:
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

  // Return `*this` if the precondition is already satisfied or an execution
  // tree has already been computed.
  CPP_template(typename Func)(
      requires ad_utility::InvocableWithConvertibleReturnType<
          Func, std::shared_ptr<QueryExecutionTree>>) PreconditionAction
      handle(Func handler) && {
    if (std::holds_alternative<MustBeSatisfiedExternally>(value_)) {
      return PreconditionAction{std::invoke(handler)};
    }
    return std::move(*this);
  }

  // Return an optional containing an execution tree if the tree has already
  // been computed, `std::nullopt` otherwise.
  std::optional<std::shared_ptr<QueryExecutionTree>> getTree() && {
    if (std::holds_alternative<std::shared_ptr<QueryExecutionTree>>(value_)) {
      return std::move(std::get<std::shared_ptr<QueryExecutionTree>>(value_));
    }
    return std::nullopt;
  }
};

const inline PreconditionAction PreconditionAction::IMPLICITLY_SATISFIED =
    PreconditionAction{ImplicitlySatisfied{}};
const inline PreconditionAction PreconditionAction::SATISFY_EXTERNALLY =
    PreconditionAction{MustBeSatisfiedExternally{}};
}  // namespace qlever

#endif  // PRECONDITIONACTION_H
