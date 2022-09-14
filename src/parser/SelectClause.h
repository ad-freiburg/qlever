// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2022-     Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)

#pragma once

#include <variant>
#include <vector>

#include "parser/Alias.h"
#include "parser/data/Variable.h"

namespace parsedQuery {

/// The `SELECT` clause of a  SPARQL query. It contains the selected variables
/// and aliases. If all variables are selected via `SELECT *` then this class
/// also stores the variables to which the `*` will expand.
struct SelectClause {
  bool reduced_ = false;
  bool distinct_ = false;

 private:
  struct VarsAndAliases {
    std::vector<Variable> vars_;
    std::vector<Alias> aliases_;
  };

  // An empty struct to identify the `SELECT *` case
  struct Asterisk {};

  // The `char` means `SELECT *`.
  std::variant<VarsAndAliases, Asterisk> varsAndAliasesOrAsterisk_;

  // The variables that are visible in the query body. Will be used in the case
  // of `SELECT *` and to check invariants of the `ParsedQuery`.
  std::vector<Variable> visibleVariables_;

 public:
  /// If true, then this `SelectClause` represents `SELECT *`. If false,
  /// variables and aliases were manually selected.
  [[nodiscard]] bool isAsterisk() const;

  // Set the selector to '*', which means that all variables for which
  // `addVariableForAsterisk()` is called are implicitly selected.
  void setAsterisk();

  // Set the (manually) selected variables and aliases. All variables
  // and aliases have to be specified at once via a single call to
  // `setSelected`.
  using VarOrAlias = std::variant<Variable, Alias>;
  void setSelected(std::vector<VarOrAlias> varsOrAliases);

  // Overload of `setSelected` (see above) for the simple case, where only
  // variables and no aliases are selected.
  void setSelected(std::vector<Variable> variables);

  // Add a variable that is visible in the query body.
  void addVisibleVariable(const Variable& variable);

  // Get all the variables that are visible in the query body.
  const std::vector<Variable>& getVisibleVariables();

  /// Get all the selected variables. This includes the variables to which
  /// aliases are bound. For example for `SELECT ?x (?a + ?b AS ?c)`,
  /// `getSelectedVariables` will return `{?x, ?c}`. If `isAsterisk()` is true
  /// (`SELECT *`), then all the variables for which `addVisibleVariable()`
  /// was called, will be returned.
  [[nodiscard]] const std::vector<Variable>& getSelectedVariables() const;

  /// Same as `getSelectedVariables`, but the variables will be returned as
  /// `std::string`.
  [[nodiscard]] std::vector<std::string> getSelectedVariablesAsStrings() const;

  /// Get all the aliases (not the simple variables) that were selected. For
  /// example for `SELECT ?x (?a + ?b AS ?c)`,
  /// `{(?a + ?b AS ?c)}` will be returned.
  [[nodiscard]] const std::vector<Alias>& getAliases() const;
};
}  // namespace parsedQuery
