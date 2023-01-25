// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2022-     Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)

#include "parser/SelectClause.h"

#include "util/Algorithm.h"
#include "util/OverloadCallOperator.h"

using namespace parsedQuery;

// ____________________________________________________________________
// TODO<joka921, qup42> use a better mechanism to remove duplicates in the
//  visible variables while still keeping the order.
void ClauseBase::addVisibleVariable(const Variable& variable) {
  if (!ad_utility::contains(visibleVariables_, variable)) {
    visibleVariables_.emplace_back(variable);
  }
}

const vector<Variable>& ClauseBase::getVisibleVariables() const {
  return visibleVariables_;
}

// ____________________________________________________________________
[[nodiscard]] bool SelectClause::isAsterisk() const {
  return std::holds_alternative<Asterisk>(varsAndAliasesOrAsterisk_);
}

// ____________________________________________________________________
void SelectClause::setAsterisk() { varsAndAliasesOrAsterisk_ = Asterisk{}; }

// ____________________________________________________________________
void SelectClause::setSelected(std::vector<VarOrAlias> varsOrAliases) {
  VarsAndAliases v;
  auto processVariable = [&v](Variable var) {
    v.vars_.push_back(std::move(var));
  };
  auto processAlias = [&v](Alias alias) {
    v.vars_.emplace_back(alias._target);
    v.aliases_.push_back(std::move(alias));
  };

  for (auto& el : varsOrAliases) {
    std::visit(ad_utility::OverloadCallOperator{processVariable, processAlias},
               std::move(el));
  }
  varsAndAliasesOrAsterisk_ = std::move(v);
}

// ____________________________________________________________________
void SelectClause::setSelected(std::vector<Variable> variables) {
  std::vector<VarOrAlias> v(std::make_move_iterator(variables.begin()),
                            std::make_move_iterator(variables.end()));
  setSelected(v);
}

// ____________________________________________________________________________
[[nodiscard]] const std::vector<Variable>& SelectClause::getSelectedVariables()
    const {
  return isAsterisk()
             ? visibleVariables_
             : std::get<VarsAndAliases>(varsAndAliasesOrAsterisk_).vars_;
}

// ____________________________________________________________________________
[[nodiscard]] std::vector<std::string>
SelectClause::getSelectedVariablesAsStrings() const {
  std::vector<std::string> result;
  const auto& vars = getSelectedVariables();
  result.reserve(vars.size());
  for (const auto& var : vars) {
    result.push_back(var.name());
  }
  return result;
}

// ____________________________________________________________________
[[nodiscard]] const std::vector<Alias>& SelectClause::getAliases() const {
  static const std::vector<Alias> emptyDummy;
  // Aliases are always manually specified
  if (isAsterisk()) {
    return emptyDummy;
  } else {
    return std::get<VarsAndAliases>(varsAndAliasesOrAsterisk_).aliases_;
  }
}
