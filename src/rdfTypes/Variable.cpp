//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "rdfTypes/Variable.h"

#include "global/Constants.h"
#include "index/Index.h"
#include "parser/data/ConstructQueryExportContext.h"

// ___________________________________________________________________________
Variable::Variable(std::string name, bool checkName) : _name{std::move(name)} {
  if (checkName && ad_utility::areExpensiveChecksEnabled) {
    AD_CONTRACT_CHECK(isValidVariableName()(_name), [this]() {
      return absl::StrCat("\"", _name, "\" is not a valid SPARQL variable");
    });
  }
  // normalize notation for consistency
  _name[0] = '?';
}

// ___________________________________________________________________________
[[nodiscard]] std::optional<std::string> Variable::evaluate(
    const ConstructQueryExportContext& context,
    [[maybe_unused]] PositionInTriple positionInTriple) const {
  return decoupledEvaluateFuncPtr()(*this, context, positionInTriple);
}

// _____________________________________________________________________________
Variable Variable::getEntityScoreVariable(
    const std::variant<Variable, std::string>& varOrEntity) const {
  std::string_view type;
  std::string entity;
  if (std::holds_alternative<Variable>(varOrEntity)) {
    type = "_var_";
    entity = std::get<Variable>(varOrEntity).name().substr(1);
  } else {
    type = "_fixedEntity_";
    appendEscapedWord(std::get<std::string>(varOrEntity), entity);
  }
  return Variable{
      absl::StrCat(SCORE_VARIABLE_PREFIX, name().substr(1), type, entity)};
}

// _____________________________________________________________________________
Variable Variable::getWordScoreVariable(std::string_view word,
                                        bool isPrefix) const {
  std::string_view type;
  std::string convertedWord;
  if (isPrefix) {
    word.remove_suffix(1);
    type = "prefix_";
  } else {
    type = "word_";
  }
  convertedWord = "_";
  appendEscapedWord(word, convertedWord);
  return Variable{absl::StrCat(SCORE_VARIABLE_PREFIX, type, name().substr(1),
                               convertedWord)};
}

// _____________________________________________________________________________
Variable Variable::getMatchingWordVariable(std::string_view term) const {
  return Variable{
      absl::StrCat(MATCHINGWORD_VARIABLE_PREFIX, name().substr(1), "_", term)};
}

// The following three functions implement the indirection for the evaluation of
// variables and for the checking of valid variable names (see the header for
// details).

// _____________________________________________________________________________
auto Variable::isValidVariableName() -> ValidNameFuncPtr& {
  static ValidNameFuncPtr ptr = &Variable::isValidVariableNameDummy;
  return ptr;
}

// _____________________________________________________________________________
[[nodiscard]] static std::optional<std::string> evaluateVariableDummy(
    const Variable&, const ConstructQueryExportContext&, PositionInTriple) {
  return std::nullopt;
}

// _____________________________________________________________________________
auto Variable::decoupledEvaluateFuncPtr() -> EvaluateFuncPtr& {
  static EvaluateFuncPtr ptr = &evaluateVariableDummy;
  return ptr;
}

// _____________________________________________________________________________
void Variable::appendEscapedWord(std::string_view word,
                                 std::string& target) const {
  for (char c : word) {
    if (isalpha(static_cast<unsigned char>(c))) {
      target += c;
    } else {
      absl::StrAppend(&target, "_", std::to_string(c), "_");
    }
  }
}
