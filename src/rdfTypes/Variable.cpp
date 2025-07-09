//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "rdfTypes/Variable.h"

#include "global/Constants.h"
#include "parser/ParserAndVisitorBase.h"
#include "parser/data/ConstructQueryExportContext.h"

// ___________________________________________________________________________
Variable::Variable(std::string name, bool checkName) : _name{std::move(name)} {
  if (checkName && ad_utility::areExpensiveChecksEnabled) {
    AD_CONTRACT_CHECK(isValidVariableName(_name), [this]() {
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

namespace {
struct IsVariableVisitor {
  using VarContext = SparqlAutomaticParser::VarContext*;
  bool visit(VarContext) { return true; }
  CPP_template(typename T)(
      requires(!ad_utility::SimilarTo<T, VarContext>)) bool visit(T) {
    return false;
  }
};
}  // namespace

// _____________________________________________________________________________
bool Variable::isValidVariableName(std::string_view var) {
  sparqlParserHelpers::ParserAndVisitorBase<IsVariableVisitor> parserAndVisitor{
      std::string{var}};
  try {
    auto [result, remaining] =
        parserAndVisitor.parseTypesafe(&SparqlAutomaticParser::var);
    return result && remaining.empty();
  } catch (...) {
    return false;
  }
}

// Implement the indirection for the evaluation of variables (see the header for
// details).
Variable::EvaluateFuncPtr& Variable::decoupledEvaluateFuncPtr() {
  static constexpr auto dummy =
      [](const Variable&, const ConstructQueryExportContext&,
         PositionInTriple) -> std::optional<std::string> {
    throw std::runtime_error(
        "Variable::decoupledEvaluateFuncPtr() not yet set");
  };
  static EvaluateFuncPtr ptr = dummy;
  return ptr;
}
