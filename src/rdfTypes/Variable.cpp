//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "rdfTypes/Variable.h"

#include <unicode/uchar.h>

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
  auto s = absl::StrCat(MATCHINGWORD_VARIABLE_PREFIX, name().substr(1), "_");
  appendEscapedWord(term, s);
  return Variable{std::move(s)};
}

namespace {
// Returns true for a  subset of characters that are valid in variable names.
// This roughly corresponds to `PN_CHARS_BASE` from the SPARQL 1.1 grammar with
// the characters 0-9 also being allowed.
constexpr bool codePointSuitableForVariableName(UChar32 cp) {
  return (cp >= 'A' && cp <= 'Z') || (cp >= 'a' && cp <= 'z') ||
         (cp >= '0' && cp <= '9') || (cp >= 0x00C0 && cp <= 0x00D6) ||
         (cp >= 0x00D8 && cp <= 0x00F6) || (cp >= 0x00F8 && cp <= 0x02FF) ||
         (cp >= 0x0370 && cp <= 0x037D) || (cp >= 0x037F && cp <= 0x1FFF) ||
         (cp >= 0x200C && cp <= 0x200D) || (cp >= 0x2070 && cp <= 0x218F) ||
         (cp >= 0x2C00 && cp <= 0x2FEF) || (cp >= 0x3001 && cp <= 0xD7FF) ||
         (cp >= 0xF900 && cp <= 0xFDCF) || (cp >= 0xFDF0 && cp <= 0xFFFD) ||
         (cp >= 0x10000 && cp <= 0xEFFFF);
}
}  // namespace

// _____________________________________________________________________________
void Variable::appendEscapedWord(std::string_view word, std::string& target) {
  const char* ptr = word.data();
  const char* end = word.data() + word.size();

  while (ptr < end) {
    // Convert all other characters based on their unicode codepoint.
    UChar32 codePoint;
    int64_t i = 0;
    U8_NEXT_OR_FFFD(reinterpret_cast<const uint8_t*>(ptr), i,
                    static_cast<int64_t>(word.size()), codePoint);
    AD_CONTRACT_CHECK(codePoint != 0xFFFD, "Invalid UTF-8");
    if (codePointSuitableForVariableName(codePoint)) {
      target.append(ptr, i);
    } else {
      absl::StrAppend(&target, "_", static_cast<int32_t>(codePoint), "_");
    }
    ptr += i;
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
