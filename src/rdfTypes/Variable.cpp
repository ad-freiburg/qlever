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
// Returns true for a subset of characters that are valid in variable names.
// This roughly corresponds to `PN_CHARS_BASE` from the SPARQL 1.1 grammar with
// the characters 0-9 also being allowed. Note that this deliberately does not
// contain the (valid) character `_`, as we use that character for escaping, and
// it thus has to be escaped itself.
constexpr bool codePointSuitableForVariableName(UChar32 cp) {
  using A = std::array<UChar32, 2>;
  constexpr std::array validRanges{
      A{'A', 'Z'},       A{'a', 'z'},       A{'0', '9'},
      A{0x00C0, 0x00D6}, A{0x00D8, 0x00F6}, A{0x00F8, 0x02FF},
      A{0x0370, 0x037D}, A{0x037F, 0x1FFF}, A{0x200C, 0x200D},
      A{0x2070, 0x218F}, A{0x2C00, 0x2FEF}, A{0x3001, 0xD7FF},
      A{0xF900, 0xFDCF}, A{0xFDF0, 0xFFFD}, A{0x10000, 0xEFFFF}};
  return ql::ranges::any_of(validRanges, [cp](const auto& arr) {
    return cp >= arr[0] && cp <= arr[1];
  });
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
    U8_NEXT(reinterpret_cast<const uint8_t*>(ptr), i,
            static_cast<int64_t>(word.size()), codePoint);
    AD_CONTRACT_CHECK(codePoint != U_SENTINEL, "Invalid UTF-8");
    if (codePointSuitableForVariableName(codePoint)) {
      target.append(ptr, i);
    } else {
      absl::StrAppend(&target, "_", codePoint, "_");
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
