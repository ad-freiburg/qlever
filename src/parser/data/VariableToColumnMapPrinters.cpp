//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <ctre-unicode.hpp>

#include "engine/ExportQueryExecutionTrees.h"
#include "global/Constants.h"
#include "index/Index.h"
#include "parser/data/ConstructQueryExportContext.h"
#include "parser/data/Variable.h"

// ___________________________________________________________________________
Variable::Variable(std::string name) : _name{std::move(name)} {
  // verify variable name starts with ? or $ and continues without any
  // special characters. This is weaker than the SPARQL grammar,
  // but it is close enough so that it will likely never cause issues.
  AD_CONTRACT_CHECK(ctre::match<"[$?][\\w]+">(_name));
  // normalize notation for consistency
  _name[0] = '?';
}

// ___________________________________________________________________________
[[nodiscard]] std::optional<std::string> Variable::evaluate(
    const ConstructQueryExportContext& context,
    [[maybe_unused]] PositionInTriple positionInTriple) const {
  // TODO<joka921> This whole function should be much further up in the
  // Call stack. Most notably the check which columns belongs to this variable
  // should be much further up in the call stack.
  size_t row = context._row;
  const ResultTable& res = context._res;
  const auto& variableColumns = context._variableColumns;
  const Index& qecIndex = context._qecIndex;
  const auto& idTable = res.idTable();
  if (variableColumns.contains(*this)) {
    size_t index = variableColumns.at(*this).columnIndex_;
    auto id = idTable(row, index);
    auto optionalStringAndType = ExportQueryExecutionTrees::idToStringAndType(
        qecIndex, id, res.localVocab());
    if (!optionalStringAndType.has_value()) {
      return std::nullopt;
    }
    auto& [literal, type] = optionalStringAndType.value();
    const char* i = XSD_INT_TYPE;
    const char* d = XSD_DECIMAL_TYPE;
    const char* b = XSD_BOOLEAN_TYPE;
    if (type == nullptr || type == i || type == d || type == b) {
      return std::move(literal);
    } else {
      return absl::StrCat("\"", literal, "\"^^<", type, ">");
    }
  }
  return std::nullopt;
}

// _____________________________________________________________________________
Variable Variable::getTextScoreVariable() const {
  return Variable{absl::StrCat(TEXTSCORE_VARIABLE_PREFIX, name().substr(1))};
}

// _____________________________________________________________________________
Variable Variable::getScoreVariable(
    const std::variant<Variable, std::string>& varOrEntity) const {
  std::string_view type;
  std::string entity;
  if (std::holds_alternative<Variable>(varOrEntity)) {
    type = "_var_";
    entity = std::get<Variable>(varOrEntity).name().substr(1);
  } else {
    type = "_fixedEntity_";
    // Converts input string to unambiguous result string not containing any
    // special characters. "_" is used as an escaping character.
    for (char c : std::get<std::string>(varOrEntity)) {
      if (isalpha(static_cast<unsigned char>(c))) {
        entity += c;
      } else {
        absl::StrAppend(&entity, "_", std::to_string(c), "_");
      }
    }
  }
  return Variable{
      absl::StrCat(SCORE_VARIABLE_PREFIX, name().substr(1), type, entity)};
}

// _____________________________________________________________________________
Variable Variable::getMatchingWordVariable(std::string_view term) const {
  return Variable{
      absl::StrCat(MATCHINGWORD_VARIABLE_PREFIX, name().substr(1), "_", term)};
}
