//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "ctre/ctre.h"
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
  AD_CONTRACT_CHECK(ctre::match<"[$?]\\w+">(_name));
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
    if (type == nullptr || type == i || type == d) {
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
