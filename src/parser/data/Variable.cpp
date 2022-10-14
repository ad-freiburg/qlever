//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "parser/data/Variable.h"

#include "ctre/ctre.h"
#include "index/Index.h"

// ___________________________________________________________________________
Variable::Variable(std::string name) : _name{std::move(name)} {
  // verify variable name starts with ? or $ and continues without any
  // special characters. This is weaker than the SPARQL grammar,
  // but it is close enough so that it will likely never cause issues.
  AD_CHECK(ctre::match<"[$?]\\w+">(_name));
  // normalise notation for consistency
  _name[0] = '?';
}

// ___________________________________________________________________________
[[nodiscard]] std::optional<std::string> Variable::evaluate(
    const Context& context, [[maybe_unused]] ContextRole role) const {
  size_t row = context._row;
  const ResultTable& res = context._res;
  const ad_utility::HashMap<string, size_t>& variableColumns =
      context._variableColumns;
  const Index& qecIndex = context._qecIndex;
  const auto& idTable = res._idTable;
  if (variableColumns.contains(_name)) {
    size_t index = variableColumns.at(_name);
    auto id = idTable(row, index);
    switch (id.getDatatype()) {
      case Datatype::Undefined:
        return std::nullopt;
      case Datatype::Double: {
        std::ostringstream stream;
        stream << id.getDouble();
        return std::move(stream).str();
      }
      case Datatype::Int: {
        std::ostringstream stream;
        stream << id.getInt();
        return std::move(stream).str();
      }
      case Datatype::VocabIndex:
        return qecIndex.idToOptionalString(id).value_or("");
      case Datatype::LocalVocabIndex:
        return res.indexToOptionalString(id.getLocalVocabIndex()).value_or("");
      case Datatype::TextRecordIndex:
        return qecIndex.getTextExcerpt(id.getTextRecordIndex());
    }
    // The switch is exhaustive
    AD_FAIL();
  }
  return std::nullopt;
}
