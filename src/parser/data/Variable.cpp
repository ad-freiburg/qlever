//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "parser/data/Variable.h"

#include "ctre/ctre.h"
#include "index/Index.h"
#include "parser/data/Context.h"

// ___________________________________________________________________________
Variable::Variable(std::string name) : _name{std::move(name)} {
  // verify variable name starts with ? or $ and continues without any
  // special characters. This is weaker than the SPARQL grammar,
  // but it is close enough so that it will likely never cause issues.
  AD_CONTRACT_CHECK(ctre::match<"[$?]\\w+">(_name));
  // normalise notation for consistency
  _name[0] = '?';
}

// ___________________________________________________________________________
[[nodiscard]] std::optional<std::string> Variable::evaluate(
    const Context& context, [[maybe_unused]] ContextRole role) const {
  size_t row = context._row;
  const ResultTable& res = context._res;
  const auto& variableColumns = context._variableColumns;
  const Index& qecIndex = context._qecIndex;
  const auto& idTable = res._idTable;
  if (variableColumns.contains(*this)) {
    size_t index = variableColumns.at(*this);
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
        return res._localVocab->getWord(id.getLocalVocabIndex());
      case Datatype::TextRecordIndex:
        return qecIndex.getTextExcerpt(id.getTextRecordIndex());
    }
    // The switch is exhaustive
    AD_FAIL();
  }
  return std::nullopt;
}

// _____________________________________________________________________________
Variable Variable::getTextScoreVariable() const {
  return Variable{absl::StrCat(TEXTSCORE_VARIABLE_PREFIX, name().substr(1))};
}
