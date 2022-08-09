// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

#include <string>
#include <utility>

#include "../../engine/ResultTable.h"
#include "../../index/Index.h"
#include "Context.h"

class Variable {
  std::string _name;

 public:
  explicit Variable(std::string name) : _name{std::move(name)} {
    // verify variable name starts with ? or $ and continues without any
    // special characters. This is weaker than the SPARQL grammar,
    // but it is close enough so that it will likely never cause issues.
    AD_CHECK(ctre::match<"[$?]\\w+">(_name));
    // normalise notation for consistency
    _name[0] = '?';
  }

  // Todo<joka921> There are several similar variants of this function across
  // the codebase. Unify them!
  // ___________________________________________________________________________
  [[nodiscard]] std::optional<std::string> evaluate(
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
          return res.indexToOptionalString(id.getLocalVocabIndex())
              .value_or("");
        case Datatype::TextRecordIndex:
          return qecIndex.getTextExcerpt(id.getTextRecordIndex());
      }
      // The switch is exhaustive
      AD_FAIL();
    }
    return std::nullopt;
  }

  // ___________________________________________________________________________
  [[nodiscard]] std::string toSparql() const { return _name; }

  // ___________________________________________________________________________
  [[nodiscard]] const std::string& name() const { return _name; }

  // Needed for consistency with the `Alias` class.
  [[nodiscard]] const std::string& targetVariable() const { return _name; }

  bool operator==(const Variable&) const = default;
};
