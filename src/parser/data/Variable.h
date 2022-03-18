// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

#include <string>
#include <utility>

#include "../../engine/ResultTable.h"
#include "../../index/Index.h"

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
      if (id.isInteger()) {
        return std::to_string(id.getIntegerUnchecked());
      }
      if (id.isDouble()) {
        return std::to_string(id.getDoubleUnchecked());
      }
      if (id.isVocab()) {
        return qecIndex.idToOptionalString(id.getVocabUnchecked()).value_or("");
      }
      if (id == ID_NO_VALUE) {
        return std::nullopt;
      }
      AD_CHECK(false);
      // TODO<joka921> Remaining valuetypes (date, text, local vocab, etc).
    }
    return std::nullopt;
  }

  // ___________________________________________________________________________
  [[nodiscard]] std::string toSparql() const { return _name; }

  // ___________________________________________________________________________
  [[nodiscard]] const std::string& name() const { return _name; }
};
