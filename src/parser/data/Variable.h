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
  explicit Variable(std::string name) : _name{std::move(name)} {};

  [[nodiscard]] std::optional<std::string> toString(
      const Context& context, [[maybe_unused]] ContextRole role) const {
    size_t row = context._row;
    const ResultTable& res = context._res;
    const ad_utility::HashMap<string, size_t>& variableColumns =
        context._variableColumns;
    const Index& qecIndex = context._qecIndex;
    const auto& idTable = res._idTable;
    if (variableColumns.contains(_name)) {
      size_t index = variableColumns.at(_name);
      std::ostringstream stream;
      switch (res.getResultType(index)) {
        case ResultTable::ResultType::KB: {
          string entity =
              qecIndex.idToOptionalString(idTable(row, index)).value_or("");
          if (ad_utility::startsWith(entity, VALUE_PREFIX)) {
            stream << ad_utility::convertIndexWordToValueLiteral(entity);
          } else {
            stream << entity;
          }
          break;
        }
        case ResultTable::ResultType::VERBATIM:
          stream << idTable(row, index);
          break;
        case ResultTable::ResultType::TEXT:
          stream << qecIndex.getTextExcerpt(idTable(row, index));
          break;
        case ResultTable::ResultType::FLOAT: {
          float f;
          std::memcpy(&f, &idTable(row, index), sizeof(float));
          stream << f;
          break;
        }
        case ResultTable::ResultType::LOCAL_VOCAB: {
          stream << res.idToOptionalString(idTable(row, index)).value_or("");
          break;
        }
        default:
          AD_THROW(ad_semsearch::Exception::INVALID_PARAMETER_VALUE,
                   "Cannot deduce output type.");
      }
      return stream.str();
    }
    return std::nullopt;
  }

  [[nodiscard]] std::string toString() const {
    return _name;
  }
};
