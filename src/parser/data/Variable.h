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
      std::ostringstream stream;
      switch (res.getResultType(index)) {
        case ResultTable::ResultType::KB: {
          string entity =
              qecIndex.idToOptionalString(idTable(row, index)).value_or("");
          if (entity.starts_with(VALUE_PREFIX)) {
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
          stream << qecIndex.getTextExcerpt(
              TextRecordIndex::make(idTable(row, index).get()));
          break;
        case ResultTable::ResultType::FLOAT: {
          float f;
          std::memcpy(&f, &idTable(row, index), sizeof(float));
          stream << f;
          break;
        }
        case ResultTable::ResultType::LOCAL_VOCAB: {
          stream << res.indexToOptionalString(
                           LocalVocabIndex::make(idTable(row, index).get()))
                        .value_or("");
          break;
        }
        default:
          AD_THROW(ad_semsearch::Exception::INVALID_PARAMETER_VALUE,
                   "Cannot deduce output type.");
      }
      return std::move(stream).str();
    }
    return std::nullopt;
  }

  // ___________________________________________________________________________
  [[nodiscard]] std::string toSparql() const { return _name; }

  // ___________________________________________________________________________
  [[nodiscard]] const std::string& name() const { return _name; }
};
