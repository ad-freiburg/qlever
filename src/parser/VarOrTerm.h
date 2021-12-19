// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

#include <string>
#include <functional>
#include "./Variable.h"
#include "./GraphTerm.h"
#include "../engine/ResultTable.h"
#include "../index/Index.h"


class VarOrTerm {
  std::function<std::string(size_t, const ResultTable&, const ad_utility::HashMap<string, size_t>&, const Index&)> _toString;

 public:
  explicit VarOrTerm(const Variable& variable) : _toString{[variable](size_t row, const ResultTable& res, const ad_utility::HashMap<string, size_t>& variableColumns, const Index& qecIndex) { return variable.toString(row, res, variableColumns, qecIndex); }} {}
  explicit VarOrTerm(const GraphTerm& term) : _toString{[term](size_t row, [[maybe_unused]] const ResultTable& res, [[maybe_unused]] const ad_utility::HashMap<string, size_t>& variableColumns, [[maybe_unused]] const Index& qecIndex) { return term.toString(row); }} {}


  std::string toString(size_t row, const ResultTable& res, const ad_utility::HashMap<string, size_t>& variableColumns, const Index& qecIndex) const {
    return _toString(row, res, variableColumns, qecIndex);
  }
};