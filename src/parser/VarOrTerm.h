// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

#include <string>
#include <functional>
#include "./Variable.h"
#include "./GraphTerm.h"


class VarOrTerm {
  // TODO<Robin> replace int with roper context type
  using Context = size_t;
  std::function<std::string(Context)> _toString;

 public:
  explicit VarOrTerm(const Variable& variable) : _toString{[variable]([[maybe_unused]] Context context) { return variable.toString(); }} {}
  explicit VarOrTerm(const GraphTerm& term) : _toString{[term](Context context) { return term.toString(context); }} {}


  std::string toString(Context context) const {
    return _toString(context);
  }
};