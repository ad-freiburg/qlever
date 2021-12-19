// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

#include <string>
#include <functional>
#include "./Literal.h"
#include "./BlankNode.h"


class GraphTerm {
  std::function<std::string(size_t)> _toString;

 public:
  explicit GraphTerm(const Literal& literal) : _toString{[literal]([[maybe_unused]] size_t col) { return literal.toString(); }} {}
  explicit GraphTerm(const std::shared_ptr<BlankNode>& node) : _toString{[node](size_t col) { return node->toString(col); }} {}


  [[nodiscard]] std::string toString(size_t col) const {
    return _toString(col);
  }
};
