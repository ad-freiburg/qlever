// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

#include <string>
#include <functional>
#include "./Literal.h"
#include "./BlankNode.h"


class GraphTerm {
  // TODO<Robin> replace int with roper context type
  using Context = size_t;
  std::function<std::string(Context)> _toString;

 public:
  explicit GraphTerm(const Literal& literal) : _toString{[literal]([[maybe_unused]] Context context) { return literal.toString(); }} {}
  explicit GraphTerm(const std::shared_ptr<BlankNode>& node) : _toString{[node](Context context) { return node->toString(context); }} {}


  std::string toString(Context context) const {
    return _toString(context);
  }
};