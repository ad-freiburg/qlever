// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

#include <string>

class Variable {
  const std::string _name;

 public:
  explicit Variable(const std::string& name) : _name{name} {};

  std::string toString() const {
    return _name;
  }
};