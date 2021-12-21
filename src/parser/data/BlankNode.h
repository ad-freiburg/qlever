// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>

class BlankNode {
  const bool _generated;
  const std::string _label;

 public:
  BlankNode(bool generated, std::string label)
      : _generated{generated}, _label{std::move(label)} {}

  [[nodiscard]] std::string toString(size_t context) const {
    std::ostringstream stream;
    stream << "_:";
    // generated or user-defined
    stream << (_generated ? 'g' : 'u');
    stream << context << '_';
    stream << _label;
    return stream.str();
  }
};