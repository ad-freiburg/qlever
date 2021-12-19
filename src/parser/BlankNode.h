// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

#include <string>
#include <sstream>
#include <memory>
#include <unordered_map>
#include <utility>

class BlankNode {
  const bool _generated;
  const std::string _label;

 public:
  BlankNode(bool generated, std::string label) : _generated{generated}, _label{std::move(label)} {}

  [[nodiscard]] std::string toString(size_t context) const {
    std::ostringstream stream;
    stream << "_:";
    // generated or user-defined
    stream << (_generated ? 'g' : 'u' );
    stream << context << '_';
    stream << _label;
    return stream.str();
  }
};


class BlankNodeCreator {
  size_t _counter = 0;
  std::unordered_map<std::string, std::shared_ptr<BlankNode>> storedNodes{};

 public:
  std::shared_ptr<BlankNode> newNode() {
    std::ostringstream output;
    output << 'b';
    output << _counter;
    _counter++;
    return std::make_shared<BlankNode>(true, output.str());
  }

  std::shared_ptr<BlankNode> fromLabel(const std::string& label) {
    if (!storedNodes.contains(label)) {
      storedNodes[label] = std::make_shared<BlankNode>(false, label);
    }

    return storedNodes[label];
  }
};
