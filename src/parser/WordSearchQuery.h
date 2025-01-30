// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#pragma once

#include "parser/MagicServiceQuery.h"

namespace parsedQuery {

class WordSearchException : public std::runtime_error {
  using std::runtime_error::runtime_error;
};

struct WordSearchQuery : MagicServiceQuery {
  std::optional<std::string> word_;
  std::optional<Variable> textVar_;
  std::optional<Variable> matchVar_;
  std::optional<Variable> scoreVar_;

  // See MagicServiceQuery
  void addParameter(const SparqlTriple& triple) override;

  TextIndexScanForWordConfiguration toConfig() const;
};

}  // namespace parsedQuery
