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

  WordSearchQuery() = default;
  WordSearchQuery(WordSearchQuery&& other) noexcept = default;
  WordSearchQuery(const WordSearchQuery& other) noexcept = default;
  WordSearchQuery& operator=(const WordSearchQuery& other) = default;
  WordSearchQuery& operator=(WordSearchQuery&& a) noexcept = default;
  ~WordSearchQuery() noexcept override = default;

  // See MagicServiceQuery
  void addParameter(const SparqlTriple& triple) override;

  TextIndexScanForWordConfiguration toTextIndexScanForWordConfiguration() const;
};

}  // namespace parsedQuery
