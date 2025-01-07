// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#pragma once

#include "parser/MagicServiceQuery.h"

namespace parsedQuery {

class TextSearchException : public std::runtime_error {
  using std::runtime_error::runtime_error;
};

struct TextSearchQuery : MagicServiceQuery {
  std::optional<std::string> word_;
  std::optional<Variable> textVar_;
  std::optional<Variable> matchVar_;
  std::optional<Variable> scoreVar_;

  TextSearchQuery() = default;
  TextSearchQuery(TextSearchQuery&& other) noexcept = default;
  TextSearchQuery(const TextSearchQuery& other) noexcept = default;
  TextSearchQuery& operator=(const TextSearchQuery& other) = default;
  TextSearchQuery& operator=(TextSearchQuery&& a) noexcept = default;
  ~TextSearchQuery() noexcept override = default;

  // See MagicServiceQuery
  void addParameter(const SparqlTriple& triple) override;

  TextIndexScanForWordConfiguration toTextIndexScanForWordConfiguration() const;
};

}  // namespace parsedQuery
