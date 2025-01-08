// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#pragma once

#include "parser/MagicServiceQuery.h"

namespace parsedQuery {

class EntitySearchException : public std::runtime_error {
  using std::runtime_error::runtime_error;
};

struct EntitySearchQuery : MagicServiceQuery {
  std::optional<std::string> word_;
  std::optional<Variable> textVar_;
  std::optional<std::string> fixedEntity_;
  std::optional<Variable> entityVar_;
  std::optional<Variable> scoreVar_;

  EntitySearchQuery() = default;
  EntitySearchQuery(EntitySearchQuery&& other) noexcept = default;
  EntitySearchQuery(const EntitySearchQuery& other) noexcept = default;
  EntitySearchQuery& operator=(const EntitySearchQuery& other) = default;
  EntitySearchQuery& operator=(EntitySearchQuery&& a) noexcept = default;
  ~EntitySearchQuery() noexcept override = default;

  // See MagicServiceQuery
  void addParameter(const SparqlTriple& triple) override;

  TextIndexScanForEntityConfiguration toTextIndexScanForEntityConfiguration()
      const;
};

}  // namespace parsedQuery
