// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#pragma once

#include <string>

#include "engine/TextIndexScanForEntity.h"
#include "engine/TextIndexScanForWord.h"
#include "parser/MagicServiceQuery.h"
#include "util/HashMap.h"

struct TextSearchConfig {
  std::optional<bool> isWordSearch_;
  std::optional<Variable> textVar_;
  std::optional<std::string> word_;
  std::optional<Variable> varToBindMatch_;
  std::optional<Variable> varToBindScore_;
  std::optional<std::variant<Variable, std::string>> entity_;
};

namespace parsedQuery {

class TextSearchException : public std::runtime_error {
  using std::runtime_error::runtime_error;
};

struct TextSearchQuery : MagicServiceQuery {
  ad_utility::HashMap<Variable, TextSearchConfig> configVarToConfigs_;

  // See MagicServiceQuery
  void addParameter(const SparqlTriple& triple) override;

  std::vector<std::variant<TextIndexScanForWordConfiguration,
                           TextIndexScanForEntityConfiguration>>
  toConfigs(QueryExecutionContext* qec) const;

  // Helper functions for addParameter

  // Checks if subject is a variable. If not throws exception.
  void throwSubjectVariableException(std::string predString,
                                     const TripleComponent& subject);
  // Checks if object and subject are variables. If not throws exception.
  void throwSubjectAndObjectVariableException(std::string predString,
                                              const TripleComponent& subject,
                                              const TripleComponent& object);
  // Checks if query already encountered <contains-word> or <contains-entity>
  // before this. If yes throws exception.
  void throwContainsWordOrEntity(const TripleComponent& subject);

  void predStringTextSearch(const Variable& subjectVar,
                            const Variable& objectVar);

  void predStringContainsWord(const Variable& subjectVar,
                              const TripleComponent::Literal& objectLiteral);

  void predStringContainsEntity(const TripleComponent& subject,
                                const TripleComponent& object);

  void predStringBindMatch(const Variable& subjectVar,
                           const Variable& objectVar);

  void predStringBindScore(const Variable& subjectVar,
                           const Variable& objectVar);
};

}  // namespace parsedQuery
