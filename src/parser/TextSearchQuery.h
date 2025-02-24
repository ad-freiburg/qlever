// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#pragma once

#include <string>

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

using FixedEntity = std::pair<std::string, VocabIndex>;

struct VarOrFixedEntity {
  std::variant<Variable, FixedEntity> entity_;

  static std::variant<Variable, FixedEntity> makeEntityVariant(
      const QueryExecutionContext* qec,
      std::variant<Variable, std::string> entity);

  VarOrFixedEntity(const QueryExecutionContext* qec,
                   std::variant<Variable, std::string> entity)
      : entity_(makeEntityVariant(qec, std::move(entity))) {}

  bool hasFixedEntity() const {
    return std::holds_alternative<FixedEntity>(entity_);
  }
};

struct TextIndexScanForEntityConfiguration {
  Variable varToBindText_;
  std::variant<Variable, std::string> entity_;
  std::string word_;
  std::optional<Variable> varToBindScore_ = std::nullopt;
  std::optional<VariableToColumnMap> variableColumns_ = std::nullopt;
  std::optional<VarOrFixedEntity> varOrFixed_ = std::nullopt;

  bool operator==(const TextIndexScanForEntityConfiguration& other) const {
    return varToBindText_ == other.varToBindText_ && entity_ == other.entity_ &&
           word_ == other.word_ && varToBindScore_ == other.varToBindScore_;
  }
};

struct TextIndexScanForWordConfiguration {
  Variable varToBindText_;
  string word_;
  std::optional<Variable> varToBindMatch_ = std::nullopt;
  std::optional<Variable> varToBindScore_ = std::nullopt;
  bool isPrefix_ = false;
  std::optional<VariableToColumnMap> variableColumns_ = std::nullopt;

  bool operator==(const TextIndexScanForWordConfiguration& other) const {
    return varToBindText_ == other.varToBindText_ && word_ == other.word_ &&
           varToBindMatch_ == other.varToBindMatch_ &&
           varToBindScore_ == other.varToBindScore_ &&
           isPrefix_ == other.isPrefix_;
  }
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
  toConfigs(const QueryExecutionContext* qec) const;

  // Helper functions for addParameter

  // Checks if subject is a variable. If not throws exception.
  static void throwSubjectVariableException(std::string_view predString,
                                            const TripleComponent& subject);
  // Checks if object and subject are variables. If not throws exception.
  static void throwSubjectAndObjectVariableException(
      std::string_view predString, const TripleComponent& subject,
      const TripleComponent& object);
  // Checks if query already encountered <contains-word> or <contains-entity>
  // before this. If yes throws exception.
  void throwContainsWordOrEntity(const TripleComponent& subject);

  // Checks if object is literal. If not throws exception.
  static void throwObjectLiteralException(std::string_view predString,
                                          const TripleComponent& object);

  void predStringTextSearch(const Variable& subjectVar,
                            const Variable& objectVar);

  void predStringContainsWord(const Variable& subjectVar,
                              const TripleComponent::Literal& objectLiteral);

  void predStringContainsEntity(const Variable& subjectVar,
                                const TripleComponent& object);

  void predStringBindMatch(const Variable& subjectVar,
                           const Variable& objectVar);

  void predStringBindScore(const Variable& subjectVar,
                           const Variable& objectVar);
};

}  // namespace parsedQuery
