// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#ifndef QLEVER_SRC_PARSER_TEXTSEARCHQUERY_H
#define QLEVER_SRC_PARSER_TEXTSEARCHQUERY_H

#include <string>

#include "parser/MagicServiceQuery.h"
#include "util/HashMap.h"

/**
 * @brief This struct holds all information given by a single configuration in
 *        the magic service query for text search. It holds information for both
 *        word and entity search and later is converted to either
 *        WordSearchConfig or EntitySearchConfig depending on isWordSearch.
 * @detail All fields are optional since in the addParam step for magic service
 *         queries all params are given one by one and in no particular order
 *         and therefore later it has to be checked if a config is valid or not.
 *         This is part of the toConfigs step of TextSearchQuery. All predicates
 *         named in the fields are predicates in context of the magic service
 *         query for text search.
 *
 *         Description of struct variables:
 *         - std::optional<bool> isWordSearch_: This has a value as soon as
 *        either the predicate <contains-word> or <contains-entity> is
 *        encountered with the config and is respectively true or false.
 *        - std::optional<Variable> textVar_: This has a value if set through
 *        the predicate <text-search>. This textVar_ is later passed to the
 *        constructed word or entity search.
 *        - std::optional<std::string> word_: This is set directly with the
 *        predicate <contains-word>.
 *        - std::optional<Variable> matchVar_: This is set with the predicate
 *        <bind-match> and used to specify the variable for the prefix match
 *        of a word search.
 *        - std::optional<Variable> scoreVar_: This is set with the predicate
 *        <bind-score> and used to specify the variable for the score of either
 *        the entity or word search.
 *        - std::optional<std::variant<Variable, std::string>> entity_: This
 *        is the specified entity for the entity search. Can be Variable or
 *        string since IRIs and literals are also searchable.
 *
 *        Fields that have to have a value for a valid word search are:
 *        - isWordSearch_ = true
 *        - textVar_
 *        - word_
 *
 *        Fields that have to have a value for a valid entity search are:
 *        - isWordSearch_ = false
 *        - textVar_
 *        - entity_
 */
struct TextSearchConfig {
  std::optional<bool> isWordSearch_;
  std::optional<Variable> textVar_;
  std::optional<std::string> word_;
  std::optional<Variable> matchVar_;
  std::optional<Variable> scoreVar_;
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

  friend bool operator==(const VarOrFixedEntity&,
                         const VarOrFixedEntity&) = default;
};

/**
 * @brief This struct holds all information for a TextIndexScanForEntity
 *        operation.
 * @details This configuration can be used in the construction of a
 *          TextIndexScanForEntity but also manages the variables
 *          TextIndexScanForEntity later uses. This means each
 *          TextIndexScanForEntity has a configuration which is either given and
 *          extended or created in the constructor.
 *          Struct variable information:
 *          - varToBindText_: See details of TextSearchConfig
 *          - entity_: See details of TextSearchConfig
 *          - word_: See details of TextSearchConfig
 *          - scoreVar_: See details of TextSearchConfig. If not given will be
 *                        constructed during the creation of
 *                       TextIndexScanForEntity.
 *          - variableColumns_: Specifies the VariableToColumnMap. Will be
 *                              overridden in the constructor of
 *                              TextIndexScanForEntity through calling of
 *                              setVariableToColumnMap()
 *          - varOrFixed_: Better typed variable for the entity. Will be
 *                         overridden in the constructor of
 *                         TextIndexScanForEntity using the value of entity_.
 * @warning The operator == is implemented in a way to only check equivalence
 *          of certain fields important to testing.
 */
struct TextIndexScanForEntityConfiguration {
  Variable varToBindText_;
  std::variant<Variable, std::string> entity_;
  std::string word_;
  std::optional<Variable> scoreVar_ = std::nullopt;
  std::optional<VariableToColumnMap> variableColumns_ = std::nullopt;
  std::optional<VarOrFixedEntity> varOrFixed_ = std::nullopt;

  bool operator==(const TextIndexScanForEntityConfiguration& other) const {
    return varToBindText_ == other.varToBindText_ && word_ == other.word_ &&
           scoreVar_ == other.scoreVar_ && varOrFixed_ == other.varOrFixed_;
  }

  friend std::ostream& operator<<(
      std::ostream& os, const TextIndexScanForEntityConfiguration& conf);
};

/**
 * @brief This struct holds all information for a TextIndexScanForWord
 *        operation.
 * @details This configuration can be used in the construction of a
 *          TextIndexScanForWord but also manages the variables
 *          TextIndexScanForWord later uses. This means each
 *          TextIndexScanForWord has a configuration which is either given and
 *          extended or created in the constructor.
 *          Struct variable information:
 *          - varToBindText_: See details of TextSearchConfig
 *          - word_: See details of TextSearchConfig
 *          - matchVar_: See details of TextSearchConfig. If not given will be
 *                       constructed during the creation of
 *                       TextIndexScanForWord.
 *          - scoreVar_: See details of TextSearchConfig. If not given will be
 *                        constructed during the creation of
 *                       TextIndexScanForWord.
 *          - isPrefix_: Will be overridden during the creation of
 *                       TextIndexScanForWord using the word_ and checking if
 *                       it ends with a '*'.
 *          - variableColumns_: Specifies the VariableToColumnMap. Will be
 *                              overridden in the constructor of
 *                              TextIndexScanForWord through calling of
 *                              setVariableToColumnMap()
 * @warning The operator == is implemented in a way to only check equivalence
 *          of certain fields important to testing.
 */
struct TextIndexScanForWordConfiguration {
  Variable varToBindText_;
  string word_;
  std::optional<Variable> matchVar_ = std::nullopt;
  std::optional<Variable> scoreVar_ = std::nullopt;
  bool isPrefix_ = false;
  std::optional<VariableToColumnMap> variableColumns_ = std::nullopt;

  bool operator==(const TextIndexScanForWordConfiguration& other) const {
    return varToBindText_ == other.varToBindText_ && word_ == other.word_ &&
           matchVar_ == other.matchVar_ && scoreVar_ == other.scoreVar_ &&
           isPrefix_ == other.isPrefix_;
  }

  friend std::ostream& operator<<(
      std::ostream& os, const TextIndexScanForWordConfiguration& conf);
};

namespace parsedQuery {

class TextSearchException : public std::runtime_error {
  using std::runtime_error::runtime_error;
};

/**
 * @brief Class to manage the magic service query for text search. Can hold
 *        multiple entity and word searches at once.
 */

struct TextSearchQuery : MagicServiceQuery {
  ad_utility::HashMap<Variable, TextSearchConfig> configVarToConfigs_;

  // See MagicServiceQuery for base implementation.
  // For details of which triples make sense look at details of
  // TextSearchConfig.
  void addParameter(const SparqlTriple& triple) override;

  // Convert each config of configVarToConfigs_ to either word search config
  // or entity search config. Check all query mistakes that can only be checked
  // once the complete query is parsed.
  std::vector<std::variant<TextIndexScanForWordConfiguration,
                           TextIndexScanForEntityConfiguration>>
  toConfigs(const QueryExecutionContext* qec) const;

  // Helper functions for addParameter

  // Checks if subject is a variable. If not throws exception.
  static void checkSubjectIsVariable(std::string_view predString,
                                     const TripleComponent& subject);
  // Checks if object and subject are variables. If not throws exception.
  static void checkSubjectAndObjectAreVariables(std::string_view predString,
                                                const TripleComponent& subject,
                                                const TripleComponent& object);
  // Checks if query already encountered <contains-word> or <contains-entity>
  // before this. If yes throws exception.
  void checkOneContainsWordOrEntity(const TripleComponent& subject);

  // Checks if object is literal. If not throws exception.
  static void checkObjectIsLiteral(std::string_view predString,
                                   const TripleComponent& object);
  // Sets pair of configVar, textVar in configVarToConfigs_.
  // Throws exception if textVar was previously set for this key.
  void predStringTextSearch(const Variable& textVar, const Variable& configVar);

  // Sets isWordSearch_ for config to true and sets the word_ to the content
  // of objectLiteral.
  // Throws exception if objectLiteral content is empty.
  void predStringContainsWord(const Variable& configVar,
                              const TripleComponent::Literal& objectLiteral);

  // Sets isWordSearch_ for config to false and sets the entity_ to the
  // variable, IRI or literal given by object.
  // Throws exception if object isn't of one of these three mentioned types.
  void predStringContainsEntity(const Variable& configVar,
                                const TripleComponent& object);

  // Sets matchVar_ for config to objectVar.
  // Throws exception if matchVar_ was previously set for this key.
  void predStringBindMatch(const Variable& configVar,
                           const Variable& objectVar);

  // Sets scoreVar_ for config to objectVar.
  // Throws exception if scoreVar_ was previously set for this key.
  void predStringBindScore(const Variable& configVar,
                           const Variable& objectVar);
};

}  // namespace parsedQuery

#endif  // QLEVER_SRC_PARSER_TEXTSEARCHQUERY_H
