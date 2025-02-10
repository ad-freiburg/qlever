// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#include "parser/TextSearchQuery.h"

#include "parser/MagicServiceIriConstants.h"

namespace parsedQuery {

// ____________________________________________________________________________
void TextSearchQuery::addParameter(const SparqlTriple& triple) {
  auto simpleTriple = triple.getSimple();
  TripleComponent subject = simpleTriple.s_;
  TripleComponent predicate = simpleTriple.p_;
  TripleComponent object = simpleTriple.o_;

  auto predString = extractParameterName(predicate, TEXT_SEARCH_IRI);

  auto throwVariableException = [predString, subject, object]() {
    if (!(subject.isVariable() && object.isVariable())) {
      throw TextSearchException(
          absl::StrCat("The predicate <", predString,
                       "> needs a Variable as subject and one as object."));
    }
  };

  if (predString == "text-search") {
    if (!(subject.isVariable() && object.isVariable())) {
      throw TextSearchException(
          "The predicate <text-search> needs a Variable as subject and one as "
          "object.");
    }
    configVarToTextVar_.emplace(object.getVariable(), subject.getVariable());
  } else if (predString == "contains-word") {
    if (!(subject.isVariable() && object.isLiteral())) {
      throw TextSearchException(
          "The predicate <contains-word> needs a Variable as subject and a "
          "Literal as object.");
    }
    std::string_view literal = object.getLiteral().toStringRepresentation();
    configVarToConfigs_[subject.getVariable()].word_ =
        std::string(literal.substr(1, literal.size() - 2));
  } else if (predString == "contains-entity") {
    if (!subject.isVariable()) {
      throw TextSearchException(
          "The predicate <contains-entity> needs a Variable as subject and a "
          "Literal or Variable as object.");
    }
    if (object.isLiteral()) {
      std::string_view literal = object.getLiteral().toStringRepresentation();
      configVarToConfigs_[subject.getVariable()].entity_ =
          std::string(literal.substr(1, literal.size() - 2));
    } else if (object.isVariable()) {
      configVarToConfigs_[subject.getVariable()].entity_ = object.getVariable();
    } else {
      throw TextSearchException(
          "The predicate <contains-entity> needs a Variable as subject and a "
          "Literal or Variable as object.");
    }
  } else if (predString == "bind-match") {
    throwVariableException();
    configVarToConfigs_[subject.getVariable()].varToBindMatch_ =
        object.getVariable();
  } else if (predString == "bind-word-score") {
    throwVariableException();
    configVarToConfigs_[subject.getVariable()].varToBindWordScore_ =
        object.getVariable();
  } else if (predString == "bind-entity-score") {
    throwVariableException();
    configVarToConfigs_[subject.getVariable()].varToBindEntityScore_ =
        object.getVariable();
  }
}

// ____________________________________________________________________________
std::vector<std::variant<TextIndexScanForWordConfiguration,
                         TextIndexScanForEntityConfiguration>>
TextSearchQuery::toConfigs() const {
  auto toWordConf = [](const Variable& textVar,
                       const TextSearchConfig& config) {
    if (!config.word_.has_value()) {
      throw TextSearchException(
          "Missing parameter <containsWord> in text search.");
    }
    return TextIndexScanForWordConfiguration{textVar, config.word_.value(),
                                             config.varToBindMatch_,
                                             config.varToBindWordScore_};
  };

  auto toEntityConf = [](const Variable& textVar,
                         const TextSearchConfig& config) {
    return TextIndexScanForEntityConfiguration{textVar, config.entity_.value(),
                                               config.word_.value(),
                                               config.varToBindEntityScore_};
  };

  std::vector<std::variant<TextIndexScanForWordConfiguration,
                           TextIndexScanForEntityConfiguration>>
      output;
  for (auto kv : configVarToConfigs_) {
    auto it = configVarToTextVar_.find(kv.first);
    if (it == configVarToTextVar_.end()) {
      throw TextSearchException(
          "Missing text Variable in text search service query.");
    }
    output.push_back(toWordConf(it->second, kv.second));
    if (kv.second.entity_.has_value()) {
      output.push_back(toEntityConf(it->second, kv.second));
    }
  }

  return output;
}

}  // namespace parsedQuery
