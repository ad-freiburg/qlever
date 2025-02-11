// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#include "parser/TextSearchQuery.h"

#include <absl/strings/str_split.h>

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
    if (configVarToConfigs_[object.getVariable()].textVar_.has_value()) {
      throw TextSearchException(
          "Each search should be linked to only on text Variable.");
    }
    configVarToConfigs_[object.getVariable()].textVar_ = subject.getVariable();
  } else if (predString == "contains-word") {
    if (!(subject.isVariable() && object.isLiteral())) {
      throw TextSearchException(
          "The predicate <contains-word> needs a Variable as subject and a "
          "Literal as object.");
    }
    if (configVarToConfigs_[subject.getVariable()].isWordSearch_.has_value()) {
      if (configVarToConfigs_[subject.getVariable()].isWordSearch_.value()) {
        throw TextSearchException(
            "One search should only contain one <contains-word>.");
      } else {
        throw TextSearchException(
            "One search should contain either <contains-word> or "
            "<contains-entity>.");
      }
    }
    configVarToConfigs_[subject.getVariable()].isWordSearch_ = true;
    std::string_view literal = object.getLiteral().toStringRepresentation();
    literal = std::string(literal.substr(1, literal.size() - 2));
    if (literal.empty()) {
      throw TextSearchException(
          "The predicate <contains-word> shouldn't have an empty literal as "
          "object.");
    }
    configVarToConfigs_[subject.getVariable()].word_ = literal;
  } else if (predString == "contains-entity") {
    if (!subject.isVariable()) {
      throw TextSearchException(
          "The predicate <contains-entity> needs a Variable as subject and a "
          "Literal or Variable as object.");
    }
    if (configVarToConfigs_[subject.getVariable()].isWordSearch_.has_value()) {
      if (configVarToConfigs_[subject.getVariable()].isWordSearch_.value()) {
        throw TextSearchException(
            "One search should contain either <contains-word> or "
            "<contains-entity>.");
      } else {
        throw TextSearchException(
            "One search should only contain one <contains-entity>.");
      }
    }
    configVarToConfigs_[subject.getVariable()].isWordSearch_ = false;
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
    if (configVarToConfigs_[subject.getVariable()]
            .varToBindMatch_.has_value()) {
      throw TextSearchException(
          "A text search should only contain one <bind-match>.");
    }
    configVarToConfigs_[subject.getVariable()].varToBindMatch_ =
        object.getVariable();
  } else if (predString == "bind-score") {
    throwVariableException();
    if (configVarToConfigs_[subject.getVariable()]
            .varToBindScore_.has_value()) {
      throw TextSearchException(
          "A text search should only contain one <bind-score>.");
    }
    configVarToConfigs_[subject.getVariable()].varToBindScore_ =
        object.getVariable();
  }
}

// ____________________________________________________________________________
std::vector<std::variant<TextIndexScanForWordConfiguration,
                         TextIndexScanForEntityConfiguration>>
TextSearchQuery::toConfigs(QueryExecutionContext* qec) const {
  std::vector<std::variant<TextIndexScanForWordConfiguration,
                           TextIndexScanForEntityConfiguration>>
      output;
  // First pass to get all word searches
  ad_utility::HashMap<Variable, vector<string>> potentialTermsForCvar;
  for (const auto& [var, conf] : configVarToConfigs_) {
    if (!conf.isWordSearch_.has_value()) {
      throw TextSearchException(
          "Text search service needs either <contains-word> or "
          "<contains-entity>.");
    }
    if (!conf.textVar_.has_value()) {
      throw TextSearchException(
          "Text search service needs a text variable to search.");
    }
    if (conf.isWordSearch_.value()) {
      auto it = potentialTermsForCvar.find(conf.textVar_.value());
      if (it == potentialTermsForCvar.end()) {
        potentialTermsForCvar.insert(
            {conf.textVar_.value(), {conf.word_.value()}});
      } else {
        it->second.push_back(conf.word_.value());
      }
    }
  }
  // Second pass to create all configs
  for (const auto& [var, conf] : configVarToConfigs_) {
    if (conf.isWordSearch_.value()) {
      output.push_back(TextIndexScanForWordConfiguration{
          conf.textVar_.value(), conf.word_.value(), conf.varToBindMatch_,
          conf.varToBindScore_});
    } else {
      auto it = potentialTermsForCvar.find(conf.textVar_.value());
      if (it == potentialTermsForCvar.end()) {
        throw TextSearchException(
            "Entity search has to happen on a text variable that is also "
            "contained in a word search.");
      }
      output.push_back(TextIndexScanForEntityConfiguration{
          conf.textVar_.value(), conf.entity_.value(),
          it->second[qec->getIndex().getIndexOfBestSuitedElTerm(it->second)],
          conf.varToBindScore_});
    }
  }
  return output;
}

}  // namespace parsedQuery
