// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#include "parser/TextSearchQuery.h"

#include <absl/strings/str_split.h>

#include "parser/MagicServiceIriConstants.h"

namespace parsedQuery {

// ____________________________________________________________________________
void TextSearchQuery::throwSubjectVariableException(
    std::string predString, const TripleComponent& subject) {
  if (!subject.isVariable()) {
    throw TextSearchException(
        absl::StrCat("The predicate <", std::move(predString),
                     "> needs a Variable as Variable as subject."));
  }
}

// ____________________________________________________________________________
void TextSearchQuery::throwSubjectAndObjectVariableException(
    std::string predString, const TripleComponent& subject,
    const TripleComponent& object) {
  if (!(subject.isVariable() && object.isVariable())) {
    throw TextSearchException(
        absl::StrCat("The predicate <", std::move(predString),
                     "> needs a Variable as subject and one as object."));
  }
}

// ____________________________________________________________________________
void TextSearchQuery::throwContainsWordOrEntity(
    const TripleComponent& subject) {
  if (configVarToConfigs_[subject.getVariable()].isWordSearch_.has_value()) {
    throw TextSearchException(
        "Each search should have exactly one occurrence of either "
        "<contains-word> or <contains-entity>");
  }
}

// ____________________________________________________________________________
void TextSearchQuery::predStringTextSearch(const Variable& subjectVar,
                                           const Variable& objectVar) {
  if (configVarToConfigs_[objectVar].textVar_.has_value()) {
    throw TextSearchException(
        absl::StrCat("Each search should only be linked to a single text "
                     "Variable. The text variable was: ",
                     configVarToConfigs_[objectVar].textVar_.value().name(),
                     "The config variable was: ", objectVar.name()));
  }
  configVarToConfigs_[objectVar].textVar_ = subjectVar;
}

// ____________________________________________________________________________
void TextSearchQuery::predStringContainsWord(
    const Variable& subjectVar, const TripleComponent::Literal& objectLiteral) {
  configVarToConfigs_[subjectVar].isWordSearch_ = true;
  std::string_view literal = asStringViewUnsafe(objectLiteral.getContent());
  if (literal.empty()) {
    throw TextSearchException(
        "The predicate <contains-word> shouldn't have an empty literal as "
        "object.");
  }
  configVarToConfigs_[subjectVar].word_ = literal;
}

// ____________________________________________________________________________
void TextSearchQuery::predStringContainsEntity(const TripleComponent& subject,
                                               const TripleComponent& object) {
  if (!subject.isVariable()) {
    throw TextSearchException(
        "The predicate <contains-entity> needs a Variable as subject and a "
        "Literal or Variable as object.");
  }
  throwContainsWordOrEntity(subject);
  configVarToConfigs_[subject.getVariable()].isWordSearch_ = false;
  if (object.isLiteral()) {
    configVarToConfigs_[subject.getVariable()].entity_ =
        std::string(asStringViewUnsafe(object.getLiteral().getContent()));
  } else if (object.isVariable()) {
    configVarToConfigs_[subject.getVariable()].entity_ = object.getVariable();
  } else {
    throw TextSearchException(
        "The predicate <contains-entity> needs a Variable as subject and a "
        "Literal or Variable as object.");
  }
}

// ____________________________________________________________________________
void TextSearchQuery::predStringBindMatch(const Variable& subjectVar,
                                          const Variable& objectVar) {
  if (configVarToConfigs_[subjectVar].varToBindMatch_.has_value()) {
    throw TextSearchException(
        "A text search should only contain one <bind-match>.");
  }
  configVarToConfigs_[subjectVar].varToBindMatch_ = objectVar;
}

// ____________________________________________________________________________
void TextSearchQuery::predStringBindScore(const Variable& subjectVar,
                                          const Variable& objectVar) {
  if (configVarToConfigs_[subjectVar].varToBindScore_.has_value()) {
    throw TextSearchException(
        "A text search should only contain one <bind-score>.");
  }
  configVarToConfigs_[subjectVar].varToBindScore_ = objectVar;
}

// ____________________________________________________________________________
void TextSearchQuery::addParameter(const SparqlTriple& triple) {
  auto simpleTriple = triple.getSimple();
  const TripleComponent& subject = simpleTriple.s_;
  const TripleComponent& predicate = simpleTriple.p_;
  const TripleComponent& object = simpleTriple.o_;

  auto predString = extractParameterName(predicate, TEXT_SEARCH_IRI);

  if (predString == "text-search") {
    throwSubjectAndObjectVariableException("text-search", subject, object);
    predStringTextSearch(subject.getVariable(), object.getVariable());
  } else if (predString == "contains-word") {
    throwSubjectVariableException("contains-word", subject);
    throwContainsWordOrEntity(subject);
    if (!object.isLiteral()) {
      throw TextSearchException(
          "The predicate <contains-word> only accepts a literal as object.");
    }
    predStringContainsWord(subject.getVariable(), object.getLiteral());
  } else if (predString == "contains-entity") {
    throwSubjectVariableException("contains-word", subject);
    throwContainsWordOrEntity(subject);
    predStringContainsEntity(subject, object);
  } else if (predString == "bind-match") {
    throwSubjectAndObjectVariableException("bind-match", subject, object);
    predStringBindMatch(subject.getVariable(), object.getVariable());
  } else if (predString == "bind-score") {
    throwSubjectAndObjectVariableException("bind-score", subject, object);
    predStringBindScore(subject.getVariable(), object.getVariable());
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
      potentialTermsForCvar[conf.textVar_.value()].push_back(
          conf.word_.value());
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
