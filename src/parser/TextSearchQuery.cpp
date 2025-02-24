// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#include "parser/TextSearchQuery.h"

#include <absl/strings/str_split.h>

#include "parser/MagicServiceIriConstants.h"
#include "util/http/HttpParser/AcceptHeaderQleverVisitor.h"

namespace parsedQuery {

// ____________________________________________________________________________
void TextSearchQuery::throwSubjectVariableException(
    std::string_view predString, const TripleComponent& subject) {
  if (!subject.isVariable()) {
    throw TextSearchException(
        absl::StrCat("The predicate <", predString,
                     "> needs a Variable as subject. The subject given was: ",
                     subject.toString()));
  }
}

// ____________________________________________________________________________
void TextSearchQuery::throwSubjectAndObjectVariableException(
    std::string_view predString, const TripleComponent& subject,
    const TripleComponent& object) {
  if (!(subject.isVariable() && object.isVariable())) {
    throw TextSearchException(absl::StrCat(
        "The predicate <", predString,
        "> needs a Variable as subject and one as object. The subject given "
        "was: ",
        subject.toString(), ". The object given was: ", object.toString()));
  }
}

// ____________________________________________________________________________
void TextSearchQuery::throwContainsWordOrEntity(
    const TripleComponent& subject) {
  if (configVarToConfigs_[subject.getVariable()].isWordSearch_.has_value()) {
    throw TextSearchException(
        "Each text search config should have exactly one occurrence of either "
        "<contains-word> or <contains-entity>.");
  }
}

// ____________________________________________________________________________
void TextSearchQuery::throwObjectLiteralException(
    std::string_view predString, const TripleComponent& object) {
  if (!object.isLiteral()) {
    throw TextSearchException(
        absl::StrCat("The predicate <", predString,
                     "> needs a Literal as object. The object given was: ",
                     object.toString()));
  }
}

// ____________________________________________________________________________
void TextSearchQuery::predStringTextSearch(const Variable& subjectVar,
                                           const Variable& objectVar) {
  if (configVarToConfigs_[objectVar].textVar_.has_value()) {
    throw TextSearchException(absl::StrCat(
        "Each text search config should only be linked to a single text "
        "Variable. The second text variable given was: ",
        subjectVar.name(), ". The config variable was: ", objectVar.name()));
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
        "The predicate <contains-word> shouldn't have an empty Literal as "
        "object.");
  }
  configVarToConfigs_[subjectVar].word_ = literal;
}

// ____________________________________________________________________________
void TextSearchQuery::predStringContainsEntity(const Variable& subjectVar,
                                               const TripleComponent& object) {
  configVarToConfigs_[subjectVar].isWordSearch_ = false;
  if (object.isLiteral()) {
    configVarToConfigs_[subjectVar].entity_ =
        std::string(asStringViewUnsafe(object.getLiteral().getContent()));
  } else if (object.isVariable()) {
    configVarToConfigs_[subjectVar].entity_ = object.getVariable();
  } else if (object.isIri()) {
    configVarToConfigs_[subjectVar].entity_ =
        object.getIri().toStringRepresentation();
  } else {
    throw TextSearchException(absl::StrCat(
        "The predicate <contains-entity> needs a Variable as subject and an "
        "IRI, Literal or Variable as object. The object given was: ",
        object.toString()));
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
  const auto& simpleTriple = triple.getSimple();
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
    throwObjectLiteralException("contains-word", object);
    predStringContainsWord(subject.getVariable(), object.getLiteral());
  } else if (predString == "contains-entity") {
    throwSubjectVariableException("contains-entity", subject);
    throwContainsWordOrEntity(subject);
    predStringContainsEntity(subject.getVariable(), object);
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
TextSearchQuery::toConfigs(const QueryExecutionContext* qec) const {
  std::vector<std::variant<TextIndexScanForWordConfiguration,
                           TextIndexScanForEntityConfiguration>>
      output;
  // First pass to get all word searches
  ad_utility::HashMap<Variable, vector<string>> potentialTermsForCvar;
  for (const auto& [var, conf] : configVarToConfigs_) {
    if (!conf.isWordSearch_.has_value()) {
      throw TextSearchException(
          "Text search service needs configs with exactly one occurrence of "
          "either <contains-word> or "
          "<contains-entity>.");
    }
    if (!conf.textVar_.has_value()) {
      AD_FAIL();
    }
    if (conf.isWordSearch_.value()) {
      potentialTermsForCvar[conf.textVar_.value()].push_back(
          conf.word_.value());
    }
  }
  // Second pass to create all configs
  for (const auto& [var, conf] : configVarToConfigs_) {
    if (conf.isWordSearch_.value()) {
      output.emplace_back(TextIndexScanForWordConfiguration{
          conf.textVar_.value(), conf.word_.value(), conf.varToBindMatch_,
          conf.varToBindScore_});
    } else {
      auto it = potentialTermsForCvar.find(conf.textVar_.value());
      if (it == potentialTermsForCvar.end()) {
        throw TextSearchException(absl::StrCat(
            "Entity search has to happen on a text Variable that is also "
            "contained in a word search. Text Variable: ",
            conf.textVar_.value().name(),
            " is not contained in a word search."));
      }
      output.emplace_back(TextIndexScanForEntityConfiguration{
          conf.textVar_.value(), conf.entity_.value(),
          it->second[qec->getIndex().getIndexOfBestSuitedElTerm(it->second)],
          conf.varToBindScore_});
    }
  }
  return output;
}

}  // namespace parsedQuery
