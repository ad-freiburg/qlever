// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#include "parser/TextSearchQuery.h"

#include <absl/strings/str_split.h>

#include "parser/MagicServiceIriConstants.h"
#include "util/http/HttpParser/AcceptHeaderQleverVisitor.h"

std::variant<Variable, FixedEntity> VarOrFixedEntity::makeEntityVariant(
    const QueryExecutionContext* qec,
    std::variant<Variable, std::string> entity) {
  if (std::holds_alternative<std::string>(entity)) {
    VocabIndex index;
    std::string fixedEntity = std::move(std::get<std::string>(entity));
    bool success = qec->getIndex().getVocab().getId(fixedEntity, &index);
    if (!success) {
      throw std::runtime_error(
          "The entity " + fixedEntity +
          " is not part of the underlying knowledge graph and can "
          "therefore not be used as the object of ql:contains-entity");
    }
    return FixedEntity(std::move(fixedEntity), std::move(index));
  }
  return std::get<Variable>(entity);
};

namespace parsedQuery {

// ____________________________________________________________________________
void TextSearchQuery::checkSubjectIsVariable(std::string_view predString,
                                             const TripleComponent& subject) {
  if (!subject.isVariable()) {
    throw TextSearchException(
        absl::StrCat("The predicate <", predString,
                     "> needs a variable as subject. The subject given was: ",
                     subject.toString()));
  }
}

// ____________________________________________________________________________
void TextSearchQuery::checkSubjectAndObjectAreVariables(
    std::string_view predString, const TripleComponent& subject,
    const TripleComponent& object) {
  if (!(subject.isVariable() && object.isVariable())) {
    throw TextSearchException(absl::StrCat(
        "The predicate <", predString,
        "> needs a variable as subject and one as object. The subject given "
        "was: ",
        subject.toString(), ". The object given was: ", object.toString()));
  }
}

// ____________________________________________________________________________
void TextSearchQuery::checkOneContainsWordOrEntity(
    const TripleComponent& subject) {
  if (configVarToConfigs_[subject.getVariable()].isWordSearch_.has_value()) {
    throw TextSearchException(
        "Each text search config should have exactly one occurrence of either "
        "<contains-word> or <contains-entity>.");
  }
}

// ____________________________________________________________________________
void TextSearchQuery::checkObjectIsLiteral(std::string_view predString,
                                           const TripleComponent& object) {
  if (!object.isLiteral()) {
    throw TextSearchException(
        absl::StrCat("The predicate <", predString,
                     "> needs a literal as object. The object given was: ",
                     object.toString()));
  }
}

// ____________________________________________________________________________
void TextSearchQuery::predStringTextSearch(const Variable& subjectVar,
                                           const Variable& objectVar) {
  if (configVarToConfigs_[objectVar].textVar_.has_value()) {
    throw TextSearchException(absl::StrCat(
        "Each text search config should only be linked to a single text "
        "variable. The second text variable given was: ",
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
        "The predicate <contains-word> shouldn't have an empty literal as "
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
        "The predicate <contains-entity> needs a variable as subject and an "
        "IRI, literal or variable as object. The object given was: ",
        object.toString()));
  }
}

// ____________________________________________________________________________
void TextSearchQuery::predStringBindMatch(const Variable& subjectVar,
                                          const Variable& objectVar) {
  if (configVarToConfigs_[subjectVar].matchVar_.has_value()) {
    throw TextSearchException(absl::StrCat(
        "Each text search config should only contain at most one <bind-match>. "
        "The second match variable given was: ",
        objectVar.name(), ". The config variable was: ", subjectVar.name()));
  }
  configVarToConfigs_[subjectVar].matchVar_ = objectVar;
}

// ____________________________________________________________________________
void TextSearchQuery::predStringBindScore(const Variable& subjectVar,
                                          const Variable& objectVar) {
  if (configVarToConfigs_[subjectVar].scoreVar_.has_value()) {
    throw TextSearchException(absl::StrCat(
        "Each text search config should only contain at most one <bind-score>. "
        "The second match variable given was: ",
        objectVar.name(), ". The config variable was: ", subjectVar.name()));
  }
  configVarToConfigs_[subjectVar].scoreVar_ = objectVar;
}

// ____________________________________________________________________________
void TextSearchQuery::addParameter(const SparqlTriple& triple) {
  const auto& simpleTriple = triple.getSimple();
  const TripleComponent& subject = simpleTriple.s_;
  const TripleComponent& predicate = simpleTriple.p_;
  const TripleComponent& object = simpleTriple.o_;

  auto predString = extractParameterName(predicate, TEXT_SEARCH_IRI);
  if (predString == "text-search") {
    checkSubjectAndObjectAreVariables("text-search", subject, object);
    predStringTextSearch(subject.getVariable(), object.getVariable());
  } else if (predString == "contains-word") {
    checkSubjectIsVariable("contains-word", subject);
    checkOneContainsWordOrEntity(subject);
    checkObjectIsLiteral("contains-word", object);
    predStringContainsWord(subject.getVariable(), object.getLiteral());
  } else if (predString == "contains-entity") {
    checkSubjectIsVariable("contains-entity", subject);
    checkOneContainsWordOrEntity(subject);
    predStringContainsEntity(subject.getVariable(), object);
  } else if (predString == "bind-match") {
    checkSubjectAndObjectAreVariables("bind-match", subject, object);
    predStringBindMatch(subject.getVariable(), object.getVariable());
  } else if (predString == "bind-score") {
    checkSubjectAndObjectAreVariables("bind-score", subject, object);
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
      throw TextSearchException(absl::StrCat(
          "Text search service needs configs with exactly one occurrence of "
          "either <contains-word> or <contains-entity>. The config variable "
          "was: ",
          var.name()));
    }
    if (!conf.textVar_.has_value()) {
      // Last updated this error message 24.02.2025
      throw TextSearchException(absl::StrCat(
          "Text search service needs a text variable that is linked to one or "
          "multiple text search config variables with the predicate "
          "<text-search>. \n"
          "The config variable can then be used with the predicates: "
          "<contains-word>, <contains-entity>, <bind-match>, <bind-score>. \n"
          "<contains-word>: This predicate needs a literal as object which has "
          "one word with optionally a * at the end. This word or prefix is "
          "then used to search the text index. \n"
          "<contains-entity>: This predicate needs a variable, IRI or literal "
          "as object. If a variable is given this variable can be used outside "
          "of this service. If an IRI or literal is given the entity is fixed. "
          "The entity given is then used to search the text index. \n"
          "A config should contain exactly one occurrence of either "
          "<contains-word> or <contains-entity>. \n"
          "<bind-match>: This predicate should only be used in a text search "
          "config with a word that is a prefix. The object should be a "
          "variable. That variable specifies the variable for the prefix "
          "match.\n"
          "<bind-score>: The object of this predicate should be a variable. "
          "That variable specifies the column name for the column containing "
          "the scores of the respective word or entity search. \n",
          "The config variable was: ", var.name()));
    }
    if (conf.isWordSearch_.value()) {
      if (conf.matchVar_.has_value() && !conf.word_.value().ends_with("*")) {
        throw TextSearchException(
            absl::StrCat("The text search config shouldn't define a variable "
                         "for the prefix match column if the word isn't a "
                         "prefix. The config variable was: ",
                         var.name(), ". The word was: \"", conf.word_.value(),
                         "\". The text variable bound to was: ",
                         conf.textVar_.value().name()));
      }
      potentialTermsForCvar[conf.textVar_.value()].push_back(
          conf.word_.value());
    }
  }
  // Second pass to create all configs
  for (const auto& [var, conf] : configVarToConfigs_) {
    if (conf.isWordSearch_.value()) {
      output.emplace_back(TextIndexScanForWordConfiguration{
          conf.textVar_.value(), conf.word_.value(), conf.matchVar_,
          conf.scoreVar_});
    } else {
      auto it = potentialTermsForCvar.find(conf.textVar_.value());
      if (it == potentialTermsForCvar.end()) {
        throw TextSearchException(absl::StrCat(
            "Entity search has to happen on a text variable that is also "
            "contained in a word search. Text variable: ",
            conf.textVar_.value().name(),
            " is not contained in a word search."));
      }
      output.emplace_back(TextIndexScanForEntityConfiguration{
          conf.textVar_.value(), conf.entity_.value(),
          it->second[qec->getIndex().getIndexOfBestSuitedElTerm(it->second)],
          conf.scoreVar_});
    }
  }
  return output;
}

}  // namespace parsedQuery
