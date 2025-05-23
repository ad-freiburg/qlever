// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#include "parser/TextSearchQuery.h"

#include <absl/strings/str_split.h>

#include "parser/MagicServiceIriConstants.h"
#include "util/http/HttpParser/AcceptHeaderQleverVisitor.h"

// ____________________________________________________________________________
std::ostream& operator<<(std::ostream& os,
                         const TextIndexScanForEntityConfiguration& conf) {
  // This is due to SonarQube not accepting nested ternary operators
  std::string varOrFixedOut;
  if (!conf.varOrFixed_.has_value()) {
    varOrFixedOut = "not set";
  } else {
    if (conf.varOrFixed_.value().hasFixedEntity()) {
      varOrFixedOut =
          std::get<FixedEntity>(conf.varOrFixed_.value().entity_).first;
    } else {
      varOrFixedOut =
          std::get<Variable>(conf.varOrFixed_.value().entity_).name();
    }
  }
  os << "varToBindText_: " << conf.varToBindText_.name() << "; entity_: "
     << (std::holds_alternative<Variable>(conf.entity_)
             ? std::get<Variable>(conf.entity_).name()
             : std::get<std::string>(conf.entity_))
     << "; word_: " << conf.word_ << "scoreVar_: "
     << (conf.scoreVar_.has_value() ? conf.scoreVar_.value().name() : "not set")
     << "; variableColumns_: "
     << (conf.variableColumns_.has_value() ? "is set" : "not set")
     << "; varOrFixed_: " << varOrFixedOut;
  return os;
}

// ____________________________________________________________________________
std::ostream& operator<<(std::ostream& os,
                         const TextIndexScanForWordConfiguration& conf) {
  os << "varToBindText_: " << conf.varToBindText_.name()
     << "; word_: " << conf.word_ << "; matchVar_: "
     << (conf.matchVar_.has_value() ? conf.matchVar_.value().name() : "not set")
     << "; scoreVar_: "
     << (conf.scoreVar_.has_value() ? conf.scoreVar_.value().name() : "not set")
     << "; isPrefix_: " << (conf.isPrefix_ ? "true" : "false")
     << "; variableColumns_: "
     << (conf.variableColumns_.has_value() ? "is set" : "not set");
  return os;
}

// ____________________________________________________________________________
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
        "<word> or <entity>.");
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
        "The predicate <word> shouldn't have an empty literal as "
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
        "The predicate <entity> needs a variable as subject and an "
        "IRI, literal or variable as object. The object given was: ",
        object.toString()));
  }
}

// ____________________________________________________________________________
void TextSearchQuery::predStringBindMatch(const Variable& subjectVar,
                                          const Variable& objectVar) {
  if (configVarToConfigs_[subjectVar].matchVar_.has_value()) {
    throw TextSearchException(absl::StrCat(
        "Each text search config should only contain at most one "
        "<prefix-match>. "
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
        "Each text search config should only contain at most one <score>. "
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
  if (predString == "contains") {
    checkSubjectAndObjectAreVariables("contains", subject, object);
    predStringTextSearch(subject.getVariable(), object.getVariable());
  } else if (predString == "word") {
    checkSubjectIsVariable("word", subject);
    checkOneContainsWordOrEntity(subject);
    checkObjectIsLiteral("word", object);
    predStringContainsWord(subject.getVariable(), object.getLiteral());
  } else if (predString == "entity") {
    checkSubjectIsVariable("entity", subject);
    checkOneContainsWordOrEntity(subject);
    predStringContainsEntity(subject.getVariable(), object);
  } else if (predString == "prefix-match") {
    checkSubjectAndObjectAreVariables("prefix-match", subject, object);
    predStringBindMatch(subject.getVariable(), object.getVariable());
  } else if (predString == "score") {
    checkSubjectAndObjectAreVariables("score", subject, object);
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
  ad_utility::HashMap<Variable, vector<string>> potentialTermsForTextVar;
  ad_utility::HashMap<Variable, string> optTermForTextVar;
  for (const auto& [var, conf] : configVarToConfigs_) {
    if (!conf.isWordSearch_.has_value()) {
      throw TextSearchException(absl::StrCat(
          "Text search service needs configs with exactly one occurrence of "
          "either <word> or <entity>. The config variable "
          "was: ",
          var.name()));
    }
    if (!conf.textVar_.has_value()) {
      // Last updated this error message 24.02.2025
      throw TextSearchException(absl::StrCat(
          "Text search service needs a text variable that is linked to one or "
          "multiple text search config variables with the predicate "
          "<contains>. \n"
          "The config variable can then be used with the predicates: "
          "<word>, <entity>, <prefix-match>, <score>. \n"
          "<word>: This predicate needs a literal as object which has "
          "one word with optionally a * at the end. This word or prefix is "
          "then used to search the text index. \n"
          "<entity>: This predicate needs a variable, IRI or literal "
          "as object. If a variable is given this variable can be used outside "
          "of this service. If an IRI or literal is given the entity is fixed. "
          "The entity given is then used to search the text index. \n"
          "A config should contain exactly one occurrence of either "
          "<word> or <entity>. \n"
          "<prefix-match>: This predicate should only be used in a text search "
          "config with a word that is a prefix. The object should be a "
          "variable. That variable specifies the variable for the prefix "
          "match.\n"
          "<score>: The object of this predicate should be a variable. "
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
      potentialTermsForTextVar[conf.textVar_.value()].push_back(
          conf.word_.value());
    }
  }
  // Get the correct words for entity scans
  for (const auto& [textVar, potentialTerms] : potentialTermsForTextVar) {
    optTermForTextVar[textVar] =
        potentialTerms[qec->getIndex().getIndexOfBestSuitedElTerm(
            potentialTerms)];
  }

  // Second pass to create all configs
  for (const auto& [var, conf] : configVarToConfigs_) {
    if (conf.isWordSearch_.value()) {
      output.emplace_back(TextIndexScanForWordConfiguration{
          conf.textVar_.value(), conf.word_.value(), conf.matchVar_,
          conf.scoreVar_});
    } else {
      if (!optTermForTextVar.contains(conf.textVar_.value())) {
        throw TextSearchException(absl::StrCat(
            "Entity search has to happen on a text variable that is also "
            "contained in a word search. Text variable: ",
            conf.textVar_.value().name(),
            " is not contained in a word search."));
      }
      output.emplace_back(TextIndexScanForEntityConfiguration{
          conf.textVar_.value(), conf.entity_.value(),
          optTermForTextVar[conf.textVar_.value()], conf.scoreVar_});
    }
  }
  return output;
}

}  // namespace parsedQuery
