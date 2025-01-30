// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#include "parser/EntitySearchQuery.h"

#include "parser/MagicServiceIriConstants.h"

namespace parsedQuery {

// ____________________________________________________________________________
void EntitySearchQuery::addParameter(const SparqlTriple& triple) {
  auto sipmleTriple = triple.getSimple();
  TripleComponent predicate = sipmleTriple.p_;
  TripleComponent object = sipmleTriple.o_;

  auto predString = extractParameterName(predicate, ENTITY_SEARCH_IRI);

  // TODO find a smoother way to interpret given strings than reading them as
  // literals
  if (predString == "containedWithWord") {
    if (!object.isLiteral()) {
      throw EntitySearchException(
          "The parameter <containedWithWord> expects a literal consisting of "
          "one search word");
    }
    std::string_view literal = object.getLiteral().toStringRepresentation();
    word_ = std::string(literal.substr(1, literal.size() - 2));
  } else if (predString == "containsEntity") {
    if (object.isLiteral()) {
      std::string_view literal = object.getLiteral().toStringRepresentation();
      fixedEntity_ = std::string(literal.substr(1, literal.size() - 2));
    } else if (object.isVariable()) {
      setVariable("containsEntity", object, entityVar_);
    } else {
      throw EntitySearchException(
          "The parameter <containsEntity> expects a literal which is "
          "interpreted as fixed entity or a variable to bind the entity to");
    }
  } else if (predString == "bindScore") {
    setVariable("bindScore", object, scoreVar_);
  } else if (predString == "bindText") {
    setVariable("bindText", object, textVar_);
  }
}

// ____________________________________________________________________________
TextIndexScanForEntityConfiguration EntitySearchQuery::toConfig() const {
  if (!word_.has_value()) {
    throw EntitySearchException(
        "Missing parameter <containedWithWord> in entity search.");
  } else if (!textVar_.has_value()) {
    throw EntitySearchException(
        "Missing parameter <bindText> in entity search.");
  } else if (!(fixedEntity_.has_value() || entityVar_.has_value())) {
    throw EntitySearchException(
        "Missing parameter <containsEntity> in entity search.");
  } else if (fixedEntity_.has_value() && entityVar_.has_value()) {
    throw EntitySearchException(
        "<containsEntity> should be used exactly once in entity search.");
  }
  if (entityVar_.has_value()) {
    return TextIndexScanForEntityConfiguration{
        textVar_.value(), entityVar_.value(), word_.value(), scoreVar_};
  } else {
    return TextIndexScanForEntityConfiguration{
        textVar_.value(), fixedEntity_.value(), word_.value(), scoreVar_};
  }
}

}  // namespace parsedQuery
