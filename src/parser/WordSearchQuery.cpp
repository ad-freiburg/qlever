// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#include "parser/WordSearchQuery.h"

#include "parser/MagicServiceIriConstants.h"

namespace parsedQuery {

// ____________________________________________________________________________
void WordSearchQuery::addParameter(const SparqlTriple& triple) {
  auto sipmleTriple = triple.getSimple();
  TripleComponent predicate = sipmleTriple.p_;
  TripleComponent object = sipmleTriple.o_;

  auto predString = extractParameterName(predicate, WORD_SEARCH_IRI);

  if (predString == "containsWord") {
    if (!object.isLiteral()) {
      throw WordSearchException(
          "The parameter <containsWord> expects a string");
    }
    std::string_view literal = object.getLiteral().toStringRepresentation();
    word_ = std::string(literal.substr(1, literal.size() - 2));
  } else if (predString == "bindMatch") {
    setVariable("bindMatch", object, matchVar_);
  } else if (predString == "bindScore") {
    setVariable("bindScore", object, scoreVar_);
  } else if (predString == "bindText") {
    setVariable("bindText", object, textVar_);
  }
}

// ____________________________________________________________________________
TextIndexScanForWordConfiguration WordSearchQuery::toConfig() const {
  if (!word_.has_value()) {
    throw WordSearchException(
        "Missing parameter <containsWord> in text search.");
  } else if (!textVar_.has_value()) {
    throw WordSearchException("Missing parameter <bindText> in text search.");
  }
  return TextIndexScanForWordConfiguration{textVar_.value(), word_.value(),
                                           matchVar_, scoreVar_};
}

}  // namespace parsedQuery
