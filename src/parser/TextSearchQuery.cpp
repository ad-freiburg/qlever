// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#include "parser/TextSearchQuery.h"

#include "parser/MagicServiceIriConstants.h"

namespace parsedQuery {

// ____________________________________________________________________________
void TextSearchQuery::addParameter(const SparqlTriple& triple) {
  auto sipmleTriple = triple.getSimple();
  TripleComponent predicate = sipmleTriple.p_;
  TripleComponent object = sipmleTriple.o_;

  auto predString = extractParameterName(predicate, TEXT_SEARCH_IRI);

  if (predString == "containsWord") {
    if (!object.isString()) {
      throw TextSearchException(
          "The parameter <containsWord> expects a string");
    }
    word_ = object.getString();
  } else if (predString == "bindMatch") {
    setVariable("bindMatch", object, matchVar_);
  } else if (predString == "bindScore") {
    setVariable("bindScore", object, scoreVar_);
  } else if (predString == "bindText") {
    setVariable("bindText", object, textVar_);
  }
}

// ____________________________________________________________________________
TextIndexScanForWordConfiguration
TextSearchQuery::toTextIndexScanForWordConfiguration() const {
  if (!word_.has_value()) {
    throw TextSearchException(
        "Missing parameter <containsWord> in text search.");
  } else if (!textVar_.has_value()) {
    throw TextSearchException("Missing parameter <bindText> in text search.");
  }
  return TextIndexScanForWordConfiguration{textVar_.value(), word_.value(),
                                           matchVar_, scoreVar_};
}

}  // namespace parsedQuery
