// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "parser/RowNumberQuery.h"

#include "parser/GraphPatternOperation.h"
#include "parser/MagicServiceIriConstants.h"
#include "parser/ParsedQuery.h"
#include "parser/SparqlTriple.h"
#include "util/Algorithm.h"

namespace parsedQuery {

// ____________________________________________________________________________
void RowNumberQuery::addParameter(const SparqlTriple& triple) {
  auto simpleTriple = triple.getSimple();
  const TripleComponent& object = simpleTriple.o_;
  auto predString = extractParameterName(simpleTriple.p_, ROW_NUMBER_IRI);

  if (predString == "bind") {
    setVariable("bind", object, rowNumberVariable_);
  } else if (predString == "offset") {
    // Negative row numbers are not allowed, because the result would then not
    // be sorted by the row number column (see `RowNumber::resultSortedOn`).
    if (!object.isInt() || object.getInt() < 0) {
      throw MagicServiceException(
          "The parameter `<offset>` expects a non-negative integer");
    }
    offset_ = object.getInt();
  } else {
    throw MagicServiceException(absl::StrCat(
        "Unknown parameter for row number query: `<", predString, ">`"));
  }
}

// ____________________________________________________________________________
void RowNumberQuery::validate() const {
  if (!rowNumberVariable_.has_value()) {
    throw MagicServiceException(
        "A row number query requires a `<bind>` parameter");
  }

  // The row numbers are only well-defined if the child is a sorted subquery.
  auto isSortedSubquery = [](const GraphPattern& pattern) {
    if (pattern._graphPatterns.size() != 1 || !pattern._filters.empty()) {
      return false;
    }
    const auto* subquery = std::get_if<Subquery>(&pattern._graphPatterns[0]);
    return subquery != nullptr && !subquery->get()._orderBy.empty();
  };
  if (!childGraphPattern_.has_value() ||
      !isSortedSubquery(childGraphPattern_.value())) {
    throw MagicServiceException(
        "A row number query requires exactly one group graph pattern that "
        "consists of a single subquery with an `ORDER BY` or `INTERNAL SORT "
        "BY` clause");
  }

  // The row number variable must not shadow a variable of the subquery.
  const auto& subquery =
      std::get<Subquery>(childGraphPattern_->_graphPatterns[0]).get();
  if (ad_utility::contains(subquery.selectClause().getSelectedVariables(),
                           rowNumberVariable_.value())) {
    throw MagicServiceException(absl::StrCat(
        "The variable ", rowNumberVariable_->name(),
        " of the `<bind>` parameter must not be selected by the subquery"));
  }
}

}  // namespace parsedQuery
