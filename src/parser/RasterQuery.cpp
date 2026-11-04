// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Tanmay Garg <gargt@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "parser/RasterQuery.h"

#include <absl/strings/str_cat.h>

#include <limits>
#include <string_view>

#include "parser/MagicServiceIriConstants.h"
#include "parser/SparqlTriple.h"

namespace parsedQuery {

// ____________________________________________________________________________
void RasterQuery::addParameter(const SparqlTriple& triple) {
  auto simpleTriple = triple.getSimple();
  TripleComponent predicate = simpleTriple.p_;
  TripleComponent object = simpleTriple.o_;

  auto predString = extractParameterName(predicate, RASTER_IRI);

  if (predString == "dataset") {
    if (dataset_.has_value()) {
      throw RasterException(
          "The parameter <dataset> must not be set more than once");
    }
    if (!object.isLiteral()) {
      throw RasterException("The parameter <dataset> expects a string literal");
    }
    dataset_ = std::string{object.getLiteral().getContent()};
  } else if (predString == "band") {
    if (band_.has_value()) {
      throw RasterException(
          "The parameter <band> must not be set more than once");
    }
    if (!object.isInt()) {
      throw RasterException("The parameter <band> expects an integer");
    }
    // The upper bound depends on the file and is checked by `RasterLookup`.
    if (object.getInt() < 1 ||
        object.getInt() > std::numeric_limits<int>::max()) {
      throw RasterException("The parameter <band> must be at least 1");
    }
    band_ = static_cast<int>(object.getInt());
  } else if (predString == "geometry") {
    setVariable("geometry", object, geometry_);
  } else if (predString == "value") {
    setVariable("value", object, value_);
  } else if (predString == "center") {
    setVariable("center", object, center_);
  } else {
    throw RasterException(absl::StrCat(
        "Unknown parameter for raster lookup: <", predString,
        ">. Supported parameters: <dataset>, <band>, <geometry>, <value>, "
        "<center>"));
  }
}

}  // namespace parsedQuery
