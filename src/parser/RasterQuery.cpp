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

#include <charconv>
#include <optional>
#include <string_view>
#include <system_error>

#include "backports/StartsWithAndEndsWith.h"
#include "backports/algorithm.h"
#include "parser/MagicServiceIriConstants.h"
#include "parser/SparqlTriple.h"
#include "util/Exception.h"

namespace parsedQuery {

namespace {

// The number `N` of a parameter `bandN`, for example 3 for `band3`. Nothing if
// `parameter` does not have this form.
std::optional<int> parseBandNumber(std::string_view parameter) {
  constexpr std::string_view prefix = "band";
  if (!ql::starts_with(parameter, prefix)) {
    return std::nullopt;
  }
  std::string_view digits = parameter.substr(prefix.size());
  int number = 0;
  auto [end, error] =
      std::from_chars(digits.data(), digits.data() + digits.size(), number);
  if (error == std::errc::result_out_of_range) {
    throw RasterException(
        absl::StrCat("The band number of `<", parameter, ">` is too large"));
  }
  // Also for no digits at all (`band`) and for characters after the digits.
  if (error != std::errc{} || end != digits.data() + digits.size()) {
    return std::nullopt;
  }
  if (number < 1) {
    throw RasterException(
        absl::StrCat("Band numbers start at 1: `<", parameter, ">`"));
  }
  return number;
}

}  // namespace

// ____________________________________________________________________________
std::string extractRasterDatasetName(const TripleComponent::Iri& iri) {
  std::string_view content = iri.getContent();
  if (!ql::starts_with(content, RASTER_DATASET_IRI_WITHOUT_BRACKETS)) {
    throw RasterException(
        absl::StrCat("A raster dataset must be an IRI of the form `<",
                     RASTER_DATASET_IRI_WITHOUT_BRACKETS, "NAME>`, but was `<",
                     content, ">`"));
  }
  content.remove_prefix(RASTER_DATASET_IRI_WITHOUT_BRACKETS.size());
  if (content.empty()) {
    throw RasterException(absl::StrCat("The raster dataset IRI `<",
                                       RASTER_DATASET_IRI_WITHOUT_BRACKETS,
                                       ">` has no name after the namespace"));
  }
  return std::string{content};
}

// ____________________________________________________________________________
void RasterQuery::addParameter(const SparqlTriple& triple) {
  auto simpleTriple = triple.getSimple();
  TripleComponent predicate = simpleTriple.p_;
  TripleComponent object = simpleTriple.o_;

  auto predString = extractParameterName(predicate, RASTER_IRI);

  if (predString == "dataset") {
    if (dataset_.has_value()) {
      throw RasterException(
          "The parameter `<dataset>` must not be set more than once");
    }
    if (object.isLiteral()) {
      dataset_ = std::string{object.getLiteral().getContent()};
    } else if (object.isIri()) {
      dataset_ = extractRasterDatasetName(object.getIri());
    } else if (object.isVariable()) {
      dataset_ = object.getVariable();
    } else {
      throw RasterException(absl::StrCat(
          "The parameter `<dataset>` expects a string literal, a variable, ",
          "or an IRI of the form `<", RASTER_DATASET_IRI_WITHOUT_BRACKETS,
          "NAME>`"));
    }
  } else if (std::optional<int> number = parseBandNumber(predString);
             number.has_value()) {
    // The real upper bound of the number depends on the file and is checked
    // by `RasterLookup`.
    Variable variable = getVariable(predString, object);
    if (ql::ranges::any_of(bands_, [&number](const auto& band) {
          return band.number_ == number.value();
        })) {
      throw RasterException(absl::StrCat("The parameter `<", predString,
                                         ">` must not be set more than once"));
    }
    bands_.push_back({number.value(), std::move(variable)});
  } else if (predString == "radius") {
    if (radius_.has_value()) {
      throw RasterException(
          "The parameter `<radius>` must not be set more than once");
    }
    std::optional<double> radius = object.getNumber();
    // Written as "positive" so that NaN is rejected too.
    if (!radius.has_value() || !(radius.value() > 0)) {
      throw RasterException(
          "The parameter `<radius>` expects a positive number (in metres)");
    }
    radius_ = radius;
  } else if (predString == "geometry") {
    setVariable("geometry", object, geometry_);
  } else if (predString == "center") {
    setVariable("center", object, center_);
  } else {
    throw RasterException(absl::StrCat(
        "Unknown parameter for raster lookup: `<", predString,
        ">`. Supported parameters: `<dataset>`, `<bandN>` (for example "
        "`<band1>`), `<radius>`, `<geometry>`, `<center>`"));
  }
}

// ____________________________________________________________________________
void RasterQuery::validate() const {
  auto throwIfMissing = [](bool isMissing, std::string_view parameter) {
    if (isMissing) {
      throw RasterException(absl::StrCat("A raster lookup requires a `<",
                                         parameter, ">` parameter"));
    }
  };
  throwIfMissing(!dataset_.has_value(), "dataset");
  throwIfMissing(!geometry_.has_value(), "geometry");
  if (bands_.empty()) {
    throw RasterException(
        "A raster lookup requires at least one band, for example "
        "`raster:band1 ?value`");
  }
  if (!childGraphPattern_.has_value()) {
    throw RasterException(
        "A raster lookup requires a group graph pattern that binds the "
        "geometry variable: SERVICE raster: { [Config Triples] { ?x "
        "geo:asWKT ?geometry } }");
  }
}

// ____________________________________________________________________________
RasterLookupConfiguration RasterQuery::toRasterLookupConfiguration() const {
  AD_CORRECTNESS_CHECK(dataset_.has_value() && geometry_.has_value() &&
                       !bands_.empty());
  return RasterLookupConfiguration{dataset_.value(), bands_, radius_,
                                   geometry_.value(), center_};
}

}  // namespace parsedQuery
