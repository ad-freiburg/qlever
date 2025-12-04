// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "parser/ProxyQuery.h"

#include <absl/strings/str_cat.h>

#include "parser/MagicServiceIriConstants.h"
#include "parser/SparqlTriple.h"

namespace parsedQuery {

// ____________________________________________________________________________
void ProxyQuery::addParameter(const SparqlTriple& triple) {
  auto simpleTriple = triple.getSimple();
  TripleComponent predicate = simpleTriple.p_;
  TripleComponent object = simpleTriple.o_;

  auto predString = extractParameterName(predicate, QLPROXY_IRI);

  if (predString == "endpoint") {
    throwIf(!object.isIri(),
            "The parameter `<endpoint>` expects an IRI (the remote endpoint "
            "URL)");
    // Extract the IRI content without angle brackets.
    endpoint_ = std::string(asStringViewUnsafe(object.getIri().getContent()));
  } else if (predString.starts_with("payload_")) {
    std::string paramName(predString.substr(8));  // Remove "payload_" prefix
    throwIf(paramName.empty(),
            "The payload parameter name cannot be empty (use "
            "`qlproxy:payload_<name>`)");
    Variable var = getVariable(predString, object);
    payloadVariables_.emplace_back(std::move(paramName), std::move(var));
  } else if (predString.starts_with("result_")) {
    std::string paramName(predString.substr(7));  // Remove "result_" prefix
    throwIf(paramName.empty(),
            "The result parameter name cannot be empty (use "
            "`qlproxy:result_<name>`)");
    Variable var = getVariable(predString, object);
    resultVariables_.emplace_back(std::move(paramName), std::move(var));
  } else if (predString.starts_with("param_")) {
    std::string paramName(predString.substr(6));  // Remove "param_" prefix
    throwIf(paramName.empty(),
            "The URL parameter name cannot be empty (use "
            "`qlproxy:param_<name>`)");
    throwIf(!object.isLiteral(),
            absl::StrCat("The parameter `<param_", paramName,
                         ">` expects a literal value"));
    std::string value(asStringViewUnsafe(object.getLiteral().getContent()));
    parameters_.emplace_back(std::move(paramName), std::move(value));
  } else {
    throw ProxyException(absl::StrCat(
        "Unsupported parameter `", predString,
        "` in qlproxy service`. Supported parameters are: `<endpoint>`, "
        "`<payload_NAME>`, `<result_NAME>`, and `<param_NAME>`"));
  }
}

// ____________________________________________________________________________
void ProxyQuery::addGraph(const GraphPatternOperation&) {
  throw ProxyException(
      "The qlproxy service does not support nested graph patterns; "
      "only configuration triples are allowed inside the SERVICE block");
}

// ____________________________________________________________________________
ProxyConfiguration ProxyQuery::toConfiguration() const {
  throwIf(!endpoint_.has_value(),
          "Missing required parameter `<endpoint>` in qlproxy service");

  throwIf(resultVariables_.empty(),
          "At least one result variable is required (use `qlproxy:result_NAME "
          "?var`)");

  return ProxyConfiguration{endpoint_.value(), payloadVariables_,
                            resultVariables_, parameters_};
}

// ____________________________________________________________________________
void ProxyQuery::throwIf(bool condition, std::string_view message) const {
  if (condition) {
    throw ProxyException{std::string(message)};
  }
}

}  // namespace parsedQuery
