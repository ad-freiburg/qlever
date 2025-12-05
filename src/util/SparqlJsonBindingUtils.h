// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_SPARQLJSONBINDINGUTILS_H
#define QLEVER_SRC_UTIL_SPARQLJSONBINDINGUTILS_H

#include "index/Index.h"
#include "parser/RdfParser.h"
#include "parser/TokenizerCtre.h"
#include "parser/TripleComponent.h"
#include "util/BlankNodeManager.h"
#include "util/HashMap.h"
#include "util/json.h"

namespace sparqlJsonBindingUtils {

// Convert a JSON binding from a `sparql-results+json` result set into a
// `TripleComponent`. The binding must have a "type" and a "value" field.
// Supported types are: "uri", "literal" or "typed-literal", and "bnode".
//
// The `blankNodeMap` is used to ensure consistent blank node `Id`s within a
// result set (the same blank node label always maps to the same `Id`).
//
// NOTE: This is used in the `Service` and `Proxy` operation, which both read
// `sparql-results+json` result sets from a remote endpoint.
inline TripleComponent bindingToTripleComponent(
    const nlohmann::json& binding, const Index& index,
    ad_utility::HashMap<std::string, Id>& blankNodeMap, LocalVocab* localVocab,
    ad_utility::BlankNodeManager* blankNodeManager) {
  if (!binding.contains("type") || !binding.contains("value")) {
    throw std::runtime_error(absl::StrCat(
        "Missing type or value field in binding. The binding is: '",
        binding.dump(), "'"));
  }

  const auto type = binding["type"].get<std::string_view>();
  const auto value = binding["value"].get<std::string_view>();

  TripleComponent tc;
  // NOTE: The type `typed-literal` is not part of the official SPARQL 1.1
  // standard, but was mentioned in a pre SPARQL 1.1 WG note and used by
  // Virtuoso until the summer of 2025. It is therefore still produced by
  // some SPARQL endpoints, and we therefore support parsing it.
  if (type == "literal" || type == "typed-literal") {
    if (binding.contains("datatype")) {
      tc = TurtleParser<TokenizerCtre>::literalAndDatatypeToTripleComponent(
          value,
          TripleComponent::Iri::fromIrirefWithoutBrackets(
              binding["datatype"].get<std::string_view>()),
          index.encodedIriManager());
    } else if (binding.contains("xml:lang")) {
      tc = TripleComponent::Literal::literalWithNormalizedContent(
          asNormalizedStringViewUnsafe(value),
          binding["xml:lang"].get<std::string>());
    } else {
      tc = TripleComponent::Literal::literalWithNormalizedContent(
          asNormalizedStringViewUnsafe(value));
    }
  } else if (type == "uri") {
    tc = TripleComponent::Iri::fromIrirefWithoutBrackets(value);
  } else if (type == "bnode") {
    auto [it, wasNew] = blankNodeMap.try_emplace(value, Id());
    if (wasNew) {
      it->second = Id::makeFromBlankNodeIndex(
          localVocab->getBlankNodeIndex(blankNodeManager));
    }
    tc = it->second;
  } else {
    throw std::runtime_error(absl::StrCat("Type ", type,
                                          " is undefined. The binding is: '",
                                          binding.dump(), "'"));
  }
  return tc;
}

}  // namespace sparqlJsonBindingUtils

#endif  // QLEVER_SRC_UTIL_SPARQLJSONBINDINGUTILS_H
