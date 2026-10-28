// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "parser/data/GraphTerm.h"

#include "backports/StartsWithAndEndsWith.h"
#include "parser/RdfParser.h"
#include "parser/TokenizerCtre.h"

// _____________________________________________________________________________
TripleComponent GraphTerm::toTripleComponent() const {
  return visit([](const auto& element) -> TripleComponent {
    using T = std::decay_t<decltype(element)>;
    if constexpr (ad_utility::SameAsAny<T, Variable, Iri>) {
      return element;
    } else if constexpr (std::is_same_v<T, Literal>) {
      return RdfStringParser<TurtleParser<TokenizerCtre>>::parseTripleObject(
          element.toSparql());
    } else {
      static_assert(std::is_same_v<T, BlankNode>);
      const auto& blankNode = element.toSparql();
      AD_CORRECTNESS_CHECK(ql::starts_with(blankNode, "_:"));
      return Variable{absl::StrCat(QLEVER_INTERNAL_BLANKNODE_VARIABLE_PREFIX,
                                   blankNode.substr(2))};
    }
  });
}
