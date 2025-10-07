//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_TEST_UTIL_TRIPLECOMPONENTTESTHELPERS_H
#define QLEVER_TEST_UTIL_TRIPLECOMPONENTTESTHELPERS_H

#include "backports/StartsWithAndEndsWith.h"
#include "parser/TripleComponent.h"

namespace ad_utility::testing {

// Create a valid `TripleComponent::Literal` that can then be stored in a
// `TripleComponent`. The contents of the literal are obtained by normalizing
// `literal` (which must be enclosed in double quotes) and the optional
// `langtagOrDatatype` (which must start with `@` or `^^`).
constexpr auto tripleComponentLiteral =
    [](std::string_view literal, std::string_view langtagOrDatatype = "") {
      std::string dummy;
      if (!(ql::starts_with(literal, '"') && ql::ends_with(literal, '"'))) {
        dummy = absl::StrCat("\"", literal, "\"");
        literal = dummy;
      }
      if (ql::starts_with(langtagOrDatatype, "@")) {
        return TripleComponent::Literal::fromEscapedRdfLiteral(
            literal, std::string(langtagOrDatatype));
      } else if (ql::starts_with(langtagOrDatatype, "^^")) {
        auto iri = ad_utility::triple_component::Iri::fromIriref(
            langtagOrDatatype.substr(2));
        return TripleComponent::Literal::fromEscapedRdfLiteral(literal,
                                                               std::move(iri));
      } else {
        AD_CONTRACT_CHECK(langtagOrDatatype.empty());
        return TripleComponent::Literal::fromEscapedRdfLiteral(literal);
      }
    };

// Create a `TripleComponent` that stores an `Iri` from the given `<iriref>`
constexpr auto iri = [](std::string_view s) {
  return TripleComponent::Iri::fromIriref(s);
};
}  // namespace ad_utility::testing

#endif  // QLEVER_TEST_UTIL_TRIPLECOMPONENTTESTHELPERS_H
