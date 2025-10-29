//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_TEST_UTIL_TRIPLECOMPONENTTESTHELPERS_H
#define QLEVER_TEST_UTIL_TRIPLECOMPONENTTESTHELPERS_H

#include "backports/StartsWithAndEndsWith.h"
#include "parser/LiteralOrIri.h"
#include "parser/TripleComponent.h"

// Forward declaration
class IndexImpl;

namespace ad_utility::testing {

// Get a default IndexImpl pointer for tests. This should be set by test
// infrastructure (e.g., from getQec()) before creating LiteralOrIri objects.
inline const IndexImpl*& getTestIndexImpl() {
  static const IndexImpl* testIndex = nullptr;
  return testIndex;
}

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

// Helper to create LiteralOrIri objects in tests with the test index
inline auto literalOrIriForTesting(std::string_view content) {
  using namespace ad_utility::triple_component;
  const IndexImpl* index = getTestIndexImpl();
  AD_CONTRACT_CHECK(index != nullptr,
                    "Test index must be set before creating LiteralOrIri. "
                    "Use getTestIndexImpl() = &index; in test setup.");
  if (ql::starts_with(content, '<')) {
    return LiteralOrIri::iriref(std::string(content), index);
  } else {
    return LiteralOrIri::literalWithoutQuotes(content, index);
  }
}

}  // namespace ad_utility::testing

#endif  // QLEVER_TEST_UTIL_TRIPLECOMPONENTTESTHELPERS_H
