// Copyright 2023 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_PARSER_IRI_H
#define QLEVER_SRC_PARSER_IRI_H

#include <string_view>

#include "backports/concepts.h"
#include "parser/NormalizedString.h"

namespace ad_utility::triple_component {

// A class to hold IRIs.
class Iri {
 private:
  // Store the string value of the IRI including the angle brackets.
  std::string iri_;

  // Create a new `Iri` object
  explicit Iri(std::string iri);

  // Create a new `Iri` using a prefix
  Iri(const Iri& prefix, NormalizedStringView suffix);

  // Pattern used to identify the scheme in an IRI. Note that we do not
  // check the validity of the part before the `://` according to RFC 3987.
  static constexpr std::string_view schemePattern = "://";

 public:
  // A default constructed IRI is empty.
  Iri() = default;
  CPP_template(typename H,
               typename I)(requires ql::concepts::same_as<I, Iri>) friend H
      AbslHashValue(H h, const I& iri) {
    return H::combine(std::move(h), iri.iri_);
  }
  bool operator==(const Iri&) const = default;
  static Iri fromStringRepresentation(std::string s);

  const std::string& toStringRepresentation() const;
  std::string& toStringRepresentation();

  // Create a new `Ìri` given an IRI string with brackets.
  static Iri fromIriref(std::string_view stringWithBrackets);

  // Create a new `Iri` given an IRI string without brackets.
  static Iri fromIrirefWithoutBrackets(std::string_view stringWithoutBrackets);

  // Create a new `Iri` given a prefix IRI and its suffix
  static Iri fromPrefixAndSuffix(const Iri& prefix, std::string_view suffix);

  // Create a new `Iri` object, considering the base IRI. For IRIs with a scheme
  // (like `<http://...>`), this is the same as `fromIriref`. For IRIs without a
  // scheme, prepend the base prefix for relative IRIs (like `<UPI001AF4585D>`)
  // or for absolute IRIs (like `</prosite/PS51927>`).
  static Iri fromIrirefConsiderBase(std::string_view iriStringWithBrackets,
                                    const Iri& basePrefixForRelativeIris,
                                    const Iri& basePrefixForAbsoluteIris);

  // Get the base IRI from this `Iri` object. The returned `Iri` always has a
  // `/` at the end. If `domainOnly` is true, remove the path part, for
  // example, for `<http://purl.uniprot.org/uniprot/>` the method returns
  // `<http://purl.uniprot.org/>`.
  Iri getBaseIri(bool domainOnly) const;

  // Return true iff the IRI is empty.
  bool empty() const { return iri_.empty(); }

  // Return the string value of the iri object without any leading or trailing
  // angled brackets.
  NormalizedStringView getContent() const;
};

}  // namespace ad_utility::triple_component

#endif  // QLEVER_SRC_PARSER_IRI_H
