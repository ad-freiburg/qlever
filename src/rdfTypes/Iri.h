// Copyright 2023 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_PARSER_IRI_H
#define QLEVER_SRC_PARSER_IRI_H

#include <string>
#include <string_view>
#include <type_traits>

#include "backports/StartsWithAndEndsWith.h"
#include "backports/concepts.h"
#include "backports/three_way_comparison.h"
#include "parser/NormalizedString.h"
#include "util/Exception.h"

namespace ad_utility::triple_component {

// A class template to hold IRIs. When `isOwning = true` (the default), storage
// is `std::string`. When `isOwning = false`, storage is `std::string_view` and
// all mutating/allocating functions are disabled. Use the `Iri` and `IriView`
// wrapper classes for the concrete owning and non-owning variants.
template <bool isOwning = true>
class BasicIri {
 protected:
  using StorageType =
      std::conditional_t<isOwning, std::string, std::string_view>;

  // Store the string value of the IRI including the angle brackets.
  StorageType iri_;

  // Create a new `BasicIri` object.
  explicit BasicIri(StorageType iri);

  // Pattern used to identify the scheme in an IRI. Note that we do not
  // check the validity of the part before the `://` according to RFC 3987.
  static constexpr std::string_view schemePattern = "://";

 public:
  // A default constructed IRI is empty.
  BasicIri() = default;

  CPP_template(typename H,
               typename I)(requires ql::concepts::same_as<I, BasicIri>) friend H
      AbslHashValue(H h, const I& iri) {
    return H::combine(std::move(h), iri.iri_);
  }

  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(BasicIri, iri_)

  static BasicIri fromStringRepresentation(StorageType s);

  std::conditional_t<isOwning, const std::string&, std::string_view>
  toStringRepresentation() const& {
    return iri_;
  }

  std::string toStringRepresentation() && requires(isOwning) {
    return std::move(iri_);
  }

  // Return true iff the IRI is empty.
  bool empty() const { return iri_.empty(); }

  // Return the string value of the iri object without any leading or trailing
  // angled brackets.
  NormalizedStringView getContent() const;
};

// Owning IRI type (stores its own `std::string`).
class Iri : public BasicIri<true> {
 public:
  using BasicIri<true>::BasicIri;
  Iri(BasicIri<true>&& base) : BasicIri<true>(std::move(base)) {}
  Iri(const BasicIri<true>& base) : BasicIri<true>(base) {}

  using BasicIri<true>::toStringRepresentation;

  template <typename H>
  friend H AbslHashValue(H h, const Iri& iri) {
    return AbslHashValue(std::move(h), static_cast<const BasicIri<true>&>(iri));
  }

  static Iri fromStringRepresentation(std::string s);

  // Create a new `Iri` given an IRI string with brackets.
  static Iri fromIriref(std::string_view stringWithBrackets);

  // Create a new `Iri` given an IRI string without brackets.
  static Iri fromIrirefWithoutBrackets(std::string_view stringWithoutBrackets);

  // Create a new `Iri` given a prefix IRI and its suffix.
  static Iri fromPrefixAndSuffix(const Iri& prefix, std::string_view suffix);

  // Create a new `Iri` object, considering the base IRI. For IRIs with a
  // scheme (like `<http://...>`), this is the same as `fromIriref`. For IRIs
  // without a scheme, prepend the base prefix for relative IRIs (like
  // `<UPI001AF4585D>`) or for absolute IRIs (like `</prosite/PS51927>`).
  static Iri fromIrirefConsiderBase(std::string_view iriStringWithBrackets,
                                    const Iri& basePrefixForRelativeIris,
                                    const Iri& basePrefixForAbsoluteIris);

  // Get the base IRI from this `Iri` object. The returned `Iri`
  // always has a `/` at the end. If `domainOnly` is true, remove the path
  // part, for example, for `<http://purl.uniprot.org/uniprot/>` the method
  // returns `<http://purl.uniprot.org/>`.
  Iri getBaseIri(bool domainOnly) const;
};

// Non-owning IRI view type (stores a `std::string_view`).
class IriView : public BasicIri<false> {
 public:
  using BasicIri<false>::BasicIri;
  IriView(BasicIri<false>&& base) : BasicIri<false>(std::move(base)) {}
  IriView(const BasicIri<false>& base) : BasicIri<false>(base) {}

  template <typename H>
  friend H AbslHashValue(H h, const IriView& iri) {
    return AbslHashValue(std::move(h),
                         static_cast<const BasicIri<false>&>(iri));
  }

  static IriView fromStringRepresentation(std::string_view sv) {
    return BasicIri<false>::fromStringRepresentation(sv);
  }
};

}  // namespace ad_utility::triple_component

#endif  // QLEVER_SRC_PARSER_IRI_H
