// Copyright 2023 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_PARSER_IRI_H
#define QLEVER_SRC_PARSER_IRI_H

#include <absl/strings/str_cat.h>

#include <string>
#include <string_view>
#include <type_traits>

#include "backports/StartsWithAndEndsWith.h"
#include "backports/concepts.h"
#include "backports/three_way_comparison.h"
#include "parser/NormalizedString.h"
#include "rdfTypes/RdfEscaping.h"
#include "util/Exception.h"
#include "util/Log.h"

namespace ad_utility::triple_component {

// A class template to hold IRIs. When `isOwning = true` (the default), storage
// is `std::string`. When `isOwning = false`, storage is `std::string_view` and
// all mutating/allocating functions are disabled. Use the `Iri` and `IriView`
// wrapper classes for the concrete owning and non-owning variants.
template <bool isOwning = true>
class BasicIri {
 private:
  using StorageType =
      std::conditional_t<isOwning, std::string, std::string_view>;

  // Store the string value of the IRI including the angle brackets.
  StorageType iri_;

  // Create a new `BasicIri` object.
  explicit BasicIri(StorageType iri);

  // Create a new `BasicIri` using a prefix (owning only).
  CPP_template_2(typename = void)(requires(isOwning))
      BasicIri(const BasicIri<true>& prefix, NormalizedStringView suffix)
      : iri_{absl::StrCat("<", asStringViewUnsafe(prefix.getContent()),
                          asStringViewUnsafe(suffix), ">")} {}

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

  CPP_template_2(typename = void)(requires(isOwning)) std::string
      toStringRepresentation() && {
    return std::move(iri_);
  }

  // Create a new `BasicIri` given an IRI string with brackets.
  CPP_template_2(typename = void)(requires(isOwning)) static BasicIri
      fromIriref(std::string_view stringWithBrackets) {
    auto first = stringWithBrackets.find('<');
    AD_CORRECTNESS_CHECK(first != std::string_view::npos);
    return BasicIri{
        absl::StrCat(stringWithBrackets.substr(0, first + 1),
                     asStringViewUnsafe(RdfEscaping::normalizeIriWithBrackets(
                         stringWithBrackets.substr(first))),
                     ">")};
  }

  // Create a new `BasicIri` given an IRI string without brackets.
  CPP_template_2(typename = void)(requires(isOwning)) static BasicIri
      fromIrirefWithoutBrackets(std::string_view stringWithoutBrackets) {
    AD_CORRECTNESS_CHECK(!ql::starts_with(stringWithoutBrackets, '<') &&
                         !ql::ends_with(stringWithoutBrackets, '>'));
    return BasicIri{absl::StrCat("<", stringWithoutBrackets, ">")};
  }

  // Create a new `BasicIri` given a prefix IRI and its suffix.
  CPP_template_2(typename = void)(requires(isOwning)) static BasicIri
      fromPrefixAndSuffix(const BasicIri& prefix, std::string_view suffix) {
    auto suffixNormalized = RdfEscaping::unescapePrefixedIri(suffix);
    return BasicIri{prefix, asNormalizedStringViewUnsafe(suffixNormalized)};
  }

  // Create a new `BasicIri` object, considering the base IRI. For IRIs with a
  // scheme (like `<http://...>`), this is the same as `fromIriref`. For IRIs
  // without a scheme, prepend the base prefix for relative IRIs (like
  // `<UPI001AF4585D>`) or for absolute IRIs (like `</prosite/PS51927>`).
  CPP_template_2(typename = void)(requires(isOwning)) static BasicIri
      fromIrirefConsiderBase(std::string_view iriStringWithBrackets,
                             const BasicIri& basePrefixForRelativeIris,
                             const BasicIri& basePrefixForAbsoluteIris) {
    auto iriSv = iriStringWithBrackets;
    AD_CORRECTNESS_CHECK(iriSv.size() >= 2);
    AD_CORRECTNESS_CHECK(iriSv[0] == '<' && iriSv[iriSv.size() - 1] == '>');
    if (iriSv.find("://") != std::string_view::npos ||
        basePrefixForAbsoluteIris.empty()) {
      // Case 1: IRI with scheme (like `<http://...>`) or
      // `BASE_IRI_FOR_TESTING` (which is `<@>`, and no valid base IRI has
      // length 3).
      return fromIriref(iriSv);
    } else if (iriSv[1] == '/') {
      // Case 2: Absolute IRI without scheme (like `</prosite/PS51927>`).
      AD_CORRECTNESS_CHECK(!basePrefixForAbsoluteIris.empty());
      return fromPrefixAndSuffix(basePrefixForAbsoluteIris,
                                 iriSv.substr(2, iriSv.size() - 3));
    } else {
      // Case 3: Relative IRI (like `<UPI001AF4585D>`).
      AD_CORRECTNESS_CHECK(!basePrefixForRelativeIris.empty());
      return fromPrefixAndSuffix(basePrefixForRelativeIris,
                                 iriSv.substr(1, iriSv.size() - 2));
    }
  }

  // Get the base IRI from this `BasicIri` object. The returned `BasicIri`
  // always has a `/` at the end. If `domainOnly` is true, remove the path
  // part, for example, for `<http://purl.uniprot.org/uniprot/>` the method
  // returns `<http://purl.uniprot.org/>`.
  CPP_template_2(typename = void)(requires(isOwning)) BasicIri
      getBaseIri(bool domainOnly) const {
    AD_CORRECTNESS_CHECK(ql::starts_with(iri_, '<') && ql::ends_with(iri_, '>'),
                         iri_);
    // Check if we have a scheme and find the first `/` after that (or the
    // first `/` at all if there is no scheme).
    size_t pos = iri_.find(schemePattern);
    if (pos == std::string::npos) {
      AD_LOG_WARN << "No scheme found in base IRI: \"" << iri_ << "\""
                  << " (but we accept it anyway)" << std::endl;
      pos = 1;
    } else {
      pos += schemePattern.size();
    }
    pos = iri_.find('/', pos);
    // Return the IRI with `/` appended in the following two cases: the IRI
    // has the empty path, or `domainOnly` is false and the final `/` is
    // missing.
    if (pos == std::string::npos ||
        (!domainOnly && iri_[iri_.size() - 2] != '/')) {
      return fromIrirefWithoutBrackets(
          absl::StrCat(std::string_view(iri_).substr(1, iri_.size() - 2), "/"));
    }
    // If `domainOnly` is true, remove the path part.
    if (domainOnly) {
      return fromIrirefWithoutBrackets(std::string_view(iri_).substr(1, pos));
    }
    // Otherwise, return the IRI as is.
    return *this;
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

  static Iri fromIriref(std::string_view sv) {
    return BasicIri<true>::fromIriref(sv);
  }
  static Iri fromIrirefWithoutBrackets(std::string_view sv) {
    return BasicIri<true>::fromIrirefWithoutBrackets(sv);
  }
  static Iri fromPrefixAndSuffix(const Iri& p, std::string_view s) {
    return BasicIri<true>::fromPrefixAndSuffix(p, s);
  }
  static Iri fromIrirefConsiderBase(std::string_view sv, const Iri& b1,
                                    const Iri& b2) {
    return BasicIri<true>::fromIrirefConsiderBase(sv, b1, b2);
  }
  static Iri fromStringRepresentation(std::string s) {
    return BasicIri<true>::fromStringRepresentation(std::move(s));
  }
};

// Non-owning IRI view type (stores a `std::string_view`).
class IriView : public BasicIri<false> {
 public:
  using BasicIri<false>::BasicIri;
  IriView(BasicIri<false>&& base) : BasicIri<false>(std::move(base)) {}
  IriView(const BasicIri<false>& base) : BasicIri<false>(base) {}

  static IriView fromStringRepresentation(std::string_view sv) {
    return BasicIri<false>::fromStringRepresentation(sv);
  }
};

}  // namespace ad_utility::triple_component

#endif  // QLEVER_SRC_PARSER_IRI_H
