// Copyright 2023 - 2026 The QLever Authors, in particular:
//
// 2023 Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>
// 2024 Hannah Bast <bast@cs.uni-freiburg.de>
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_PARSER_IRI_H
#define QLEVER_SRC_PARSER_IRI_H

#include <string>
#include <string_view>
#include <type_traits>

#include "backports/concepts.h"
#include "backports/three_way_comparison.h"
#include "parser/NormalizedString.h"
#include "util/Exception.h"
#include "util/ParsedUri.h"

namespace ad_utility::triple_component {

// A class template to hold IRIs. When `isOwning = true` (the default), storage
// is `std::string`. When `isOwning = false`, storage is `std::string_view` and
// all mutating/allocating functions are disabled. Use the `Iri` and `IriView`
// wrapper classes for the concrete owning and non-owning variants.
//
// The IRI is stored in QLever's internal normalized format: the full IRI
// including the surrounding angle brackets, with all `\u`/`\U` escape
// sequences resolved to their literal UTF-8 characters (see `fromIriref`).
// For example, the IRI written as `<http://x/\u00E9>` in SPARQL is stored
// as `<http://x/é>`.
template <bool isOwning = true>
class BasicIri {
 public:
  using StorageType =
      std::conditional_t<isOwning, std::string, std::string_view>;

  // Pattern used to identify the scheme in an IRI. Note that we do not
  // check the validity of the part before the `://` according to RFC 3987.
  static constexpr std::string_view schemePattern = "://";

 private:
  // Store the string value of the IRI including the angle brackets.
  StorageType iri_;

 protected:
  // Create a new `BasicIri` object.
  explicit BasicIri(StorageType iri);
  const StorageType& iri() const { return iri_; }

 public:
  // A default constructed IRI is empty.
  BasicIri() = default;

  CPP_template(typename H,
               typename I)(requires ql::concepts::same_as<I, BasicIri>) friend H
      AbslHashValue(H h, const I& iri) {
    return H::combine(std::move(h), iri.iri_);
  }

  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(BasicIri, iri_)

  // Build an `Iri` from a string that is already in QLever's internal
  // normalized format (see the class comment). The string is stored verbatim
  // (no validation, no transformation); this is the exact inverse of
  // `toStringRepresentation`. To build an `Iri` from a raw (possibly escaped)
  // SPARQL/Turtle IRI instead, use `fromIriref`.
  static BasicIri fromStringRepresentation(StorageType s);

  // Return the IRI in QLever's internal normalized format (see the class
  // comment). This is the exact inverse of `fromStringRepresentation`. Code
  // that uses the IRI semantically (e.g. for RDF/SPARQL output) should use
  // `toSparql()` instead.
  std::conditional_t<isOwning, const std::string&, std::string_view>
  toStringRepresentation() const& {
    return iri_;
  }

  std::conditional_t<isOwning, std::string, std::string_view>
  toStringRepresentation() && {
    return std::move(iri_);
  }

  // Return a valid RDF/SPARQL representation of the IRI.
  //
  // NOTE: currently this is equal to the internal string representation. Should
  // this ever change, all call sites that rely on this equality have to be
  // inspected.
  std::conditional_t<isOwning, std::string, std::string_view> toSparql()
      const& {
    return toStringRepresentation();
  }

  std::conditional_t<isOwning, std::string, std::string_view> toSparql() && {
    return std::move(*this).toStringRepresentation();
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

 private:
  explicit Iri(BasicIri<true>&& base) : BasicIri<true>(std::move(base)) {}

 public:
  using BasicIri<true>::toStringRepresentation;

  template <typename H>
  friend H AbslHashValue(H h, const Iri& iri) {
    return H::combine(std::move(h), static_cast<const BasicIri<true>&>(iri));
  }

  static Iri fromStringRepresentation(std::string s);

  // Create a new `Iri` given an IRI string with brackets.
  static Iri fromIriref(std::string_view stringWithBrackets);

  // Like `fromIriref`, but first validate that `stringWithBrackets` is a
  // syntactically valid `IRIREF` and `throw` an `ad_utility::Exception`
  // otherwise. The accepted format is the SPARQL/Turtle `IRIREF` production
  // according to https://www.ietf.org/rfc/rfc3987.txt:
  //
  //   IRIREF ::= '<' ([^<>"{}|^`\] - [#x00-#x20])* '>'
  //
  // which the regex below encodes: a `<`, then zero or more characters that are
  // none of `<>"{}|^`\` and not a control character or space (the `\0- ` range
  // covers all bytes from `#x00` to `#x20` inclusive), then a closing `>`. The
  // body may be empty (`<>` is valid).
  static Iri fromIrirefValidated(std::string_view stringWithBrackets);

  // Create a new `Iri` given an IRI string without brackets.
  static Iri fromIrirefWithoutBrackets(std::string_view stringWithoutBrackets);

  // Create a new `Iri` given a prefix IRI and its suffix.
  static Iri fromPrefixAndSuffix(const Iri& prefix, std::string_view suffix);

  // Create a new `Iri` object, considering the base IRI, following the
  // specification of RFC 3986. For absolute IRIs with a scheme (like
  // `<http://...>`), this is the same as `fromIriref`.
  static Iri fromIrirefConsiderBase(std::string_view iriStringWithBrackets,
                                    const qlever::util::ParsedUri& baseUri);

  // Create an `Iri` object given from the given `ParsedUri` object.
  static Iri fromUri(const qlever::util::ParsedUri& uri);
};

// Non-owning IRI view type (stores a `std::string_view`).
class IriView : public BasicIri<false> {
 public:
  using BasicIri<false>::BasicIri;

 private:
  explicit IriView(BasicIri<false>&& base) : BasicIri<false>(std::move(base)) {}

 public:
  template <typename H>
  friend H AbslHashValue(H h, const IriView& iri) {
    return H::combine(std::move(h), static_cast<const BasicIri<false>&>(iri));
  }

  static IriView fromStringRepresentation(std::string_view sv) {
    return IriView{BasicIri<false>::fromStringRepresentation(sv)};
  }
};

}  // namespace ad_utility::triple_component

#endif  // QLEVER_SRC_PARSER_IRI_H
