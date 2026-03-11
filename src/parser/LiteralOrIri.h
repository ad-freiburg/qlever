// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#ifndef QLEVER_SRC_PARSER_LITERALORIRI_H
#define QLEVER_SRC_PARSER_LITERALORIRI_H

#include <string>
#include <string_view>
#include <type_traits>
#include <variant>

#include "backports/three_way_comparison.h"
#include "rdfTypes/Iri.h"
#include "rdfTypes/Literal.h"
#include "util/Forward.h"

namespace ad_utility::triple_component {
static constexpr char literalPrefixChar = '"';
static constexpr char iriPrefixChar = '<';
static constexpr std::string_view iriPrefix{&iriPrefixChar, 1};
static constexpr std::string_view literalPrefix{&literalPrefixChar, 1};

// A class template that can contain either a `BasicIri` or a `BasicLiteral`.
// When `isOwning = true` (the default), stores owning data. When
// `isOwning = false`, stores non-owning views. Use the `LiteralOrIri` and
// `LiteralOrIriView` wrapper classes for the concrete owning and non-owning
// variants.
template <bool isOwning = true>
class alignas(16) BasicLiteralOrIri {
 protected:
  using StorageType =
      std::conditional_t<isOwning, std::string, std::string_view>;
  using IriT = std::conditional_t<isOwning, Iri, IriView>;
  using LiteralT = std::conditional_t<isOwning, Literal, LiteralView>;
  using LiteralOrIriVariant = std::variant<IriT, LiteralT>;
  LiteralOrIriVariant data_;

 private:
  static constexpr auto toStringRepresentationImpl =
      [](auto&& val) -> decltype(auto) {
    return AD_FWD(val).toStringRepresentation();
  };

 public:
  // Return contained Iri object if available, throw exception otherwise.
  const IriT& getIri() const;
  IriT& getIri() {
    return const_cast<IriT&>(
        static_cast<const BasicLiteralOrIri*>(this)->getIri());
  }

  // Return contained Literal object if available, throw exception otherwise.
  const LiteralT& getLiteral() const;
  LiteralT& getLiteral() {
    return const_cast<LiteralT&>(
        static_cast<const BasicLiteralOrIri*>(this)->getLiteral());
  }

  // Create a new BasicLiteralOrIri based on a BasicLiteral object.
  explicit BasicLiteralOrIri(LiteralT literal) : data_{std::move(literal)} {}

  // Create a new BasicLiteralOrIri based on a BasicIri object.
  explicit BasicLiteralOrIri(IriT iri) : data_{std::move(iri)} {}

  std::conditional_t<isOwning, const std::string&, std::string_view>
  toStringRepresentation() const& {
    return std::visit(toStringRepresentationImpl, data_);
  }

  static BasicLiteralOrIri fromStringRepresentation(StorageType internal) {
    char tag = internal.front();
    if (tag == literalPrefixChar) {
      return BasicLiteralOrIri{
          LiteralT::fromStringRepresentation(std::move(internal))};
    } else {
      return BasicLiteralOrIri{
          IriT::fromStringRepresentation(std::move(internal))};
    }
  }

  decltype(auto) toStringRepresentation() && {
    return std::visit(
        [](auto&& val) -> decltype(auto) {
          return std::move(val).toStringRepresentation();
        },
        std::move(data_));
  }

  CPP_template(typename H, typename L)(
      requires ql::concepts::same_as<L, BasicLiteralOrIri>) friend H
      AbslHashValue(H h, const L& literalOrIri) {
    return H::combine(std::move(h), literalOrIri.data_);
  }

  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(BasicLiteralOrIri, data_)

  ql::strong_ordering compareThreeWay(const BasicLiteralOrIri& rhs) const;
  QL_DEFINE_CUSTOM_THREEWAY_OPERATOR_LOCAL(BasicLiteralOrIri)

  // Return true if object contains an Iri object.
  bool isIri() const;

  // Return iri content of contained Iri object without any leading or trailing
  // angled brackets. Throws Exception if object does not contain an Iri object.
  NormalizedStringView getIriContent() const;

  // Return true if object contains a Literal object.
  bool isLiteral() const;

  // Return true if contained Literal object has a language tag, throw
  // exception if no Literal object is contained.
  bool hasLanguageTag() const;

  // Return true if contained Literal object has a datatype, throw
  // exception if no Literal object is contained.
  bool hasDatatype() const;

  // Return content of contained Literal as string without leading or trailing
  // quotation marks. Throw exception if no Literal object is contained.
  NormalizedStringView getLiteralContent() const;

  // Return the language tag of the contained Literal without leading @
  // character. Throw exception if no Literal object is contained or object has
  // no language tag.
  NormalizedStringView getLanguageTag() const;

  // Return the datatype of the contained Literal without "^^" prefix.
  // Throw exception if no Literal object is contained or object has no
  // datatype.
  NormalizedStringView getDatatype() const;

  // Return the content of the contained Iri, or the contained Literal.
  NormalizedStringView getContent() const;

  // Printing for GTest.
  friend void PrintTo(const BasicLiteralOrIri& literalOrIri, std::ostream* os) {
    auto& s = *os;
    s << literalOrIri.toStringRepresentation();
  }
};

// Owning LiteralOrIri type (stores its own strings).
class LiteralOrIri : public BasicLiteralOrIri<true> {
 public:
  using Base = BasicLiteralOrIri<true>;
  using Base::Base;
  LiteralOrIri(Base&& base) : Base(std::move(base)) {}
  LiteralOrIri(const Base& base) : Base(base) {}

  template <typename H>
  friend H AbslHashValue(H h, const LiteralOrIri& v) {
    return AbslHashValue(std::move(h), static_cast<const Base&>(v));
  }

  static LiteralOrIri fromStringRepresentation(std::string internal);

  // Create a new Literal with optional datatype or language tag.
  static LiteralOrIri literalWithQuotes(
      std::string_view rdfContentWithQuotes,
      std::optional<std::variant<Iri, std::string>> descriptor = std::nullopt);

  // Similar to `literalWithQuotes`, except the rdfContent is expected to
  // NOT BE surrounded by quotation marks.
  static LiteralOrIri literalWithoutQuotes(
      std::string_view rdfContentWithoutQuotes,
      std::optional<std::variant<Iri, std::string>> descriptor = std::nullopt);

  // Create a new iri given an iri with surrounding brackets.
  static LiteralOrIri iriref(const std::string& stringWithBrackets);

  // Create a new iri given a prefix iri and its suffix.
  static LiteralOrIri prefixedIri(const Iri& prefix, std::string_view suffix);
};

// Non-owning LiteralOrIri view type (stores string_views).
class LiteralOrIriView : public BasicLiteralOrIri<false> {
 public:
  using Base = BasicLiteralOrIri<false>;
  using Base::Base;
  LiteralOrIriView(Base&& base) : Base(std::move(base)) {}
  LiteralOrIriView(const Base& base) : Base(base) {}

  template <typename H>
  friend H AbslHashValue(H h, const LiteralOrIriView& v) {
    return AbslHashValue(std::move(h), static_cast<const Base&>(v));
  }

  static LiteralOrIriView fromStringRepresentation(std::string_view sv) {
    return Base::fromStringRepresentation(sv);
  }
};

}  // namespace ad_utility::triple_component

#endif  // QLEVER_SRC_PARSER_LITERALORIRI_H
