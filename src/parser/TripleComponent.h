// Copyright 2018 - 2022, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <johannes.kalmbach@gmail.com>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#pragma once

#include <concepts>
#include <cstdint>
#include <string>
#include <type_traits>
#include <variant>

#include "engine/LocalVocab.h"
#include "global/Constants.h"
#include "global/Id.h"
#include "global/SpecialIds.h"
#include "parser/LiteralOrIri.h"
#include "parser/RdfEscaping.h"
#include "parser/data/Variable.h"
#include "util/Date.h"
#include "util/Exception.h"
#include "util/Forward.h"

/// A wrapper around a `std::variant` that can hold the different types that the
/// subject, predicate, or object of a triple can have in the Turtle Parser.
/// Those currently are `double` (xsd:double and xsd:decimal), `int64_t`
/// (xsd:int and xsd:integer) and `std::string` (variables, IRIs, and literals
/// of any other type).
class TripleComponent {
 public:
  using Literal = ad_utility::triple_component::Literal;
  using Iri = ad_utility::triple_component::Iri;
  // Own class for the UNDEF value.
  struct UNDEF {
    // Default equality operator.
    bool operator==(const UNDEF&) const = default;
    // Hash to arbitrary (fixed) value. For example, needed in
    // `Values::computeMultiplicities`.
    template <typename H>
    friend H AbslHashValue(H h, [[maybe_unused]] const UNDEF& undef) {
      return H::combine(std::move(h), 42);
    }
  };

 private:
  // The underlying variant type.
  using Variant = std::variant<Id, std::string, double, int64_t, bool, UNDEF,
                               Variable, Literal, Iri, DateYearOrDuration>;
  Variant _variant;

 public:
  // There are several places during the parsing where an uninitizalized
  // `TripleComponent` is currently used.
  TripleComponent() = default;
  /// Construct from anything that is able to construct the underlying
  /// `Variant`.
  template <typename FirstArg, typename... Args>
  requires(!std::same_as<std::remove_cvref_t<FirstArg>, TripleComponent> &&
           std::is_constructible_v<Variant, FirstArg &&, Args && ...>)
  TripleComponent(FirstArg&& firstArg, Args&&... args)
      : _variant(AD_FWD(firstArg), AD_FWD(args)...) {
    if (isString()) {
      // Storing variables and literals as strings is deprecated. The following
      // checks help find places, where this is accidentally still done.
      AD_CONTRACT_CHECK(!getString().starts_with("?"));
      AD_CONTRACT_CHECK(!getString().starts_with('"'));
      AD_CONTRACT_CHECK(!getString().starts_with("'"));
    }
  }

  /// Construct from `string_view`s. We need to explicitly implement this
  /// constructor because  `string_views` are not implicitly convertible to
  /// `std::string`. Note that this constructor is deliberately NOT explicit.
  TripleComponent(std::string_view sv) : _variant{std::string{sv}} {
    checkThatStringIsValid();
  }

  /// Defaulted copy and move constructors.
  TripleComponent(const TripleComponent&) = default;
  TripleComponent(TripleComponent&&) noexcept = default;

  /// Assignment for types that can be directly assigned to the underlying
  /// variant.
  template <typename T>
  requires requires(Variant v, T&& t) { _variant = t; }
  TripleComponent& operator=(T&& value) {
    _variant = AD_FWD(value);
    checkThatStringIsValid();
    return *this;
  }

  /// Assign a `std::string` to the variant that is constructed from `value`.
  TripleComponent& operator=(std::string_view value) {
    _variant = std::string{value};
    checkThatStringIsValid();
    return *this;
  }

  /// Defaulted copy and move assignment.
  TripleComponent& operator=(const TripleComponent&) = default;
  TripleComponent& operator=(TripleComponent&&) = default;

  /// Make a `TripleComponent` directly comparable to the underlying types.
  template <typename T>
  requires requires(T&& t) { _variant == t; }
  bool operator==(const T& other) const {
    return _variant == other;
  }

  /// Equality comparison between two `TripleComponent`s.
  bool operator==(const TripleComponent&) const = default;

  /// Hash value for `TripleComponent` object.
  /// Note: It is important to use `std::same_as` because otherwise this
  /// overload would also be eligible for the contained types that are
  /// implicitly convertible to `TripleComponent` which would lead to strange
  /// bugs.
  template <typename H>
  friend H AbslHashValue(H h, const std::same_as<TripleComponent> auto& tc) {
    return H::combine(std::move(h), tc._variant);
  }

  /// Check which type the underlying variants hold.
  [[nodiscard]] bool isString() const {
    return std::holds_alternative<std::string>(_variant);
  }
  [[nodiscard]] bool isDouble() const {
    return std::holds_alternative<double>(_variant);
  }
  [[nodiscard]] bool isInt() const {
    return std::holds_alternative<int64_t>(_variant);
  }

  [[nodiscard]] bool isVariable() const {
    return std::holds_alternative<Variable>(_variant);
  }

  bool isLiteral() const { return std::holds_alternative<Literal>(_variant); }
  Literal& getLiteral() { return std::get<Literal>(_variant); }
  const Literal& getLiteral() const { return std::get<Literal>(_variant); }

  bool isIri() const { return std::holds_alternative<Iri>(_variant); }

  Iri& getIri() { return std::get<Iri>(_variant); }
  const Iri& getIri() const { return std::get<Iri>(_variant); }

  bool isUndef() const { return std::holds_alternative<UNDEF>(_variant); }

  /// Access the value. If one of those methods is called but the variant
  /// doesn't hold the correct type, an exception is thrown.
  [[nodiscard]] const std::string& getString() const {
    return std::get<std::string>(_variant);
  }

  // Non-const overload. TODO<C++23> Deducing this.
  std::string& getString() { return std::get<std::string>(_variant); }

  [[nodiscard]] const double& getDouble() const {
    return std::get<double>(_variant);
  }
  [[nodiscard]] const int64_t& getInt() const {
    return std::get<int64_t>(_variant);
  }

  [[nodiscard]] const Variable& getVariable() const {
    return std::get<Variable>(_variant);
  }
  [[nodiscard]] Variable& getVariable() { return std::get<Variable>(_variant); }

  /// Convert to an RDF literal. `std::strings` will be emitted directly,
  /// `int64_t` is converted to a `xsd:integer` literal, and a `double` is
  /// converted to a `xsd:double`.
  // TODO<joka921> This function is used in only few places and  ignores the
  // strong typing of `Literal`s etc. It should be removed and its calls be
  // replaced by calls that work on the strongly typed `TripleComponent`
  // directly.
  [[nodiscard]] std::string toRdfLiteral() const;

  /// Convert the `TripleComponent` to an ID if it is not a string. In case of a
  /// string return `std::nullopt`. This is used in `toValueId` below and during
  /// the index building when we haven't built the vocabulary yet.
  [[nodiscard]] std::optional<Id> toValueIdIfNotString() const;

  // Convert the `TripleComponent` to an ID. If the `TripleComponent` is a
  // string, the IDs are resolved using the `vocabulary`. If a string is not
  // found in the vocabulary, `std::nullopt` is returned.
  template <typename Vocabulary>
  [[nodiscard]] std::optional<Id> toValueId(
      const Vocabulary& vocabulary) const {
    AD_CONTRACT_CHECK(!isString());
    if (isLiteral() || isIri()) {
      VocabIndex idx;
      const std::string& content = isLiteral()
                                       ? getLiteral().toStringRepresentation()
                                       : getIri().toStringRepresentation();
      if (vocabulary.getId(content, &idx)) {
        return Id::makeFromVocabIndex(idx);
      } else if (qlever::specialIds().contains(content)) {
        return qlever::specialIds().at(content);
      } else {
        return std::nullopt;
      }
    } else {
      return toValueIdIfNotString();
    }
  }

  // Same as the above, but also consider the given local vocabulary. If the
  // string is neither in `vocabulary` nor in `localVocab`, it will be added to
  // `localVocab`. Therefore, we get a valid `Id` in any case. The modifier is
  // `&&` because in our uses of this method, the `TripleComponent` object is
  // created solely to call this method and we want to avoid copying the
  // `std::string` when passing it to the local vocabulary.
  template <typename Vocabulary>
  [[nodiscard]] Id toValueId(const Vocabulary& vocabulary,
                             LocalVocab& localVocab) && {
    std::optional<Id> id = toValueId(vocabulary);
    if (!id) {
      // If `toValueId` could not convert to `Id`, we have a string, which we
      // look up in (and potentially add to) our local vocabulary.
      AD_CORRECTNESS_CHECK(isLiteral() || isIri());
      using LiteralOrIri = ad_utility::triple_component::LiteralOrIri;
      auto moveWord = [&]() -> LiteralOrIri {
        if (isLiteral()) {
          return LiteralOrIri{std::move(getLiteral())};
        } else {
          return LiteralOrIri{std::move(getIri())};
        }
      };
      // NOTE: There is a `&&` version of `getIndexAndAddIfNotContained`.
      // Otherwise, `newWord` would be copied here despite the `std::move`.
      id = Id::makeFromLocalVocabIndex(
          localVocab.getIndexAndAddIfNotContained(moveWord()));
    }
    return id.value();
  }

  // Human-readable output. Is used for debugging, testing, and for the creation
  // of descriptors and cache keys.
  friend std::ostream& operator<<(std::ostream& stream,
                                  const TripleComponent& obj);

  // Return as string.
  [[nodiscard]] std::string toString() const;

 private:
  // The `std::string` alternative of the  underlying variant previously
  // was also used for variables and literals, which now have their
  // own alternative. This function checks that a stored `std::string` does not
  // store a literal or a variable.
  // TODO<joka921> In most parts of the code, the `std::string` case only stores
  // IRIs and blank nodes. It would be desirable to check that we are indeed in
  // one of these cases. However, the `TurtleParser` currently uses a
  // `TripleComponent` to store literals like `true`, `false`, `12.3` etc. in a
  // TripleComponent as an intermediate step. Change the turtle parser to make
  // these cases unnecessary.
  void checkThatStringIsValid() {
    if (isString()) {
      const auto& s = getString();
      AD_CONTRACT_CHECK(!s.starts_with('?'));
      AD_CONTRACT_CHECK(!s.starts_with('"'));
      AD_CONTRACT_CHECK(!s.starts_with('\''));
    }
  }
};
