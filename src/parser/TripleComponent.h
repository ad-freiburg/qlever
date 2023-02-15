// Copyright 2018 - 2022, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <johannes.kalmbach@gmail.com>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#pragma once

#include <cstdint>
#include <string>
#include <variant>

#include "engine/LocalVocab.h"
#include "global/Constants.h"
#include "global/Id.h"
#include "parser/data/Variable.h"
#include "util/Exception.h"
#include "util/Forward.h"

/// A wrapper around a `std::variant` that can hold the different types that the
/// subject, predicate, or object of a triple can have in the Turtle Parser.
/// Those currently are `double` (xsd:double and xsd:decimal), `int64_t`
/// (xsd:int and xsd:integer) and `std::string` (variables, IRIs, and literals
/// of any other type).
class TripleComponent {
 private:
  // The underlying variant type.
  using Variant = std::variant<std::string, double, int64_t, Variable>;
  Variant _variant;

 public:
  /// Construct from anything that is able to construct the underlying
  /// `Variant`.
  template <typename... Args>
  requires std::is_constructible_v<Variant, Args&&...> TripleComponent(
      Args&&... args)
      : _variant(AD_FWD(args)...) {
    if (isString()) {
      // Previously we stored variables as strings, so this check is a way
      // to easily track places where this old behavior is accidentally still
      // in place.
      AD_CONTRACT_CHECK(!getString().starts_with("?"));
    }
  }

  /// Construct from `string_view`s. We need to explicitly implement this
  /// constructor because  `string_views` are not implicitly convertible to
  /// `std::string`. Note that this constructor is deliberately NOT explicit.
  TripleComponent(std::string_view sv) : _variant{std::string{sv}} {
    // Previously we stored variables as strings, so this check is a way
    // to easily track places where this old behavior is accidentally still
    // in place.
    AD_CONTRACT_CHECK(!getString().starts_with("?"));
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
    // See the similar check in the constructor for details.
    if (isString()) {
      AD_CONTRACT_CHECK(!getString().starts_with("?"));
    }
    return *this;
  }

  /// Assign a `std::string` to the variant that is constructed from `value`.
  TripleComponent& operator=(std::string_view value) {
    _variant = std::string{value};
    // See the similar check in the constructor for details.
    AD_CONTRACT_CHECK(!value.starts_with("?"));
    return *this;
  }

  /// Defaulted copy and move assignment.
  TripleComponent& operator=(const TripleComponent&) = default;
  TripleComponent& operator=(TripleComponent&&) = default;

  /// Make a `TripleComponent` directly comparable to the underlying types.
  template <typename T>
  requires requires(T&& t) { _variant == t; }
  bool operator==(const T& other) const { return _variant == other; }

  /// Equality comparison between two `TripleComponent`s.
  bool operator==(const TripleComponent&) const = default;

  /// Hash value for `TripleComponent` object.
  template <typename H>
  friend H AbslHashValue(H h, const TripleComponent& tc) {
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

  /// TODO<joka921> This is a hack which has to be replaced once we have a
  /// proper type for a variable.
  [[nodiscard]] bool isVariable() const {
    return std::holds_alternative<Variable>(_variant);
  }

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

  /// Convert to an RDF literal. `std::strings` will be emitted directly,
  /// `int64_t` is converted to a `xsd:integer` literal, and a `double` is
  /// converted to a `xsd:double`.
  [[nodiscard]] std::string toRdfLiteral() const {
    if (isString()) {
      return getString();
    }
    if (isDouble()) {
      return absl::StrCat("\"", getDouble(), "\"^^<", XSD_DOUBLE_TYPE, ">");
    } else {
      AD_CONTRACT_CHECK(isInt());
      return absl::StrCat("\"", getInt(), "\"^^<", XSD_INTEGER_TYPE, ">");
    }
  }

  /// Convert the `TripleComponent` to an ID if it is not a string. In case of a
  /// string return `std::nullopt`. This is used in `toValueId` below and during
  /// the index building when we haven't built the vocabulary yet.
  [[nodiscard]] std::optional<Id> toValueIdIfNotString() const {
    auto visitor = []<typename T>(const T& value) -> std::optional<Id> {
      if constexpr (std::is_same_v<T, std::string>) {
        return std::nullopt;
      } else if constexpr (std::is_same_v<T, int64_t>) {
        return Id::makeFromInt(value);
      } else if constexpr (std::is_same_v<T, double>) {
        return Id::makeFromDouble(value);
      } else if constexpr (std::is_same_v<T, Variable>) {
        // Cannot turn a variable into a ValueId.
        AD_FAIL();
      } else {
        static_assert(ad_utility::alwaysFalse<T>);
      }
    };
    return std::visit(visitor, _variant);
  }

  // Convert the `TripleComponent` to an ID. If the `TripleComponent` is a
  // string, the IDs are resolved using the `vocabulary`. If a string is not
  // found in the vocabulary, `std::nullopt` is returned.
  template <typename Vocabulary>
  [[nodiscard]] std::optional<Id> toValueId(
      const Vocabulary& vocabulary) const {
    if (isString()) {
      VocabIndex idx;
      if (vocabulary.getId(getString(), &idx)) {
        return Id::makeFromVocabIndex(idx);
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
      AD_CORRECTNESS_CHECK(isString());
      // NOTE: There is a `&&` version of `getIndexAndAddIfNotContained`.
      // Otherwise, `newWord` would be copied here despite the `std::move`.
      std::string& newWord = getString();
      id = Id::makeFromLocalVocabIndex(
          localVocab.getIndexAndAddIfNotContained(std::move(newWord)));
    }
    return id.value();
  }

  // Human readable output. Is used for debugging, testing, and for the creation
  // of descriptors and cache keys.
  friend std::ostream& operator<<(std::ostream& stream,
                                  const TripleComponent& obj);

  // Return as string.
  [[nodiscard]] std::string toString() const;
};
