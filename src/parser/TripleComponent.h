// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <johannes.kalmbach@gmail.com>

#ifndef QLEVER_TRIPLECOMPONENT_H
#define QLEVER_TRIPLECOMPONENT_H

#include <absl/strings/str_cat.h>

#include <cstdint>
#include <string>
#include <variant>

#include "../global/Constants.h"
#include "../global/Id.h"
#include "../util/Exception.h"
#include "../util/Forward.h"

/// A wrapper around a `std::variant` that can hold the different types that the
/// subject, predicate, or object of a triple can have in the Turtle Parser.
/// Those currently are `double` (xsd:double and xsd:decimal), `int64_t`
/// (xsd:int and xsd:integer) and `std::string` (variables, IRIs, and literals
/// of any other type).
class TripleComponent {
 private:
  // The underlying variant type.
  using Variant = std::variant<std::string, double, int64_t>;
  Variant _variant;

 public:
  /// Construct from anything that is able to construct the underlying
  /// `Variant`.
  template <typename... Args>
  requires std::is_constructible_v<Variant, Args&&...> TripleComponent(
      Args&&... args)
      : _variant(AD_FWD(args)...) {}

  /// Construct from `string_view`s. We need to explicitly implement this
  /// constructor because  `string_views` are not implicitly convertible to
  /// `std::string`. Note that this constructor is deliberately NOT explicit.
  TripleComponent(std::string_view sv) : _variant{std::string{sv}} {}

  /// Defaulted copy and move constructors.
  TripleComponent(const TripleComponent&) = default;
  TripleComponent(TripleComponent&&) noexcept = default;

  /// Assignment for types that can be directly assigned to the underlying
  /// variant.
  template <typename T>
  requires requires(Variant v, T&& t) {
    _variant = t;
  }
  TripleComponent& operator=(T&& value) {
    _variant = AD_FWD(value);
    return *this;
  }

  /// Assign a `std::string` to the variant that is constructed from `value`.
  TripleComponent& operator=(std::string_view value) {
    _variant = std::string{value};
    return *this;
  }

  /// Defaulted copy and move assignment.
  TripleComponent& operator=(const TripleComponent&) = default;
  TripleComponent& operator=(TripleComponent&&) = default;

  /// Make a `TripleComponent` directly comparable to the underlying types.
  template <typename T>
  requires requires(T&& t) {
    _variant == t;
  }
  bool operator==(const T& other) const { return _variant == other; }

  /// Equality comparison between two `TripleComponent`s.
  bool operator==(const TripleComponent&) const = default;

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
    return isString() && getString().starts_with('?');
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
      AD_CHECK(isInt());
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
      } else {
        static_assert(ad_utility::alwaysFalse<T>);
      }
    };
    return std::visit(visitor, _variant);
  }

  /// Convert the `TripleComponent` to an ID. If the `TripleComponent` is a
  /// string, the IDs are resolved using the `vocabulary`. If a string is not
  /// found in the vocabulary, `std::nullopt` is returned.
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

  /// Human readable output. Is used for debugging, testing, and for the
  /// creation of descriptors and cache keys.
  friend std::ostream& operator<<(std::ostream& stream,
                                  const TripleComponent& obj) {
    std::visit([&stream](const auto& value) -> void { stream << value; },
               obj._variant);
    return stream;
  }

  [[nodiscard]] std::string toString() const {
    std::stringstream stream;
    stream << *this;
    return std::move(stream).str();
  }
};

#endif  // QLEVER_TRIPLECOMPONENT_H
