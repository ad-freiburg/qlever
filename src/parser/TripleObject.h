// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <johannes.kalmbach@gmail.com>

#include <absl/strings/str_cat.h>

#include <cstdint>
#include <string>
#include <variant>

#include "../global/Constants.h"
#include "../util/Exception.h"
#include "../util/Forward.h"

/// A wrapper around a `std::variant` that can hold the different types that the
/// object of a triple can have in the Turtle Parser. Those currently are
/// `double` (xsd:double and xsd:decimal), `int64_t` (xsd:int and xsd:integer)
/// and `std::string` (IRIs and literals of any other type).
class TripleObject {
 private:
  // The underlying variant type.
  using Variant = std::variant<std::string, double, int64_t>;
  Variant _variant;

 public:
  /// Construct from anything that is able to construct the underlying
  /// `Variant`.
  template <typename... Args>
  requires std::is_constructible_v<Variant, Args&&...> TripleObject(
      Args&&... args)
      : _variant(AD_FWD(args)...) {}

  /// Construct from `string_view`s. We need to explicitly implement this
  /// constructor because  `string_views` are not implicitly convertible to
  /// `std::string`.
  TripleObject(std::string_view sv) : _variant{std::string{sv}} {}

  /// Defaulted copy and move constructors.
  TripleObject(const TripleObject&) = default;
  TripleObject(TripleObject&&) noexcept = default;

  /// Assignment for types that can be directly assigned to the underlying
  /// variant.
  template <typename T>
  requires requires(Variant v, T&& t) {
    _variant = t;
  }
  TripleObject& operator=(T&& value) {
    _variant = AD_FWD(value);
    return *this;
  }

  /// Assign a `std::string` to the variant that is constructed from `value`.
  TripleObject& operator=(std::string_view value) {
    _variant = std::string{value};
    return *this;
  }

  /// Defaulted copy and move assignment.
  TripleObject& operator=(const TripleObject&) = default;
  TripleObject& operator=(TripleObject&&) = default;

  /// Make a `TripleObject` directly comparable to the underlying types.
  template <typename T>
  requires requires(T&& t) {
    _variant == t;
  }
  bool operator==(const T& other) const { return _variant == other; }

  /// Equality comparison between two `TripleObject`s.
  bool operator==(const TripleObject&) const = default;

  /// Check which type the underlying variants hold.
  [[nodiscard]] bool isString() const noexcept {
    return std::holds_alternative<std::string>(_variant);
  }
  [[nodiscard]] bool isDouble() const noexcept {
    return std::holds_alternative<double>(_variant);
  }
  [[nodiscard]] bool isInt() const noexcept {
    return std::holds_alternative<int64_t>(_variant);
  }

  /// Access the value. If one of those methods is called but the variant
  /// doesn't hold the correct type, an exception is thrown.
  [[nodiscard]] const std::string& getString() const {
    return std::get<std::string>(_variant);
  }
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

  /// Human readable output for debugging and testing.
  friend std::ostream& operator<<(std::ostream& stream,
                                  const TripleObject& obj) {
    if (obj.isString()) {
      stream << "string:\"" << obj.getString() << '"';
    } else if (obj.isInt()) {
      stream << "int:" << obj.getInt();
    } else if (obj.isDouble()) {
      stream << "double:" << obj.getDouble();
    } else {
      AD_CHECK(false);
    }
    return stream;
  }
};

#ifndef QLEVER_TRIPLEOBJECT_H
#define QLEVER_TRIPLEOBJECT_H

#endif  // QLEVER_TRIPLEOBJECT_H
