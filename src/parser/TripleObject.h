// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <johannes.kalmbach@gmail.com>

#include <variant>
#include <string>
#include <cstdint>
#include <absl/strings/str_cat.h>
#include "../util/Forward.h"
#include "../global/Constants.h"
#include "../util/Exception.h"
struct TripleObject {
  using Variant = std::variant<std::string, double, int64_t>;

  Variant _variant;

  // Construct from anything that is able to construct the underlying `Variant`
  template<typename... Args> requires std::is_constructible_v<Variant, Args&&...>
  TripleObject(Args&&... args) : _variant(AD_FWD(args)...) {}

  TripleObject(std::string_view sv): _variant{std::string{sv}} {}
  TripleObject(const TripleObject&) = default;
  TripleObject(TripleObject&&) noexcept = default;

  template <typename T>
  requires requires(Variant v, T&& t) {
    _variant = t;
  }
  TripleObject& operator=(T&& value)  {
    _variant = AD_FWD(value);
    return *this;
  }
  TripleObject& operator=(std::string_view value) {
    _variant = std::string{value};
    return *this;
  }
  TripleObject& operator=(const TripleObject&) = default;
  TripleObject& operator=(TripleObject&&) = default;

  template <typename T>
  requires requires(T&& t) {
    _variant == t;
  }
  bool operator==(const T& other) const { return _variant == other; }

  bool operator==(const TripleObject&) const = default;

  Variant& variant() { return _variant; }
  [[nodiscard]] bool isString() const noexcept {
    return std::holds_alternative<std::string>(_variant);
  }
  [[nodiscard]] bool isDouble() const noexcept {
    return std::holds_alternative<double>(_variant);
  }
  [[nodiscard]] bool isInt() const noexcept {
    return std::holds_alternative<int64_t>(_variant);
  }

  // TODO<C++23> Deducing this.
  std::string& getString() { return std::get<std::string>(_variant); }
  [[nodiscard]] const std::string& getString() const {
    return std::get<std::string>(_variant);
  }
  [[nodiscard]] decltype(auto) getDouble() const { return std::get<double>(_variant); }
  [[nodiscard]] decltype(auto) getInt() const { return std::get<int64_t>(_variant); }

  std::string toRdf() {
    if (isString()) {
      return getString();
    }
    if (isDouble()) {
      return absl::StrCat("\"", getDouble(), "\"^^<", XSD_DOUBLE_TYPE, ">");
    } else {
      AD_CHECK(isInt());
      return absl::StrCat("\"", getInt(), "\"^^<", XSD_INT_TYPE, ">");
    }
  }
  friend std::ostream& operator<<(std::ostream& stream, const TripleObject& obj) {
    if (obj.isString()) {
      stream << "string: " << obj.getString();
    } else if (obj.isInt()) {
      stream << "int: " << obj.getInt();
    } else if (obj.isDouble()) {
      stream << "double: " << obj.getDouble();
    }
    else {
      AD_CHECK(false);
    }
    return stream;
  }


};

#ifndef QLEVER_TRIPLEOBJECT_H
#define QLEVER_TRIPLEOBJECT_H

#endif  // QLEVER_TRIPLEOBJECT_H
