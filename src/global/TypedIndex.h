//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include <ostream>

#include "util/ConstexprSmallString.h"

namespace ad_utility {
using IndexTag = ConstexprSmallString<30>;
// A strong Index type that internally stores a value of `Type` but can only be
// explicitly converted to and from the underlying `Value`
template <typename Type, IndexTag tag>
struct TypedIndex {
 private:
  Type _value;

 public:
  static constexpr TypedIndex make(Type id) noexcept { return {id}; }
  [[nodiscard]] constexpr const Type& get() const noexcept { return _value; }
  constexpr Type& get() noexcept { return _value; }

  constexpr TypedIndex() = default;

  constexpr bool operator==(const TypedIndex&) const = default;
  constexpr auto operator<=>(const TypedIndex&) const = default;

  static constexpr TypedIndex max() {
    return {std::numeric_limits<Type>::max()};
  }
  static constexpr TypedIndex min() {
    return {std::numeric_limits<Type>::min()};
  }

  [[nodiscard]] constexpr TypedIndex decremented() const {
    return {_value - 1};
  }

  [[nodiscard]] constexpr TypedIndex incremented() const {
    return {_value + 1};
  }

  template <typename H>
  friend H AbslHashValue(H h, const TypedIndex& id) {
    return H::combine(std::move(h), id.get());
  }

  template <typename Serializer>
  friend void serialize(Serializer& serializer, TypedIndex& id) {
    serializer | id._value;
  }

  // This is only used in debug and test code.
  friend std::ostream& operator<<(std::ostream& ostr, const TypedIndex& id) {
    ostr << tag << ':' << id.get();
    return ostr;
  }

 private:
  constexpr TypedIndex(Type value) noexcept : _value{value} {}
};
}  // namespace ad_utility
