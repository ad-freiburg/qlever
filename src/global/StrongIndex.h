//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_STRONGINDEX_H
#define QLEVER_STRONGINDEX_H

#include "../util/ConstexprSmallString.h"

namespace ad_utility {
using IndexTag = ConstexprSmallString<30>;
// A strong Index type that internally stores a value of `Type` but can only be
// explicitly converted to and from the underlying `Value`
template <typename Type, IndexTag tag>
struct StrongIndex {
 private:
  Type _data;

 public:
  static StrongIndex make(Type id) noexcept { return {id}; }
  [[nodiscard]] const Type& get() const noexcept { return _data; }
  Type& get() noexcept { return _data; }

  StrongIndex() = default;

  bool operator==(const StrongIndex&) const = default;
  auto operator<=>(const StrongIndex&) const = default;

  static constexpr StrongIndex max() {
    return {std::numeric_limits<Type>::max()};
  }
  static constexpr StrongIndex min() {
    return {std::numeric_limits<Type>::min()};
  }

  StrongIndex& operator++() {
    ++_data;
    return *this;
  }

  StrongIndex operator++(int) {
    StrongIndex copy = *this;
    ++_data;
    return copy;
  }

  StrongIndex& operator--() {
    --_data;
    return *this;
  }

  StrongIndex operator--(int) {
    StrongIndex copy = *this;
    --_data;
    return copy;
  }

  template <typename H>
  friend H AbslHashValue(H h, const StrongIndex& id) {
    return H::combine(std::move(h), id.get());
  }

  template <typename Serializer>
  friend void serialize(Serializer& serializer, StrongIndex& id) {
    serializer | id._data;
  }

  // This is only used in debug and test code.
  friend std::ostream& operator<<(std::ostream& ostr, const StrongIndex& id) {
    ostr << tag << ':' << id.get();
    return ostr;
  }

 private:
  constexpr StrongIndex(Type data) noexcept : _data{data} {}
};
}  // namespace ad_utility

#endif  // QLEVER_STRONGINDEX_H
