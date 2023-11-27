// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (kalmbach@cs.uni-freiburg.de)

#ifndef QLEVER_VALUESIZEGETTERS_H
#define QLEVER_VALUESIZEGETTERS_H

#include <util/MemorySize/MemorySize.h>

#include <string>

// Callables that determine the actual size (in terms of memory used on stack +
// heap) for various types. They are used to limit the memory consumption of
// QLever, for example in the cache or during the index building.
namespace ad_utility {
/*
Concept for `ValueSizeGetter`. Must be default- initializable, regular invocable
with the given value type as const l-value reference and return a `MemorySize`.
*/
template <typename T, typename ValueType>
concept ValueSizeGetter =
    std::default_initializable<T> &&
    RegularInvocableWithSimilarReturnType<T, MemorySize, const ValueType&>;

// A templated default value size getter. Can be specialized for additional
// custom types.
template <typename T>
struct DefaultValueSizeGetter;

// Trivially copyable types do not own heap memory (otherwise they are
// incorred), so we just use sizeof.
template <typename T>
requires std::is_trivially_copyable_v<T> struct DefaultValueSizeGetter<T> {
  constexpr MemorySize operator()(const T&) const {
    return MemorySize::bytes(sizeof(T));
  }
};

static_assert(ValueSizeGetter<DefaultValueSizeGetter<size_t>, size_t>);

// `std::string` stores additional heap memory which has to be accounted for.
template <>
struct DefaultValueSizeGetter<std::string> {
  MemorySize operator()(const std::string& s) const {
    // Note: This overestimates the size of very small strings for which the
    // small string optimization is used.
    return MemorySize::bytes(sizeof(std::string) + s.size());
  }
};

}  // namespace ad_utility

#endif  // QLEVER_VALUESIZEGETTERS_H
