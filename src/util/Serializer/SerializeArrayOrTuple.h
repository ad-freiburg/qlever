// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include <array>
#include <tuple>

#include "util/ConstexprUtils.h"
#include "util/Serializer/Serializer.h"
#include "util/TypeTraits.h"

namespace ad_utility::serialization {

// Allow trivial serialization for `std::array` if the `value_type` is already
// `TriviallySerializable`.
template <ad_utility::SimilarToArray T>
requires TriviallySerializable<typename T::value_type>
std::true_type allowTrivialSerialization(T, auto);

// A helper function to figure out whether all types contained in a tuple are
// trivially seraizliable.
namespace detail {
template <typename T>
consteval bool tupleTriviallySerializableImpl() {
  bool result = true;
  ad_utility::forEachTypeInTemplateType<T>(
      [&result]<typename U>() { result = result && TriviallySerializable<U>; });
  return result;
}
}  // namespace detail

// Serialization function for `std::array` and `std::tuple`.
AD_SERIALIZE_FUNCTION_WITH_CONSTRAINT(
    ((ad_utility::isArray<std::decay_t<T>> ||
      ad_utility::similarToInstantiation<
          T, std::tuple>)&&!TriviallySerializable<T>)) {
  using Arr = std::decay_t<T>;
  // Tuples are not technically not trivially copyable, but for the purpose of
  // serialization we still can safely `memcpy`. Note that for `std::array` the
  // "normal" memcopy default for `TriviallySerializable` types will kick in.
  if constexpr (similarToInstantiation<Arr, std::tuple>) {
    if constexpr (detail::tupleTriviallySerializableImpl<Arr>()) {
      using CharPtr = std::conditional_t<ReadSerializer<S>, char*, const char*>;
      serializer.serializeBytes(reinterpret_cast<CharPtr>(&arg), sizeof(arg));
      return;
    }
  }
  ad_utility::ConstexprForLoop(
      std::make_index_sequence<std::tuple_size_v<Arr>>(),
      [&serializer, &arg]<size_t I> { serializer | std::get<I>(arg); });
}
}  // namespace ad_utility::serialization
