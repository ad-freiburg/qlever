// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Pascal Keßler <kesslerp@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_SERIALIZER_SERIALIZEPARALLELARRAYS_H
#define QLEVER_SRC_UTIL_SERIALIZER_SERIALIZEPARALLELARRAYS_H

#include <array>
#include <cstddef>
#include <tuple>
#include <type_traits>
#include <utility>

#include "backports/algorithm.h"
#include "backports/concepts.h"
#include "backports/span.h"
#include "util/Exception.h"
#include "util/Forward.h"
#include "util/TupleForEach.h"
#include "util/TypeTraits.h"
#include "util/serializer/SerializeVector.h"
#include "util/serializer/Serializer.h"

namespace ad_utility::serialization {

namespace detail {
// An array whose size can be changed, like a `std::vector`.
template <typename T>
CPP_requires(resizableRequires, requires(T& t, size_t size)(t.resize(size)));
template <typename T>
CPP_concept Resizable = CPP_requires_ref(resizableRequires, T);
}  // namespace detail

// Contiguous arrays of the same size whose elements belong together by their
// index, for example the payloads and the datatypes of a column. They are
// serialized with their common size only once, followed by the elements of
// each array like for a `ql::span`. When reading, an array that can be resized
// (like a `std::vector`) gets the size that is read, the others (like a
// `ql::span`) must already have it. Arrays that are passed as lvalues are held
// by reference, temporary ones by value.
template <typename... Arrays>
struct ParallelArrays {
  std::tuple<Arrays...> arrays_;

  explicit ParallelArrays(Arrays&&... arrays)
      : arrays_{std::forward<Arrays>(arrays)...} {}
};
template <typename... Arrays>
ParallelArrays(Arrays&&...) -> ParallelArrays<Arrays...>;

AD_SERIALIZE_FUNCTION_WITH_CONSTRAINT(
    (ad_utility::similarToInstantiation<T, ParallelArrays>)) {
  // The value is ignored when reading.
  size_t size = ql::ranges::size(std::get<0>(arg.arrays_));
  serializer | size;

  if constexpr (ReadSerializer<S>) {
    ad_utility::forEachInTuple(arg.arrays_, [&serializer, size](auto& array) {
      using Array = std::decay_t<decltype(array)>;
      if constexpr (detail::Resizable<Array>) {
        checkAllocation(serializer, size,
                        sizeof(ql::ranges::range_value_t<Array>));
        array.resize(size);
      }
    });
  }
  auto sizes = ad_utility::tupleToArray(arg.arrays_, [](const auto& array) {
    return static_cast<size_t>(ql::ranges::size(array));
  });
  AD_CONTRACT_CHECK(
      ql::ranges::all_of(sizes, [size](size_t s) { return s == size; }),
      "All the arrays of `ParallelArrays` must have the same size. When "
      "reading, the arrays that cannot be resized must already have the size "
      "that is read.");
  ad_utility::forEachInTuple(arg.arrays_, [&serializer](auto& array) {
    detail::serializeElements(serializer, array);
  });
}

// Read `ParallelArrays` of trivially copyable values of the types `Ts` without
// copying them, as spans into the buffer of the `serializer` (like
// `zeroCopyDeserializeToSpan`).
CPP_template(typename... Ts, typename S)(
    requires ZeroCopyReadSerializer<S> && (TriviallySerializable<Ts> && ...))
std::tuple<ql::span<const Ts>...> zeroCopyDeserializeParallelArrays(
    S& serializer) {
  std::size_t size;
  serializer >> size;
  // The elements of a braced initializer list are evaluated in order, so the
  // arrays are read in the order in which they were written.
  return std::tuple<ql::span<const Ts>...>{
      detail::zeroCopyDeserializeElements<Ts>(serializer, size)...};
}

}  // namespace ad_utility::serialization

#endif  // QLEVER_SRC_UTIL_SERIALIZER_SERIALIZEPARALLELARRAYS_H
