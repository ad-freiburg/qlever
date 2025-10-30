// Copyright 2025 The QLever Authors, in particular:
//
// 2025, NN, BMW
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
// BMW = Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#ifndef CPPCORO_GENERATOR_CONVERTER_HPP_INCLUDED
#define CPPCORO_GENERATOR_CONVERTER_HPP_INCLUDED

#include "util/Generator.h"
#include "util/Iterators.h"

namespace cppcoro {

// helper function to convert ad_utility::InputRangeTypeErased<T,D> to
// cppcoro::generator<T,D>, details type is copied if necessary
template <typename T, typename D>
generator<T, D> fromInputRange(ad_utility::InputRangeTypeErased<T, D> range) {
  if constexpr (!std::is_same_v<D, ad_utility::NoDetails>) {
    co_await cppcoro::SetDetailsPointer<D>{&range.details()};
  }

  for (auto& value : range) {
    co_yield value;
  }

  // The range is about to be destroyed, copy the details.
  if constexpr (!std::is_same_v<D, ad_utility::NoDetails>) {
    co_await cppcoro::SetDetails<std::decay_t<decltype(range.details())>>{
        std::move(range.details())};
  }
}
}  // namespace cppcoro

#endif
