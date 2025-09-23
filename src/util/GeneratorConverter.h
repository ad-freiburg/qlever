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

namespace cppcoro {

// helper function to convert ad_utility::InputRangeTypeErased<T,D> to
// cppcoro::generator<T,D>, details type is copied if necessary
template <typename T, typename D>
generator<T, D> fromInputRange(ad_utility::InputRangeTypeErased<T, D> range) {
  if constexpr (!std::is_same_v<D, ad_utility::NoDetails>) {
    auto& detailsRef = co_await cppcoro::getDetails;
    range.setDetailsPointer(&detailsRef);
  }

  for (auto& value : range) {
    co_yield value;
  }
}
}  // namespace cppcoro

#endif
