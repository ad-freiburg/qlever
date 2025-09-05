///////////////////////////////////////////////////////////////////////////////
// Copyright (c) Lewis Baker, Johannes Kalmbach (functionality to add details).
// Licenced under MIT license. See LICENSE.txt for details.
///////////////////////////////////////////////////////////////////////////////
#ifndef CPPCORO_GENERATOR_CONVERTER_HPP_INCLUDED
#define CPPCORO_GENERATOR_CONVERTER_HPP_INCLUDED

#include "util/Generator.h"

namespace cppcoro {

// helper function to convert ad_utility::InputRangeTypeErased<T,D> to
// cppcoro::generator<T,D>, details type is copied if neccessarry
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
