// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_UTIL_STRINGPAIRHASHMAP_H_
#define QLEVER_SRC_UTIL_STRINGPAIRHASHMAP_H_

#include "util/HashMap.h"

// This module provides a modified version of `ad_utility::HashMap` that uses
// pairs of strings as keys. Unlike the default hash map it allows looking up
// values with pairs of string views as keys. This is implemented using custom
// transparent hash and equality operators.
//
// NOTE: Since `StringViewPair` does not convert to `StringPair` implicitly,
// insertion can only use `StringPair` where lookup can use both `StringPair`
// and `StringViewPair`.

// TODO<ullingerc> This could be extended to support `std::tuple` or
// `std::array`, not only `std::pair`, and other transparently comparable types.

// _____________________________________________________________________________
namespace ad_utility {

// _____________________________________________________________________________
namespace detail {

using StringPair = std::pair<std::string, std::string>;
using StringViewPair = std::pair<std::string_view, std::string_view>;

static_assert(std::is_convertible_v<StringPair, StringViewPair>);
static_assert(!std::is_convertible_v<StringViewPair, StringPair>);
static_assert(std::is_constructible_v<StringViewPair, StringPair>);

// _____________________________________________________________________________
struct StringPairHash {
  // Allows looking up values from a hash map with `StringPair` keys also with
  // `StringViewPair`.
  using is_transparent = void;

  size_t operator()(const StringPair& p) const {
    return absl::HashOf(p.first, p.second);
  }

  size_t operator()(const StringViewPair& p) const {
    return absl::HashOf(p.first, p.second);
  }
};

// _____________________________________________________________________________
struct StringPairEq {
  using is_transparent = void;

  bool operator()(const StringPair& a, const StringPair& b) const {
    return a == b;
  }

  bool operator()(const StringPair& a, const StringViewPair& b) const {
    return a.first == b.first && a.second == b.second;
  }

  bool operator()(const StringViewPair& a, const StringPair& b) const {
    return b.first == a.first && b.second == a.second;
  }
};

}  // namespace detail

template <typename ValueType>
using StringPairHashMap =
    ad_utility::HashMap<ad_utility::detail::StringPair, ValueType,
                        ad_utility::detail::StringPairHash,
                        ad_utility::detail::StringPairEq>;

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_STRINGPAIRHASHMAP_H_
