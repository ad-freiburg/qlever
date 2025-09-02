//
// Created by kalmbacj on 9/2/25.
//

// TODO<joka921> Correct header
#ifndef TYPE_TRAITS_H
#define TYPE_TRAITS_H

#include <range/v3/all.hpp>
#include <type_traits>

namespace ql {
template <typename T>
struct type_identity {
  using type = T;
};

template <typename T>
using type_identity_t = typename type_identity<T>::type;

template <typename T>
struct remove_cvref {
  using type = std::remove_cv_t<std::remove_reference_t<T>>;
};

template <typename T>
using remove_cvref_t = typename remove_cvref<T>::type;

using ::ranges::iter_reference_t;
}  // namespace ql

#endif  // TYPE_TRAITS_H
