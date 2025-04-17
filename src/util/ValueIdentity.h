// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#ifndef QLEVER_SRC_UTIL_VALUEIDENTITY_H
#define QLEVER_SRC_UTIL_VALUEIDENTITY_H

// The use case of the following aliases and constants is as follows:
// Locally (inside a function) put a `using namespace
// ad_utility::use_alue_identity`, then use `VI<someType>` as a convenient short
// cut for `std::value_identity<someValue>`. This can be used to make template
// arguments to local lambdas more readable, as C++ doesn't allow for local
// templates. For example:
//
// auto f1 = []<int i>() {/* use i */};
// auto f2 = []<int i>(VI<i>) { /* use i /*};
// f1.template operator()<1>(); // Works but is rather unreadable.
// f2(vi<1>); // Minimal visual overhead for specifying the template parameter.

namespace ad_utility {

namespace use_value_identity {

template <auto V>
struct ValueIdentity {
  static constexpr auto value = V;
};

template <auto V>
using VI = ValueIdentity<V>;

template <auto V>
static constexpr auto vi = VI<V>{};

}  // namespace use_value_identity

// Wrapper for functors, which function is to forward compile-time values
// provided as template parameters to the functor itself as arguments by
// wrapping each of them in a 'ValueIdentity'.
template <typename F>
struct ApplyAsValueIdentity {
  F functor;

  template <auto... Is, typename... Args>
  decltype(auto) operator()(Args&&... args) const {
    using ad_utility::use_value_identity::vi;
    return functor(vi<Is>..., AD_FWD(args)...);
  }
};

template <typename F>
ApplyAsValueIdentity(F&&) -> ApplyAsValueIdentity<F>;

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_VALUEIDENTITY_H
