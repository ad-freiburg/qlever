//
// Created by kalmbacj on 2/10/25.
//

#include "backports/concepts.h"

template <typename... T>
CPP_concept Something = (... && (sizeof(T) <= 4));

// Those are rewritten as follows:
CPP_template(typename T)(requires(Something<T>)) struct C {
  // A constructor that takes an int only exists for certain `T`
  explicit constexpr CPP_ctor(C)(int i)(requires Something<T>) {}

  // A member function that is templated and constrained on an independent
  // type `F`.
  CPP_template_2(typename F)(requires Something<F>) void f(F arg) { (void)arg; }

  // Same, but the constraint combines F and T.
  CPP_template_2(typename F)(requires(Something<F, T>)) auto i() {
  }  // Additional template parameter + `auto`

  // Member function with explicit return type that has no other template
  // arguments but poses additional constraints on `T`.
  CPP_member auto g() -> CPP_ret(void)(requires Something<T>) {
  }  // only exists for some `T`

  // Member function with `auto` return type that has no other template
  // arguments but poses additional constraints on `T`.
  CPP_auto_member auto CPP_fun(h)()(requires Something<T>) {}
};
