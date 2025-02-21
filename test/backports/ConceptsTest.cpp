//
// Created by kalmbacj on 2/10/25.
//

#include <gmock/gmock.h>

#include "backports/concepts.h"

template <typename... T>
CPP_concept Something = (... && (sizeof(T) <= 4));

// A templated class with a variety of members.
CPP_template(typename T)(requires(Something<T>)) struct C {
  // A constructor that takes an int only exists for certain `T`
  explicit constexpr CPP_ctor(C)([[maybe_unused]] int i)(
      requires Something<T>) {}

  // A member function that is templated and constrained on an independent
  // type `F`.
  CPP_template_2(typename F)(requires Something<F>) void f(F arg) { (void)arg; }

  // Same, but the constraint combines F and T. Note that you have to use
  // `CPP_and_2` instead of `CPP_and`.
  CPP_template_2(typename F)(
      requires Something<F> CPP_and_2 Something<T>) auto i() {
  }  // Additional template parameter + `auto`

  // Member function with explicit return type that has no other template
  // arguments but poses additional constraints on `T`.
  CPP_member auto g() -> CPP_ret(void)(requires Something<T>) {
  }  // only exists for some `T`

  // Member function with `auto` return type that has no other template
  // arguments but poses additional constraints on `T`.
  CPP_auto_member auto CPP_fun(h)()(requires Something<T>) {}
};

// A variadic function template.  NOTE: you currently have to use plain `&&`,
// the `CPP_and...` macros won't work here.
CPP_variadic_template(typename... Ts)(
    requires(Something<Ts...>&& Something<Ts...>)) void f(Ts...) {}

TEST(ConceptBackports, lambdas) {
  int i = 3;
  auto f = CPP_lambda(i)(auto t)(requires std::is_same_v<decltype(t), int>) {
    return i + t;
  };
  auto g = CPP_template_lambda(&i)(typename T)(T t)(
      requires std::is_same_v<decltype(t), int>) {
    return (i++) + t;
  };

  EXPECT_EQ(g(4), 7);
  EXPECT_EQ(f(5), 8);
  EXPECT_EQ(i, 4);
}
