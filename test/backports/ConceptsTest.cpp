//
// Created by kalmbacj on 2/10/25.
//

#include "backports/concepts.h"

template <typename... T>
CPP_concept Something = (... && (sizeof(T) <= 4));

// Those are rewritten as follows:
CPP_template(typename T)(requires(Something<T>)) class C {
  CPP_template_2(typename F)(requires Something<F>)

      CPP_member auto g() -> CPP_ret(void)(requires Something<T>) {
  }  // only exists for some `T`

  CPP_auto_member auto CPP_fun(h)()(requires Something<T>) {
  }  // additionally `auto` returned.

  CPP_template_2(typename F)(requires(Something<F, T>)) auto i() {
  }  // Additional template parameter + `auto`
};
