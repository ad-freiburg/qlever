// Copyright 2020, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (joka921) <johannes.kalmbach@gmail.com>

#pragma once

#include "./TaskQueue.h"
#include "TypeTraits.h"
#include <type_traits>

namespace ad_pipeline {

template<typename... Ts>
using First = typename std::tuple_element<0, std::tuple<Ts...>>::type;

template<typename... Ts>
using Second = typename std::tuple_element<1, std::tuple<Ts...>>::type;




template<typename T1, typename T2, typename... Ts>
struct TuplePairsT {
 using type =  ad_utility::TupleCatT<std::tuple<std::function<T2(T1)>>, typename TuplePairsT<T2, Ts...>::type>;
};

template<typename T1, typename T2>
struct TuplePairsT<T1, T2> {
  using type = std::tuple<std::function<T2(T1)>>;
};

using dispatched = TuplePairsT<int, short, bool>::type;





template <typename T1, typename T2, typename... Ts>
struct Pipeline {
  std::vector<ad_utility::TaskQueue<true>> _queues;


  void finish() {
    for (auto& queue : _queues) {
      static_assert(std::is_same_v<dispatched, std::tuple<std::function<short(int)>, std::function<bool(short)>>>);
      queue.finish();
    }
  }

  ~Pipeline() {
    finish();
  }
};




}  // namespace ad_pipeline
