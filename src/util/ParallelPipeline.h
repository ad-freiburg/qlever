// Copyright 2020, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (joka921) <johannes.kalmbach@gmail.com>

#pragma once

#include <type_traits>

#include "../util/ResourcePool.h"
#include "./Log.h"
#include "./TaskQueue.h"
#include "TypeTraits.h"

namespace ad_pipeline {

template <typename... Ts>
using First = typename std::tuple_element<0, std::tuple<Ts...>>::type;

template <typename... Ts>
using Second = typename std::tuple_element<1, std::tuple<Ts...>>::type;

template <typename T1, typename T2, typename... Ts>
struct TuplePairsT {
  using type = ad_utility::TupleCat<std::tuple<std::function<T2(T1)>>,
                                     typename TuplePairsT<T2, Ts...>::type>;
};

template <typename T1, typename T2>
struct TuplePairsT<T1, T2> {
  using type = std::tuple<std::function<T2(T1)>>;
};

using dispatched = TuplePairsT<int, short, bool>::type;

template <typename F1, typename F2, typename... Fs>
struct Pipeline {
  std::vector<std::unique_ptr<ad_utility::TaskQueue<true>>> _queues;

  Pipeline(bool ordered, std::vector<size_t> t_parallelisms, F1 f1, F2 f2,
           Fs... fs)
      : m_functions{std::move(f1), std::move(f2), std::move(fs)...},
        _ordered(ordered) {
    AD_CHECK(t_parallelisms.size() == numFunctions);
    AD_CHECK(t_parallelisms[0] == 1);
    for (auto p : t_parallelisms) {
      // TODO:: the last one might be 0
      // AD_CHECK(p > 0 || p == numFunctions - 1);
      _queues.push_back(
          std::make_unique<ad_utility::TaskQueue<true>>(2 * p + 1, p));
    }

    for (size_t i = 0; i < t_parallelisms[0]; ++i) {
      _queues[0]->push(makeChainedFunction<0>(i));
    }
    finish();
  }

  auto popManually() { return _queues.back()->popManually(); }

  void finish() {
    for (auto& queue : _queues) {
      static_assert(
          std::is_same_v<dispatched, std::tuple<std::function<short(int)>,
                                                std::function<bool(short)>>>);
      queue->finish();
    }
  }

  template <size_t I, typename... Ts>
  std::function<void(void)> makeChainedFunction(size_t requestId,
                                                Ts&&... input) {
    if constexpr (I == 0) {
      return std::function([&]() {
        size_t requestCounter = 0;
        while (auto optional = get<0>()()) {
          _queues[1]->push(
              makeChainedFunction<1>(requestCounter++, std::move(*optional)));
          _pushedNextOrderedRequest[1].notify_all();
        }
      });
    } else if constexpr (I == numFunctions - 1) {
      return std::function(
          [... input = std::forward<Ts>(input), this]() mutable {
            get<I>()(std::forward<Ts>(input)...);
          });
    } else {
      return std::function(
          [this, requestId, ... input = std::forward<Ts>(input)]() mutable {
            auto result = get<I>()(std::forward<Ts>(input)...);
            if (_ordered) {
              std::unique_lock l{_mutexesForOrdering[I]};
              _pushedNextOrderedRequest[I].wait(l, [requestId, this]() {
                return requestId == _nextRequestId[I];
              });
              _nextRequestId[I]++;
              _queues[I + 1]->push(
                  makeChainedFunction<I + 1>(requestId, std::move(result)));
              _pushedNextOrderedRequest[I + 1].notify_all();
            } else {
              _queues[I + 1]->push(
                  makeChainedFunction<I + 1>(requestId, std::move(result)));
            }
          });
    }
  }

  ~Pipeline() {}

  std::string getTimeStatistics() const {
    std::string res;
    for (const auto& q : _queues) {
      res.append(q->getTimeStatistics());
      res.append("\n\t");
    }
    return res;
  }

  template <size_t I>
  auto& get() {
    return std::get<I>(m_functions);
  }

  using Functions = std::tuple<F1, F2, Fs...>;
  Functions m_functions;
  static constexpr size_t numFunctions = std::tuple_size_v<Functions>;
  std::array<std::condition_variable, numFunctions> _pushedNextOrderedRequest;
  std::array<std::mutex, numFunctions> _mutexesForOrdering;
  std::array<std::size_t, numFunctions> _nextRequestId{};

 public:
  bool _ordered = false;
};

}  // namespace ad_pipeline
