// Copyright 2020, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (johannes.kalmbach@gmail.com)

#ifndef QLEVER_TUPLEHELPERS_H
#define QLEVER_TUPLEHELPERS_H

#include <cstddef>
#include <memory>
#include <tuple>
#include <utility>

namespace ad_tuple_helpers {

namespace detail {
/* Implementation of setupTupleFromCallable (see below)
 * Needed because we need the Pattern matching on the index_sequence
 * TODO<joka921> In C++ 20 this could be done in place with templated
 * lambdas
 */
template <class F, size_t... I>
auto setupTupleFromCallableImpl(F&& f, std::index_sequence<I...>) {
  return std::make_tuple(f(I)...);
}
}  // namespace detail

/**
 * @brief create a Tuple with numEntries entries where the i-th entry is created
 * by a call to f(i). Note that the evaluation order of the calls to f is
 * unspecified, so if f has side effects /state you might run into undefined
 * behavior.
 * @tparam numEntries the size of the tuple to be created
 * @tparam F A function object that takes a single size_t as an argument
 * @param f
 * @return std::make_tuple(f(0), f(1), f(2), ... f(numEntries - 1));
 * @todo<joka921> Can we simplify this implementation via fold expressions etc?
 */
template <size_t numEntries, class F>
auto setupTupleFromCallable(F&& f) {
  auto idxSeq = std::make_index_sequence<numEntries>{};
  static_assert(numEntries > 0);
  return detail::setupTupleFromCallableImpl(std::forward<F>(f), idxSeq);
}

/**
 * @brief convert a parameter pack to a tuple of unique_ptrs that are
 * constructed from the elements of the parameter pack. E.g.
 * toUniquePtrTuple(int(3), std::string("foo")) ==
 * std::make_tuple(std::make_unique<int>(3),
 * std::make_unique<std::string>("foo")) It can take an arbitrary positive
 * number of arguments. Currently this template is linearly expanded, if this
 * becomes a bottleneck at compile time (e.g. for very long parameter packs) we
 * could do something more intelligent.
 * @return std::make_tuple(std::make_unique<First>(std::forward<First>(l),
 * std::make_unique<FirstOfRem.....
 */
template <typename... Rest>
auto toUniquePtrTuple(Rest&&... rem) {
  return std::make_tuple(
      std::make_unique<std::decay_t<Rest>>(std::forward<Rest>(rem))...);
}

/// For a pack of types returns the return type of a call to toUniquePtrTuple
/// with arguments of that type
template <typename... ts>
using toUniquePtrTuple_t = decltype(toUniquePtrTuple(std::declval<ts>()...));

/**
 * @brief converts a tuple of smart pointers (or other types that have a .get()
 * method) to a tuple of the corresponding raw pointers (return types of the
 * get() methods). Does NOT change ownership.
 * @tparam Tuple Must have form T<Ts...> where T must support
 * std::get<size_t>(T), and the types Ts contained in T must support a call to a
 * .get() method e.g. std::tuple<std::unique_ptr<int>,
 * std::shared_ptr<std::string>> or std::array<std::shared_ptr<int>, 42>;
 * @param tuple A tuple/array of type Tuple
 * @return std::make_tuple(std::get<0>(tuple).get(), std::get<1>(tuple).get(),
 * ... std::get<std::tuple_size_v<Tuple>>(tuple).get());
 */
template <typename Tuple, bool safe = true>
auto toRawPtrTuple(Tuple&& tuple) {
  if constexpr (safe) {
    static_assert(
        !std::is_rvalue_reference_v<decltype(tuple)>,
        "You can't pass an rvalue reference to toRawPtrTuple, because "
        "the argument preserves the ownership to the pointers");
  }
  auto f = [](auto&&... args) { return std::make_tuple(args.get()...); };
  return std::apply(f, tuple);  // std::forward<Tuple>(tuple));
}

/// The return type of toRawPtrTuple<T>(T&& t);
template <typename T>
using toRawPtrTuple_t = decltype(toRawPtrTuple<T, false>(
    std::declval<T>()));  // we have to bypass the toRawPtrTuple function since
                          // declval returns an rvalue reference

namespace detail {
// _____________________________________________________
template <typename T>
struct is_tuple_impl : std::false_type {};

template <typename... Ts>
struct is_tuple_impl<std::tuple<Ts...>> : std::true_type {};
}  // namespace detail

/// is_tuple<T>::value is true iff T is a specialization of std::tuple
template <typename T>
struct is_tuple : detail::is_tuple_impl<std::decay_t<T>> {};

/// is_tuple_v is true iff T is a specialization of std::tuple
template <typename T>
constexpr bool is_tuple_v = is_tuple<T>::value;
}  // namespace ad_tuple_helpers

#endif  // QLEVER_TUPLEHELPERS_H
