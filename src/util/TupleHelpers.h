// Copyright 2020, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (johannes.kalmbach@gmail.com)

#ifndef QLEVER_TUPLEHELPERS_H
#define QLEVER_TUPLEHELPERS_H
#include <utility>

namespace ad_tuple_helpers {

namespace detail {
// Recursive implementation of setupTupleFromCallable (see below)
template <size_t NumEntries, size_t Current, class F>
auto setupTupleFromCallableImpl(F&& f) {
  if constexpr (Current == NumEntries - 1) {
    return std::tuple(f(Current));
  } else {
    auto fst = std::tuple(f(Current));
    return std::tuple_cat(std::move(fst),
                          setupTupleFromCallableImpl<NumEntries, Current + 1>(
                              std::forward<F>(f)));
  }
}
}  // namespace detail

/**
 * @brief create a Tuple with numEntries entries where the i-th entry is created
 * by a call to f(i)
 * @tparam numEntries the size of the tuple to be created
 * @tparam F A function object that takes a single size_t as an argument
 * @param f
 * @return std::tuple(f(0), f(1), f(2), ... f(numEntries - 1));
 * @todo<joka921> Can we simplify this implementation via fold expressions etc?
 */
template <size_t numEntries, class F>
auto setupTupleFromCallable(F&& f) {
  static_assert(numEntries > 0);
  return detail::setupTupleFromCallableImpl<numEntries, 0>(std::forward<F>(f));
}

/**
 * @brief convert a parameter pack to a tuple of unique_ptrs that are
 * constructed from the elements of the parameter pack. E.g.
 * toUniquePtrTuple(int(3), std::string("foo")) ==
 * std::tuple(std::make_unique<int>(3), std::make_unique<std::string>("foo")) It
 * can take an arbitrary positive number of arguments. Currently this template
 * is linearly expanded, if this becomes a bottleneck at compiletime (e.g. for
 * very long parameter packs) we could do something more intelligent.
 * @return std::tuple(std::make_unique<First>(std::forward<First>(l),
 * std::make_unique<FirstOfRem.....
 */
template <typename First, typename... Rest>
auto toUniquePtrTuple(First&& l, Rest&&... rem) {
  if constexpr (sizeof...(Rest) == 0) {
    return std::tuple(
        std::make_unique<std::decay_t<First>>(std::forward<First>(l)));
  } else {
    return std::tuple_cat(std::tuple(std::make_unique<std::decay_t<First>>(
                              std::forward<First>(l))),
                          toUniquePtrTuple(std::forward<Rest>(rem)...));
  }
}

/// For a pack of types returns the return type of a call to toUniquePtrTuple
/// with arguments of that type
template <typename... ts>
using toUniquePtrTuple_t = decltype(toUniquePtrTuple(std::declval<ts>()...));

namespace detail {
// the implementation of toRawPtrTuple, idx denotes the next index to deal with
// (initially 0)
template <size_t idx, typename Tuple>
auto toRawPtrTupleImpl(Tuple&& tuple) {
  if constexpr (idx == std::tuple_size_v<std::decay_t<Tuple>> - 1) {
    return std::tuple(std::get<idx>(tuple).get());
  } else {
    return std::tuple_cat(
        std::tuple(std::get<idx>(tuple).get()),
        toRawPtrTupleImpl<idx + 1>(std::forward<Tuple>(tuple)));
  }
}
}  // namespace detail

/**
 * @brief converts a tuple of smart pointers (or other types that have a .get()
 * method) to a tuple of the corresponding raw pointers (return types of the
 * get() methods). Does NOT change ownership.
 * @tparam Tuple Must have form T<Ts...> where T must support
 * std::get<size_t>(T), and the types Ts contained in T must support a call to a
 * .get() method e.g. std::tuple<std::unique_ptr<int>,
 * std::shared_ptr<std::string>> or std::array<std::shared_ptr<int>, 42>;
 * @param tuple A tuple/array of type Tuple
 * @return std::tuple(std::get<0>(tuple).get(), std::get<1>(tuple).get(), ...
 * std::get<std::tuple_size_v<Tuple>>(tuple).get());
 */
template <typename Tuple>
auto toRawPtrTuple(Tuple&& tuple) {
  static_assert(!std::is_rvalue_reference_v<decltype(tuple)>,
                "You can't pass an rvalue reference to toRawPtrTuple, because "
                "the argument preserves the ownership to the pointers");
  return detail::toRawPtrTupleImpl<0>(std::forward<Tuple>(tuple));
}

/// The return type of toRawPtrTuple<T>(T&& t);
template <typename T>
using toRawPtrTuple_t = decltype(detail::toRawPtrTupleImpl<0>(
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
