// Copyright 2020, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (johannes.kalmbach@gmail.com)

#ifndef QLEVER_TUPLEHELPERS_H
#define QLEVER_TUPLEHELPERS_H
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
  return std::tuple(f(I)...);
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
 * @return std::tuple(f(0), f(1), f(2), ... f(numEntries - 1));
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
 * std::tuple(std::make_unique<int>(3), std::make_unique<std::string>("foo")) It
 * can take an arbitrary positive number of arguments. Currently this template
 * is linearly expanded, if this becomes a bottleneck at compiletime (e.g. for
 * very long parameter packs) we could do something more intelligent.
 * @return std::tuple(std::make_unique<First>(std::forward<First>(l),
 * std::make_unique<FirstOfRem.....
 */
template <typename... Rest>
auto toUniquePtrTuple(Rest&&... rem) {
  return std::tuple(
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
 * @return std::tuple(std::get<0>(tuple).get(), std::get<1>(tuple).get(), ...
 * std::get<std::tuple_size_v<Tuple>>(tuple).get());
 */
template <typename Tuple, bool safe = true>
auto toRawPtrTuple(Tuple&& tuple) {
  if constexpr (safe) {
    static_assert(
        !std::is_rvalue_reference_v<decltype(tuple)>,
        "You can't pass an rvalue reference to toRawPtrTuple, because "
        "the argument preserves the ownership to the pointers");
  }
  auto f = [](auto&&... args) { return std::tuple(args.get()...); };
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

/// TODO<joka921> Comment
namespace detail {
template <size_t i, typename Getter, typename... Vectors>
auto getElementFromVectorTupleImpl(const std::tuple<Vectors...>& tuple,
                                   size_t index, Getter g = Getter{}) {
  static_assert(i < std::tuple_size_v<std::decay_t<decltype(tuple)>>);
  const auto& vector = std::get<i>(tuple);
  if (index < vector.size()) {
    return g(vector[index]);
  } else {
    if constexpr (i + 1 < std::tuple_size_v<std::decay_t<decltype(tuple)>>) {
      return getElementFromVectorTupleImpl<i + 1>(tuple, index - vector.size(),
                                                  g);
    } else {
      throw std::out_of_range(
          "Total size of vectors in tuple was smaller than the requested index");
    }
  }
}

template<typename T>
static constexpr bool always_false = false;

template <size_t i, typename ValueType, typename BoundFunction, typename... Vectors>
size_t tupleBoundFunctionImpl(const std::tuple<Vectors...>& tuple, size_t offset, BoundFunction boundFunction,
                         const ValueType& value) {
  static_assert(i < std::tuple_size_v<std::decay_t<decltype(tuple)>>);
  const auto& vector = std::get<i>(tuple);
  if constexpr (std::is_same_v<ValueType, typename std::decay_t<decltype(vector)>::value_type>) {
    return offset + static_cast<size_t>(boundFunction(vector.begin(), vector.end(), value) - vector.begin());
  } else {
    if constexpr (i + 1 < std::tuple_size_v<std::decay_t<decltype(tuple)>>) {
      return tupleBoundFunctionImpl<i + 1>(tuple, offset + vector.size(), boundFunction, value);
    } else {
      static_assert(always_false<ValueType>, "calling lower bound on a tuple of vectors must match one of the value types exactly");
    }
  }
}

template <size_t i, typename Function, typename... Vectors>
void forEachVectorImpl(std::tuple<Vectors...>& tuple,
                           const Function& f) {
  static_assert(i < std::tuple_size_v<std::decay_t<decltype(tuple)>>);
  auto& vector = std::get<i>(tuple);
  f(vector);
  if constexpr (i + 1 < std::tuple_size_v<std::decay_t<decltype(tuple)>>) {
    return forEachVectorImpl<i + 1>(tuple, f);
  }
}
}

/// TODO<joka921> Comment
template <typename Getter, typename... Vectors>
auto getElementFromVectorTuple(const std::tuple<Vectors...>& tuple,
                                   size_t index, Getter g = Getter{})  {
  return detail::getElementFromVectorTupleImpl<0>(tuple, index, g);
}

/// TODO<joka921> Comment
template <typename... Vectors>
auto getTotalSizeFromVectorTuple(const std::tuple<Vectors...>& tuple)  {
  auto addSizes = [](const auto&... vecs) {
    return (... + vecs.size());
  };
  return std::apply(addSizes, tuple);
}

template<typename... InnerTypes>
class TupleOfVectors {
 public:
  using Tuple = std::tuple<std::vector<InnerTypes>...>;

  TupleOfVectors() = default;
  TupleOfVectors(Tuple tuple) : _tuple(std::move(tuple)) {}
  size_t size() const {
    if constexpr (isEmptyType) {
      return 0ull;
    } else {
      return getTotalSizeFromVectorTuple(_tuple);
    }
  }

  template<typename T>
  size_t lowerBound(const T& value) const {
    auto lb = []<typename... Args>(Args&&... args) {return std::lower_bound(std::forward<Args>(args)...);};
    return detail::tupleBoundFunctionImpl<0>(_tuple, 0, lb, value);
  }

  template<typename T>
  size_t upperBound(const T& value) const {
    auto ub = []<typename... Args>(Args&&... args) {return std::upper_bound(std::forward<Args>(args)...);};
    return detail::tupleBoundFunctionImpl<0>(_tuple, 0, ub, value);
  }

  template<typename T>
  bool getIndex(const T& value, size_t* id) const {
    auto idxToCheck = lowerBound(value);
    if (idxToCheck >= size()) {
      return false;
    }
    *id = idxToCheck;
    auto check = [id, idxToCheck, &value](const auto& element) {
      if constexpr (std::is_same_v<std::decay_t< decltype(value)>, std::decay_t<decltype(element)>>) {
        if (value == element) {
          return true;
        }
      }
      return false;
    };

    return at(idxToCheck, check);
  }

  template<typename T>
  static constexpr bool isTypeContained_v = (std::is_same_v<InnerTypes, std::decay_t<T>> || ...);

  template<typename T>
  static constexpr bool isTypeContained([[maybe_unused]] T&& t) {
    return (std::is_same_v<InnerTypes, std::decay_t<T>> || ...);
  }

  template<typename Getter>
  auto at(size_t index, Getter g) const {
    if constexpr (isEmptyType) {
      throw std::out_of_range("Tried to get an element from a TupleOfVectors which contains no types");
    } else {
      return getElementFromVectorTuple(_tuple, index, g);
    }
  }

  void clear() {
    if constexpr( !isEmptyType) {
      detail::forEachVectorImpl<0>(_tuple, [](auto& vec){vec.clear();});
    }
  };

  static constexpr bool isEmptyType = sizeof...(InnerTypes) == 0;
 private:
   Tuple _tuple;

};

}  // namespace ad_tuple_helpers
#endif  // QLEVER_TUPLEHELPERS_H
