// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Julian Mundhahs (mundhahj@informatik.uni-freiburg.de)

#ifndef QLEVER_ALGORITHM_H
#define QLEVER_ALGORITHM_H

namespace ad_utility {

/**
 * Checks whether an element is contained in a container.
 *
 * @param container Container& Elements to be searched
 * @param element T Element to search for
 * @return bool
 */
template <typename Container, typename T>
inline bool contains(const Container& container, const T& element) {
  return std::find(container.begin(), container.end(), element) !=
         container.end();
}

/**
 * Checks whether an element in the container satisfies the predicate.
 *
 * @param container Container& Elements to be searched
 * @param predicate Predicate Predicate to check
 * @return bool
 */
template <typename Container, typename Predicate>
bool contains_if(const Container& container, const Predicate& predicate) {
  return std::find_if(container.begin(), container.end(), predicate) !=
         container.end();
}

/**
 * Appends the second vector to the first one.
 *
 * @param destination Vector& to which to append
 * @param source Vector&& to append
 */
template <typename T>
void appendVector(std::vector<T>& destination, std::vector<T>&& source) {
  destination.insert(destination.end(), std::make_move_iterator(source.begin()),
                     std::make_move_iterator(source.end()));
}

/**
 * Applies the UnaryOperation to all elements of the vector.
 */
template <typename Input, typename F, typename Output = std::invoke_result_t<F, Input&&>
std::vector<Output> transform(std::vector<Input>&& input, F unaryOp) {
  std::vector<Output> out;
  out.reserve(input.size());
  std::transform(std::make_move_iterator(input.begin()),
                 std::make_move_iterator(input.end()), std::back_inserter(out),
                 unaryOp);
  return out;
}

/**
 * Flatten a vector<vector<T>> into a vector<T>.
 */
template <typename T>
std::vector<T> flatten(std::vector<std::vector<T>> input) {
  std::vector<T> out;
  for (std::vector<T> sub : input) {
    appendVector(out, std::move(sub));
  }
  return out;
}

}  // namespace ad_utility

#endif  // QLEVER_ALGORITHM_H
