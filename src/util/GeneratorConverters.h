#include <optional>

#include "util/Generator.h"
#include "util/Iterators.h"

template <typename ValueType>
cppcoro::generator<ValueType> fromInputRange(
    ad_utility::InputRangeTypeErased<ValueType> inputRange) {
  for (auto& value : inputRange) {
    co_yield value;
  }
}
