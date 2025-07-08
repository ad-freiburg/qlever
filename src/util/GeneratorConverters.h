#include <optional>
#include <type_traits>

#include "Generator.h"
#include "util/Generator.h"
#include "util/Iterators.h"

template <typename ValueType, typename DetailsType>
cppcoro::generator<ValueType, DetailsType> fromInputRange(
    ad_utility::InputRangeTypeErased<ValueType, DetailsType> inputRange) {

  DetailsType& details = co_await cppcoro::getDetails;
  details = *inputRange.details();

  for (auto& value : inputRange) {
    co_yield value;
  }
}

template <typename ValueType>
cppcoro::generator<ValueType, cppcoro::NoDetails> fromInputRange(
    ad_utility::InputRangeTypeErased<ValueType, cppcoro::NoDetails> inputRange) {

  for (auto& value : inputRange) {
    co_yield value;
  }
}
