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

template <typename ValueType>
ad_utility::InputRangeTypeErased<ValueType> fromGenerator(
    cppcoro::generator<ValueType> generator) {
  struct Generator : ad_utility::InputRangeFromGet<ValueType> {
    Generator(cppcoro::generator<ValueType> generator)
        : generator{std::move(generator)},
          iterator{this->generator.begin()},
          end{this->generator.end()} {}

    std::optional<ValueType> get() override {
      if (iterator != end) {
        const auto value{*iterator};
        iterator++;
        return value;
      }

      return std::nullopt;
    };

    cppcoro::generator<ValueType> generator;
    decltype(generator.begin()) iterator;
    decltype(generator.end()) end;
  };

  return ad_utility::InputRangeTypeErased<ValueType>{std::move(generator)};
}
