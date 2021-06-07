//
// Created by johannes on 07.06.21.
//

#ifndef QLEVER_PARAMETERS_H
#define QLEVER_PARAMETERS_H

#include "./ConstexprSmallString.h"
#include <tuple>

using ParameterName = ad_utility::ConstexprSmallString<100>;

template<typename Type, ParameterName Name>
struct Parameter {
  constexpr static ParameterName name = Name;
  Parameter() = default;
  Parameter(Type defaultValue) : value{std::move(defaultValue)}{} ;
  Type value{};
};

template<size_t i, ParameterName Name, typename Tuple>
constexpr size_t getParameterIndex() {
  static_assert(i < std::tuple_size_v<Tuple>, "the parameter name was not found among the specified parameters");
  using CurrentType =typename  std::tuple_element<i, Tuple>::type;
  if constexpr (CurrentType::name == Name) {
    return i;
  } else {
    return getParameterIndex<i + 1, Name, Tuple>();
  }
  };

template<typename... ParameterTypes>
struct Parameters {
  using Tuple = std::tuple<ParameterTypes...>;
  Tuple _parameters{};

  Parameters() = default;
  Parameters(ParameterTypes... ts) : _parameters{std::move(ts)...} {}

  template<ParameterName Name>
  auto& get() {
    return std::get<getParameterIndex<0, Name, decltype(_parameters)>()>(_parameters).value;
  }

  template<ParameterName Name>
  const auto& get() const {
    return std::get<getParameterIndex<0, Name, decltype(_parameters)>()>(_parameters).value;
  }
};



#endif  // QLEVER_PARAMETERS_H
