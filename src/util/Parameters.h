//
// Created by johannes on 07.06.21.
//

#ifndef QLEVER_PARAMETERS_H
#define QLEVER_PARAMETERS_H

#include <atomic>
#include <tuple>

#include "./ConstexprSmallString.h"
#include "./HashMap.h"
#include "./HashSet.h"

namespace ad_utility {
using ParameterName = ad_utility::ConstexprSmallString<100>;

template <typename Type, typename FromString, typename ToString,
          ParameterName Name>
struct Parameter {
  constexpr static ParameterName name = Name;
  Parameter() = default;
  Parameter(Type defaultValue) : value{std::move(defaultValue)} {};
  Parameter(const Parameter& rhs) : value{rhs.value.load()} {};
  Parameter& operator=(const Parameter& rhs) { value = rhs.value.load(); };
  void set(const std::string& stringInput) {
    value = FromString{}(stringInput);
  }
  std::atomic<Type> value{};

  std::string toString() const { return ToString{}(value); }
};

namespace detail::parameterShortNames {

// TODO<joka921> Replace these by versionsj that actually parse the whole
// string.
inline auto fl = [](const auto& s) { return std::stof(s); };
inline auto dbl = [](const auto& s) { return std::stod(s); };
inline auto szt = [](const auto& s) { return std::stoull(s); };

inline auto toString = [](const auto& s) { return std::to_string(s); };

template <ParameterName Name>
using Float = Parameter<float, decltype(fl), decltype(toString), Name>;

template <ParameterName Name>
using Double = Parameter<double, decltype(dbl), decltype(toString), Name>;

template <ParameterName Name>
using SizeT = Parameter<size_t, decltype(szt), decltype(toString), Name>;
}  // namespace detail::parameterShortNames

template <size_t i, ParameterName Name, typename Tuple>
constexpr size_t getParameterIndex() {
  static_assert(
      i < std::tuple_size_v<Tuple>,
      "the parameter name was not found among the specified parameters");
  using CurrentType = typename std::tuple_element<i, Tuple>::type;
  if constexpr (CurrentType::name == Name) {
    return i;
  } else {
    return getParameterIndex<i + 1, Name, Tuple>();
  }
};

template <typename... ParameterTypes>
struct Parameters {
  using Tuple = std::tuple<ParameterTypes...>;
  Tuple _parameters{};

  Parameters() = default;
  Parameters(ParameterTypes... ts) : _parameters{std::move(ts)...} {}

  template <ParameterName Name>
  auto& get() {
    return std::get<getParameterIndex<0, Name, decltype(_parameters)>()>(
               _parameters)
        .value;
  }

  template <ParameterName Name>
  const auto& get() const {
    return std::get<getParameterIndex<0, Name, decltype(_parameters)>()>(
               _parameters)
        .value;
  }

  void set(const std::string& parameterName, const std::string& value) {
    auto keyExists = [&](const auto&... params) {
      return (... || (std::string_view(parameterName) ==
                      std::string_view(params.name)));
    };

    auto setSingle = [](auto& param, const std::string& parameterName,
                        const std::string& value) {
      if (std::string_view(param.name) == std::string_view(parameterName)) {
        param.set(value);
      }
    };
    if (!std::apply(keyExists, _parameters)) {
      throw std::runtime_error{"No parameter with name " + parameterName +
                               " exists"};
    }
    try {
      std::apply(
          [&](auto&... args) {
            return (..., setSingle(args, parameterName, value));
          },
          _parameters);
    } catch (const std::exception& e) {
      throw std::runtime_error("Could not set parameter " + parameterName +
                               " to value " + value +
                               ". Exception was: " + e.what());
    }
  }

  // ___________________________________________________________________________
  ad_utility::HashMap<std::string, std::string> toMap() {
    ad_utility::HashMap<std::string, std::string> result;
    std::apply(
        [&](const auto&... els) {
          (..., (result[std::string{els.name}] = els.toString()));
        },
        _parameters);
    return result;
  }

  // __________________________________________________________________________
  const ad_utility::HashSet<std::string>& getKeys() const {
    static ad_utility::HashSet<std::string> result = std::apply(
        [&](const auto&... els) {
          ad_utility::HashSet<std::string> result;
          (..., (result.insert(std::string{els.name})));
          return result;
        },
        _parameters);
    return result;
  }
};
}  // namespace ad_utility

#endif  // QLEVER_PARAMETERS_H
