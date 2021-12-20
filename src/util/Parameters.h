// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)

#ifndef QLEVER_PARAMETERS_H
#define QLEVER_PARAMETERS_H

#include <atomic>
#include <tuple>

#include "./ConstexprSmallString.h"
#include "./HashMap.h"
#include "./HashSet.h"
#include "./TypeTraits.h"

namespace ad_utility {
using ParameterName = ad_utility::ConstexprSmallString<100>;

/// Abstraction for an Parameter that connects a (compile time) `Name` to a
/// runtime value. The value is stored as a `std::atomic<Type>`, making a single
/// Parameter threadsafe. \tparam Type The type of the parameter value \tparam
/// FromString A function type, that takes a const string& (representation of a
/// `Type`) and converts it to `Type`. \tparam ToString A function type, that
/// takes a `Type` and produces a std::string representation. \tparam Name The
/// Name of the parameter (there are typically a lot of parameters with the same
/// `Type`
template <typename Type, typename FromString, typename ToString,
          ParameterName Name>
struct Parameter {
  constexpr static ParameterName name = Name;
  /// Construction is only allowed using an initial parameter value
  Parameter() = delete;
  explicit Parameter(Type initialValue) : value{std::move(initialValue)} {};

  // Atomic values are neither copy- nor movable.
  // TODO<joka921> Is it possible to get rid of these constructors by using
  // aggregate initialization for the `Parameters` class?
  Parameter(const Parameter& rhs) : value{rhs.value.load()} {};
  Parameter& operator=(const Parameter& rhs) { value = rhs.value.load(); };

  /// Set the value by using the `FromString` conversion
  void set(const std::string& stringInput) {
    value = FromString{}(stringInput);
  }

  // The value is public, which is ok, because it is atomic and we have no
  // further invariants.
  std::atomic<Type> value{};

  [[nodiscard]] std::string toString() const { return ToString{}(value); }
};

namespace detail::parameterShortNames {

// TODO<joka921> Replace these by versions that actually parse the whole
// string.
using fl = decltype([](const auto& s) { return std::stof(s); });
using dbl = decltype([](const auto& s) { return std::stod(s); });
using szt = decltype([](const auto& s) { return std::stoull(s); });

using toString = decltype([](const auto& s) { return std::to_string(s); });

/// Partial template specialization for Parameters with common types (numeric
/// types and strings)
template <ParameterName Name>
using Float = Parameter<float, fl, toString, Name>;

template <ParameterName Name>
using Double = Parameter<double, dbl, toString, Name>;

template <ParameterName Name>
using SizeT = Parameter<size_t, szt, toString, Name>;

template <ParameterName Name>
using String = Parameter<std::string, std::identity, std::identity, Name>;

}  // namespace detail::parameterShortNames

/// A container class that stores several atomic `Parameters`. The reading (via
/// the `get()` method) and writing (via `set()`) is atomic and therefore
/// threadsafe. Note: The design only allows atomic setting of a single
/// parameter to a fixed value. It does not support atomic updates depending on
/// the current value nor consistent updates of multiple parameters at the same
/// time. For such requirements, a different implementation would be needed.
/// \tparam ParameterTypes
template <typename... ParameterTypes>
class Parameters {
 private:
  using Tuple = std::tuple<ParameterTypes...>;
  Tuple _parameters;

 public:
  Parameters() = delete;
  explicit(sizeof...(ParameterTypes) == 1) Parameters(ParameterTypes... ts)
      : _parameters{std::move(ts)...} {}

  // Obtain a reference to the parameter value vor `Name`. `Name` has to be
  // specified at compile time. A reference to the `std::atomic` is returned, so
  // it is safe to also return a non-const reference which then might be
  // (atomically) changed. Note that we deliberately do not have an overload of
  // `get()` where the `Name` can be specified at runtime, because we want to
  // check the existence of a parameter at compile time.
  template <ParameterName Name>
  auto& get() {
    constexpr auto index = getIndex(Name);
    return std::get<index>(_parameters).value;
  }

  // TODO<C++23> remove duplication using the `deducing this` feature.
  template <ParameterName Name>
  const auto& get() const {
    constexpr auto index = getIndex<Name>();
    return std::get<index>(_parameters).value;
  }

  // Set the parameter with the name `parameterName` to the `value`. Note that
  // each parameter is associated with a function to convert a string to its
  // actual type. If the `parameterName` does not exist, or the `value` could
  // not be converted to the type.
  void set(const std::string& parameterName, const std::string& value) {
    // Set a single `param` to the `value` if the `parameterName` matches.
    // Return true iff the `parameterName` matches.
    auto setSingle = [](auto& param, const std::string& parameterName,
                        const std::string& value) -> bool {
      if (std::string_view(param.name) == std::string_view(parameterName)) {
        param.set(value);
        return true;
      }
      return false;
    };

    try {
      bool keyExists = std::apply(
          [&](auto&... parameters) {
            return (... || setSingle(parameters, parameterName, value));
          },
          _parameters);
      if (!keyExists) {
        throw std::runtime_error{"No parameter with name " + parameterName +
                                 " exists"};
      }
    } catch (const std::exception& e) {
      throw std::runtime_error("Could not set parameter " + parameterName +
                               " to value " + value +
                               ". Exception was: " + e.what());
    }
  }

  // Obtain a map from parameter names to parameter values.
  // This map only contains strings and is purely for logging
  // to human users.
  ad_utility::HashMap<std::string, std::string> toMap() {
    ad_utility::HashMap<std::string, std::string> result;
    std::apply(
        [&](const auto&... els) {
          (..., (result[std::string{els.name}] = els.toString()));
        },
        _parameters);
    return result;
  }

  // Obtain the set of all parameter names.
  [[nodiscard]] const ad_utility::HashSet<std::string>& getKeys() const {
    static ad_utility::HashSet<std::string> result = std::apply(
        [&](const auto&... els) {
          ad_utility::HashSet<std::string> result;
          (..., (result.insert(std::string{els.name})));
          return result;
        },
        _parameters);
    return result;
  }

 private:
  // Get the index of the `name` in the `_parameters` tuple.
  // TODO<LLVM14??, joka921> to my understanding this function should
  // be consteval.
  [[nodiscard]] static constexpr size_t getIndex(const ParameterName& name) {
    constexpr auto names = std::array{ParameterTypes::name...};
    for (size_t i = 0; i < names.size(); ++i) {
      if (name == names[i]) {
        return i;
      }
    }
    throw std::runtime_error(
        "The parameter name was not found among the specified parameters");
  }
};
}  // namespace ad_utility

#endif  // QLEVER_PARAMETERS_H
