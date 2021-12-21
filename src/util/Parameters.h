// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)

#ifndef QLEVER_PARAMETERS_H
#define QLEVER_PARAMETERS_H

#include <atomic>
#include <tuple>

#include "./ConstexprMap.h"
#include "./ConstexprSmallString.h"
#include "./HashMap.h"
#include "./HashSet.h"
#include "./TupleForEach.h"
#include "./TypeTraits.h"

namespace ad_utility {
using ParameterName = ad_utility::ConstexprSmallString<100>;

// Virtual Base class for a parameter that can be written and read as a
// `std::string`
struct ParameterBase {
  /// Set the parameter by converting the string to the actual parameter
  virtual void set(const std::string& stringInput) = 0;

  /// Get a representation of the parameter as a `std::string`.
  [[nodiscard]] virtual std::string toString() const = 0;
};

/// Abstraction for a parameter that connects a (compile time) `Name` to a
/// runtime value. The value is stored as a `std::atomic<Type>`, making a single
/// `Parameter` threadsafe.
/// \tparam Type The type of the parameter value
/// \tparam FromString A function type, that takes a const string&
///         (representation of a `Type`) and converts it to `Type`.
/// \tparam ToString A function type, that takes a `Type` and produces
///         a std::string representation.
/// \tparam Name The Name of the parameter (there are typically a lot of
///         parameters with the same `Type`).
template <typename Type, typename FromString, typename ToString,
          ParameterName Name>
struct Parameter : public ParameterBase {
  constexpr static ParameterName name = Name;
  // The value is public, which is ok, because it is atomic and we have no
  // further invariants.
  std::atomic<Type> value{};

  /// Construction is only allowed using an initial parameter value
  Parameter() = delete;
  explicit Parameter(Type initialValue) : value{std::move(initialValue)} {};

  // Atomic values are neither copyable nor movable.
  // TODO<joka921> Is it possible to get rid of these constructors by using
  // aggregate initialization for the `Parameters` class?
  Parameter(const Parameter& rhs) : value{rhs.value.load()} {};
  Parameter& operator=(const Parameter& rhs) { value = rhs.value.load(); };

  /// Set the value by using the `FromString` conversion
  void set(const std::string& stringInput) override {
    value = FromString{}(stringInput);
  }

  [[nodiscard]] std::string toString() const override {
    return ToString{}(value);
  }
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
  // A compile-time map from `ParameterName` to the index of the corresponding
  // `Parameter` in the `_parameters` tuple.
  static constexpr auto _nameToIndex = []() {
    size_t i = 0;
    // {firstName, 0}, {secondName, 1}, {thirdName, 2}...
    auto arr = std::array{std::pair{ParameterTypes::name, i++}...};

    // Assert that the indices are in fact correct.
    for (size_t k = 0; k < arr.size(); ++k) {
      if (arr[k].second != k) {
        throw std::runtime_error{"This should never happen"};
      }
    }
    return ConstexprMap{std::move(arr)};
  }();

  // The i-th element in this vector points to the i-th element in th
  // `_parameters` tuple.
  using RuntimeVector = std::vector<ParameterBase*>;
  RuntimeVector _runtimePointers = [this]() {
    RuntimeVector m;
    m.reserve(std::tuple_size_v<Tuple>);
    auto insert = [&, this](auto& parameter) { m.push_back(&parameter); };
    ad_utility::forEachTuple(_parameters, insert);
    return m;
  }();

 public:
  Parameters() = delete;
  explicit(sizeof...(ParameterTypes) == 1) Parameters(ParameterTypes... ts)
      : _parameters{std::move(ts)...} {}

  // Obtain a reference to the parameter value for `Name`. `Name` has to be
  // specified at compile time. A reference to the `std::atomic` is returned, so
  // it is safe to also return a non-const reference which then might be
  // (atomically) changed. Note that we deliberately do not have an overload of
  // `get()` where the `Name` can be specified at runtime, because we want to
  // check the existence of a parameter at compile time.
  template <ParameterName Name>
  auto& get() {
    constexpr auto index = _nameToIndex.at(Name);
    return std::get<index>(_parameters).value;
  }

  // TODO<C++23> remove duplication using the `deducing this` feature.
  template <ParameterName Name>
  const auto& get() const {
    constexpr auto index = _nameToIndex.at(Name);
    return std::get<index>(_parameters).value;
  }

  // Set the parameter with the name `parameterName` to the `value`. Note that
  // each parameter is associated with a function to convert a string to its
  // actual type. If the `parameterName` does not exist, or the `value` could
  // not be converted to the type.
  void set(std::string_view parameterName, const std::string& value) {
    if (!_nameToIndex.contains(parameterName)) {
      throw std::runtime_error{"No parameter with name " +
                               std::string{parameterName} + " exists"};
    }
    try {
      // call the virtual set(std::string) function on the
      // correct ParameterBase* in the `_runtimePointers`
      _runtimePointers[_nameToIndex.at(parameterName)]->set(value);
    } catch (const std::exception& e) {
      throw std::runtime_error("Could not set parameter " +
                               std::string{parameterName} + " to value " +
                               value + ". Exception was: " + e.what());
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
};
}  // namespace ad_utility

#endif  // QLEVER_PARAMETERS_H
