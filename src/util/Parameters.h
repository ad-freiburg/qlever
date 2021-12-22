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
  virtual void setFromString(const std::string& stringInput) = 0;

  /// Get a representation of the parameter as a `std::string`.
  [[nodiscard]] virtual std::string toString() const = 0;

  virtual ~ParameterBase() = default;
};

/// Abstraction for a parameter that connects a (compile time) `Name` to a
/// runtime value.
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

 private:
  // The value is public, which is ok, because it is atomic and we have no
  // further invariants.
  Type value{};

  // A function which is called, when the value isChanged.
  // Note: The call to this function is not atomic, if this parameter
  // is accessed concurrently, the `_onUpdateFunction` has to be threadsafe.
  using OnUpdateFunction = std::function<void(Type)>;
  std::optional<OnUpdateFunction> _onUpdateFunction = std::nullopt;

 public:
  /// Construction is only allowed using an initial parameter value
  Parameter() = delete;
  explicit Parameter(Type initialValue) : value{std::move(initialValue)} {};

  Parameter(const Parameter& rhs) = delete;
  Parameter& operator=(const Parameter& rhs) = delete;

  Parameter(Parameter&&) noexcept = default;
  Parameter& operator=(Parameter&&) noexcept = default;

  /// Set the value by using the `FromString` conversion
  void setFromString(const std::string& stringInput) override {
    set(FromString{}(stringInput));
  }

  /// Set the value
  void set(Type newValue) {
    value = std::move(newValue);
    triggerOnUpdateFunction();
  }

  const Type& get() const { return value; }

  /// Specify the onUpdateFunction and directly trigger it.
  void setOnUpdateFunction(OnUpdateFunction onUpdateFunction) {
    _onUpdateFunction = std::move(onUpdateFunction);
    triggerOnUpdateFunction();
  }

  void triggerOnUpdateFunction() {
    if (_onUpdateFunction.has_value()) {
      _onUpdateFunction.value()(value);
    }
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

/// A container class that stores several `Parameters`. The reading (via
/// the `get()` method) and writing (via `set()`) of the individual `Parameters`
/// is threadsafe (There is a mutex for each of the parameters). Note: The
/// design only allows atomic setting of a single parameter to a fixed value. It
/// does not support atomic updates depending on the current value nor
/// consistent updates of multiple parameters at the same time. Such a
/// Functionality could however be added to the current implementation. \tparam
/// ParameterTypes
template <typename... ParameterTypes>
class Parameters {
 private:
  using Tuple = std::tuple<ad_utility::Synchronized<ParameterTypes>...>;
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
  using RuntimePointers =
      std::array<ad_utility::Synchronized<ParameterBase&, std::shared_mutex&>,
                 std::tuple_size_v<Tuple>>;
  RuntimePointers _runtimePointers = [this]() {
    auto toBase = [](auto& synchronizedParameter) {
      return synchronizedParameter.template toBaseReference<ParameterBase>();
    };
    return ad_utility::tupleToArray(_parameters, toBase);
  }();

 public:
  Parameters() = delete;
  explicit(sizeof...(ParameterTypes) == 1) Parameters(ParameterTypes... ts)
      : _parameters{std::move(ts)...} {}

  // Obtain the current parameter value for `Name`. `Name` has to be
  // specified at compile time. The parameter is returned  by value, since
  // we otherwise cannot guarantee threadsafety of this class.
  //  Note that we deliberately do not have an overload of
  // `get()` where the `Name` can be specified at runtime, because we want to
  // check the existence of a parameter at compile time.
  template <ParameterName Name>
  auto get() const {
    constexpr auto index = _nameToIndex.at(Name);
    return std::get<index>(_parameters).rlock()->get();
  }

  // TODO<joka921> comment and common code refactor
  template <ParameterName Name, typename Value>
  void set(Value newValue) {
    constexpr auto index = _nameToIndex.at(Name);
    return std::get<index>(_parameters).wlock()->set(std::move(newValue));
  }

  // For the parameter with name `Name` specify the function that is to be
  // called, when this parameter value changes.
  template <ParameterName Name, typename UpdateFunction>
  auto setUpdateOption(UpdateFunction updateFunction) {
    constexpr auto index = _nameToIndex.at(Name);
    std::get<index>(_parameters)
        .wlock()
        ->setOnUpdateFunction(std::move(updateFunction));
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
      _runtimePointers[_nameToIndex.at(parameterName)].wlock()->setFromString(
          value);
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

    auto insert = [&]<typename T>(const T& synchronizedParameter) {
      std::string name{T::value_type::name};
      result[std::move(name)] = synchronizedParameter.rlock()->toString();
    };
    ad_utility::forEachTuple(_parameters, insert);
    return result;
  }

  // Obtain the set of all parameter names.
  [[nodiscard]] const ad_utility::HashSet<std::string>& getKeys() const {
    static ad_utility::HashSet<std::string> value = [this]() {
      ad_utility::HashSet<std::string> result;

      auto insert = [this, &result]<typename T>(const T&) {
        result.insert(std::string{T::value_type::name});
      };
      ad_utility::forEachTuple(_parameters, insert);
      return result;
    }();
    return value;
  }
};
}  // namespace ad_utility

#endif  // QLEVER_PARAMETERS_H
