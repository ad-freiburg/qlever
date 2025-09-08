// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#ifndef QLEVER_SRC_UTIL_PARAMETERS_H
#define QLEVER_SRC_UTIL_PARAMETERS_H

#include <atomic>
#include <concepts>
#include <optional>
#include <tuple>

#include "backports/keywords.h"
#include "backports/type_traits.h"
#include "util/ConstexprMap.h"
#include "util/ConstexprSmallString.h"
#include "util/HashMap.h"
#include "util/HashSet.h"
#include "util/MemorySize/MemorySize.h"
#include "util/ParseableDuration.h"
#include "util/TupleForEach.h"
#include "util/TypeTraits.h"

namespace ad_utility {
using ParameterName = ad_utility::ConstexprSmallString<100>;

// Abstract base class for a parameter that can be written and read as a
// `std::string`.
struct ParameterBase {
  /// Set the parameter by converting the string to the actual parameter.
  virtual void setFromString(const std::string& stringInput) = 0;

  /// Get a representation of the parameter as a `std::string`.
  [[nodiscard]] virtual std::string toString() const = 0;

  virtual ~ParameterBase() = default;
};

// Concepts for the template types of `Parameter`.
template <typename FunctionType, typename ToType>
CPP_concept ParameterFromStringType =
    std::default_initializable<FunctionType> &&
    InvocableWithSimilarReturnType<FunctionType, ToType, const std::string&>;

template <typename FunctionType, typename FromType>
CPP_concept ParameterToStringType =
    std::default_initializable<FunctionType> &&
    InvocableWithSimilarReturnType<FunctionType, std::string, FromType>;

/// Abstraction for a parameter that connects a (compile time) `Name` to a
/// runtime value.
/// \tparam Type The type of the parameter value
/// \tparam FromString A function type, that takes a const string&
///         (representation of a `Type`) and converts it to `Type`.
/// \tparam ToString A function type, that takes a `Type` and produces
///         a std::string representation.
/// \tparam Name The Name of the parameter (there are typically a lot of
///         parameters with the same `Type`).
CPP_template(typename Type, typename FromString, typename ToString,
             ParameterName Name)(
    requires std::semiregular<Type> CPP_and
        ParameterFromStringType<FromString, Type>
            CPP_and ParameterToStringType<ToString, Type>) struct Parameter
    : public ParameterBase {
  constexpr static ParameterName name = Name;

 private:
  Type value_{};

  // This function is called each time the value is changed.
  using OnUpdateAction = std::function<void(Type)>;
  std::optional<OnUpdateAction> onUpdateAction_ = std::nullopt;
  using ParameterConstraint = std::function<void(Type, std::string_view)>;
  std::optional<ParameterConstraint> parameterConstraint_ = std::nullopt;

 public:
  /// Construction is only allowed using an initial parameter value
  Parameter() = delete;
  explicit Parameter(Type initialValue) : value_{std::move(initialValue)} {};

  /// Copying is disabled, but moving is ok
  Parameter(const Parameter& rhs) = delete;
  Parameter& operator=(const Parameter& rhs) = delete;

  Parameter(Parameter&&) noexcept = default;
  Parameter& operator=(Parameter&&) noexcept = default;

  /// Set the value by using the `FromString` conversion.
  void setFromString(const std::string& stringInput) override {
    set(FromString{}(stringInput));
  }

  /// Set the value.
  void set(Type newValue) {
    if (parameterConstraint_.has_value()) {
      std::invoke(parameterConstraint_.value(), newValue, name);
    }
    value_ = std::move(newValue);
    triggerOnUpdateAction();
  }

  const Type& get() const { return value_; }

  /// Specify the onUpdateAction and directly trigger it.
  /// Note that this useful when the initial value of the parameter
  /// is known before the `onUpdateAction`.
  void setOnUpdateAction(OnUpdateAction onUpdateAction) {
    onUpdateAction_ = std::move(onUpdateAction);
    triggerOnUpdateAction();
  }

  /// Set an constraint that will be executed every time the value changes
  /// and once initially when setting it. It is intended to throw an exception
  /// if the value is invalid.
  void setParameterConstraint(ParameterConstraint&& parameterConstraint) {
    std::invoke(parameterConstraint, value_, name);
    parameterConstraint_ = std::move(parameterConstraint);
  }

  // ___________________________________________________________________
  [[nodiscard]] std::string toString() const override {
    return ToString{}(value_);
  }

 private:
  // Manually trigger the `_onUpdateAction` if it exists
  void triggerOnUpdateAction() {
    if (onUpdateAction_.has_value()) {
      onUpdateAction_.value()(value_);
    }
  }
};

// Concept that checks whether a type is an instantiation of the `Parameter`
// template.
namespace detail::parameterConceptImpl {
template <typename T>
struct ParameterConceptImpl : std::false_type {};

template <typename Type, typename FromString, typename ToString,
          ParameterName Name>
struct ParameterConceptImpl<Parameter<Type, FromString, ToString, Name>>
    : std::true_type {};
}  // namespace detail::parameterConceptImpl

template <typename T>
CPP_concept IsParameter =
    detail::parameterConceptImpl::ParameterConceptImpl<T>::value;

namespace detail::parameterShortNames {

// TODO<joka921> Replace these by versions that actually parse the whole
// string.
struct fl {
  template <typename T>
  float operator()(const T& s) const {
    return std::stof(s);
  }
};
struct dbl {
  template <typename T>
  double operator()(const T& s) const {
    return std::stod(s);
  }
};
struct szt {
  template <typename T>
  size_t operator()(const T& s) const {
    return std::stoull(s);
  }
};
struct bl {
  template <typename T>
  bool operator()(const T& s) const {
    if (s == "true") return true;
    if (s == "false") return false;
    AD_THROW(
        R"(The string value for bool parameter must be either "true" or "false".)");
  }
};

struct toString {
  template <typename T>
  std::string operator()(const T& s) const {
    return std::to_string(s);
  }
};
struct boolToString {
  std::string operator()(const bool& v) const { return v ? "true" : "false"; }
};

template <typename DurationType>
struct durationToString {
  std::string operator()(
      const ad_utility::ParseableDuration<DurationType>& duration) const {
    return duration.toString();
  }
};

template <typename DurationType>
struct durationFromString {
  ad_utility::ParseableDuration<DurationType> operator()(
      const std::string& duration) const {
    return ad_utility::ParseableDuration<DurationType>::fromString(duration);
  }
};

// To/from string for `MemorySize`.
struct MemorySizeToString {
  std::string operator()(const MemorySize& m) const { return m.asString(); }
};
struct MemorySizeFromString {
  MemorySize operator()(const std::string& str) const {
    return MemorySize::parse(str);
  }
};

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

template <ParameterName Name>
using Bool = Parameter<bool, bl, boolToString, Name>;

template <ParameterName Name>
using MemorySizeParameter =
    Parameter<MemorySize, MemorySizeFromString, MemorySizeToString, Name>;

template <typename DurationType, ParameterName Name>
using DurationParameter = Parameter<ad_utility::ParseableDuration<DurationType>,
                                    durationFromString<DurationType>,
                                    durationToString<DurationType>, Name>;
}  // namespace detail::parameterShortNames

/// A container class that stores several `Parameters`. The reading (via
/// the `get()` method) and writing (via `set()`) of the individual `Parameters`
/// is threadsafe (there is a mutex for each of the parameters). Note: The
/// design only allows atomic setting of a single parameter to a fixed value. It
/// does not support atomic updates depending on the current value (for example,
/// "increase the cache size by 20%") nor an atomic update of multiple
/// parameters at the same time. If needed, this functionality could be added
/// to the current implementation.
template <QL_CONCEPT_OR_TYPENAME(IsParameter)... ParameterTypes>
class Parameters {
  // In C++17 mode we cannot use SFINAE, but we also currently don't need it,
  // add a static_assert for safety should this ever change.
  static_assert((... && IsParameter<ParameterTypes>));

 private:
  using Tuple = std::tuple<ad_utility::Synchronized<ParameterTypes>...>;
  Tuple _parameters;

  // A compile-time map from `ParameterName` to the index of the corresponding
  // `Parameter` in the `_parameters` tuple.
  static constexpr auto _nameToIndex = []() {
    size_t i = 0;
    // {firstName, 0}, {secondName, 1}, {thirdName, 2}...
    auto arr = std::array{boost::hana::pair{ParameterTypes::name, i++}...};

    // Assert that the indices are in fact correct.
    for (size_t k = 0; k < arr.size(); ++k) {
      if (boost::hana::second(arr[k]) != k) {
        throw std::runtime_error{
            "Wrong order in parameter array, this should never happen."};
      }
    }
    return ConstexprMap{std::move(arr)};
  }();

  // The i-th element in this vector points to the i-th element in the
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
  QL_EXPLICIT(sizeof...(ParameterTypes) == 1)
  Parameters(ParameterTypes... ts) : _parameters{std::move(ts)...} {}

  // Get value for parameter `Name` known at compile time.
  // The parameter is returned  by value, since
  // we otherwise cannot guarantee threadsafety of this class.
  //  Note that we deliberately do not have an overload of
  // `get()` where the `Name` can be specified at runtime, because we want to
  // check the existence of a parameter at compile time.
  template <ParameterName Name>
  auto get() const {
    constexpr auto index = _nameToIndex.at(Name);
    return std::get<index>(_parameters).rlock()->get();
  }

  // Set value for parameter `Name` known at compile time.
  template <ParameterName Name, typename Value>
  void set(Value newValue) {
    constexpr auto index = _nameToIndex.at(Name);
    return std::get<index>(_parameters).wlock()->set(std::move(newValue));
  }

  // For the parameter with name `Name` specify the function that is to be
  // called, when this parameter value changes.
  template <ParameterName Name, typename OnUpdateAction>
  auto setOnUpdateAction(OnUpdateAction onUpdateAction) {
    constexpr auto index = _nameToIndex.at(Name);
    std::get<index>(_parameters)
        .wlock()
        ->setOnUpdateAction(std::move(onUpdateAction));
  }

  // Set the parameter with the name `parameterName` known at runtime to the
  // `value`. Note that each parameter is associated with a function to convert
  // a string to its actual type. If the `parameterName` does not exist, or
  // the conversion fails, an exception is thrown.
  void set(std::string_view parameterName, const std::string& value) {
    if (!_nameToIndex.contains(parameterName)) {
      throw std::runtime_error{"No parameter with name " +
                               std::string{parameterName} + " exists"};
    }
    try {
      // Call the virtual set(std::string) function on the
      // correct ParameterBase& in the `_runtimePointers`.
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
  [[nodiscard]] ad_utility::HashMap<std::string, std::string> toMap() const {
    ad_utility::HashMap<std::string, std::string> result;

    auto insert = [&](const auto& synchronizedParameter) {
      using T = std::decay_t<decltype(synchronizedParameter)>;
      std::string name{T::value_type::name};
      result[std::move(name)] = synchronizedParameter.rlock()->toString();
    };
    ad_utility::forEachInTuple(_parameters, insert);
    return result;
  }

  // Obtain the set of all parameter names.
  [[nodiscard]] const ad_utility::HashSet<std::string>& getKeys() const {
    static ad_utility::HashSet<std::string> value = [this]() {
      ad_utility::HashSet<std::string> result;

      auto insert = [&result](const auto& t) {
        using T = std::decay_t<decltype(t)>;
        result.insert(std::string{T::value_type::name});
      };
      ad_utility::forEachInTuple(_parameters, insert);
      return result;
    }();
    return value;
  }
};
}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_PARAMETERS_H
