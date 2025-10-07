// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#ifndef QLEVER_SRC_UTIL_PARAMETERS_H
#define QLEVER_SRC_UTIL_PARAMETERS_H

#include <optional>

#include "backports/concepts.h"
#include "util/HashMap.h"
#include "util/MemorySize/MemorySize.h"
#include "util/ParseableDuration.h"
#include "util/TypeTraits.h"

namespace ad_utility {

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

/// Abstraction for a parameter that stores a value of the given `Type`.
/// The value can be accessed via a typesafe interface, but also via a
/// type-erased interface the that reads/writes the value to/from a
/// `std::string`.
/// \tparam Type The type of the parameter value
/// \tparam FromString A function type, that takes a const string&
///         (representation of a `Type`) and converts it to `Type`.
/// \tparam ToString A function type, that takes a `Type` and produces
///         a std::string representation.
CPP_template(typename Type, typename FromString, typename ToString)(
    requires std::semiregular<Type> CPP_and
        ParameterFromStringType<FromString, Type>
            CPP_and ParameterToStringType<ToString, Type>) struct Parameter
    : public ParameterBase {
 public:
  using value_type = Type;

 private:
  Type value_{};
  std::string name_;

  // This function is called each time the value is changed.
  using OnUpdateAction = std::function<void(Type)>;
  std::optional<OnUpdateAction> onUpdateAction_ = std::nullopt;
  using ParameterConstraint = std::function<void(Type, std::string_view)>;
  std::optional<ParameterConstraint> parameterConstraint_ = std::nullopt;

 public:
  /// Construction is only allowed using an initial parameter value
  Parameter() = delete;
  explicit Parameter(Type initialValue, std::string name)
      : value_{std::move(initialValue)}, name_{std::move(name)} {};

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
      std::invoke(parameterConstraint_.value(), newValue, name_);
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
    std::invoke(parameterConstraint, value_, name_);
    parameterConstraint_ = std::move(parameterConstraint);
  }

  // ___________________________________________________________________
  [[nodiscard]] std::string toString() const override {
    return ToString{}(value_);
  }

  // ___________________________________________________________________
  const std::string& name() const { return name_; }

 private:
  // Manually trigger the `_onUpdateAction` if it exists
  void triggerOnUpdateAction() {
    if (onUpdateAction_.has_value()) {
      onUpdateAction_.value()(value_);
    }
  }
};

// Helper structs to define `Parameter`s. They provide operators to serialize
// common types to and from `std::string`.
namespace detail::parameterSerializers {

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

}  // namespace detail::parameterSerializers

namespace detail::parameterShortNames {
namespace n = detail::parameterSerializers;
/// Partial template specialization for Parameters with common types (numeric
/// types and strings)
using Float = Parameter<float, n::fl, n::toString>;
using Double = Parameter<double, n::dbl, n::toString>;

using SizeT = Parameter<size_t, n::szt, n::toString>;

using String = Parameter<std::string, std::identity, std::identity>;

using Bool = Parameter<bool, n::bl, n::boolToString>;

using MemorySizeParameter =
    Parameter<MemorySize, n::MemorySizeFromString, n::MemorySizeToString>;

template <typename DurationType>
using DurationParameter = Parameter<ParseableDuration<DurationType>,
                                    n::durationFromString<DurationType>,
                                    n::durationToString<DurationType>>;
}  // namespace detail::parameterShortNames
}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_PARAMETERS_H
