//  Copyright 2021, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_PROGRAMOPTIONSHELPERS_H
#define QLEVER_PROGRAMOPTIONSHELPERS_H

#include <boost/program_options.hpp>
#include <vector>

#include "./Parameters.h"
namespace ad_utility {

// An implicit wrapper that can be implicitly converted to and from `size_t`.
// When using it as a target value in `boost::program_options` it will only
// accept positive values because of the `validate` function below.
class NonNegative {
 public:
  operator size_t() { return _value; }
  NonNegative(size_t value) : _value{value} {}
  NonNegative() = default;

 private:
  size_t _value;
};

template <typename Stream>
Stream& operator<<(Stream& stream, NonNegative nonNegative) {
  return stream << static_cast<size_t>(nonNegative);
}

// This function is required  to use the `NonNegative` class in the
// `boost::program_options`. It throws an exception when parsing a negative
// value.
void validate(boost::any& v, const std::vector<std::string>& values,
              NonNegative*, int) {
  using namespace boost::program_options;
  validators::check_first_occurrence(v);

  std::string const& s = validators::get_single_string(values);
  if (s.empty() || s[0] == '-') {
    throw std::runtime_error("Expected a positive number but got " + s + ".");
  }

  v = NonNegative{boost::lexical_cast<size_t>(s)};
}

// This function is required  to use `std::optional` in
// `boost::program_options`.
template <typename T>
void validate(boost::any& v, const std::vector<std::string>& values,
              std::optional<T>*, int) {
  // First parse as a T
  T* dummy = nullptr;
  validate(v, values, dummy, 0);

  // Wrap the T inside std::optional
  AD_CHECK(!v.empty());
  v = std::optional<T>(boost::any_cast<T>(v));
}

template <ParameterName name, typename Parameters>
class ParameterToProgramOption {
 public:
  using Type = typename Parameters::template Type<name>;

 private:
  Parameters* _parameters = nullptr;
  std::optional<Type> _value;

 public:
  explicit ParameterToProgramOption(Parameters* parameters)
      : _parameters(parameters) {}
  explicit ParameterToProgramOption(Type value) : _value{std::move(value)} {}
  ParameterToProgramOption() = default;

  ParameterToProgramOption(const ParameterToProgramOption&) = default;

  ParameterToProgramOption& operator=(const ParameterToProgramOption& rhs) {
    if (this == &rhs) {
      return *this;
    }
    if (rhs._parameters) {
      _parameters = rhs._parameters;
    } else if (_parameters && rhs._value.has_value()) {
      set(rhs._value.value());
    } else {
      _value = rhs._value;
    }
    return *this;
  }

  void set(Type value) {
    AD_CHECK(_parameters);
    _parameters->template set<name>(std::move(value));
  }
  Type get() const {
    AD_CHECK(_parameters);
    return _parameters->template get<name>();
  }

  friend std::ostream& operator<<(std::ostream& stream,
                                  const ParameterToProgramOption& param) {
    return stream << param.get();
  }
};
// This function is required  to use `ParameterToProgramOption` in
// `boost::program_options`.
template <ParameterName name, typename Parameters>
void validate(boost::any& v, const std::vector<std::string>& values,
              ParameterToProgramOption<name, Parameters>*, int) {
  // First parse as a T
  using Param = ParameterToProgramOption<name, Parameters>;
  using Type = typename Param::Type;
  Type* dummy = nullptr;
  using boost::program_options::validate;
  validate(v, values, dummy, 0);

  // Wrap the T inside ParameterToProgramOption
  AD_CHECK(!v.empty());
  v = Param{boost::any_cast<Type>(v)};
}

template <typename ParametersT>
class ParameterToProgramOptionFactory {
 private:
  using Parameters = std::decay_t<ParametersT>;
  Parameters* _parameters;

 public:
  explicit ParameterToProgramOptionFactory(ParametersT* parameters)
      : _parameters{parameters} {
    AD_CHECK(_parameters);
  }

  template <ad_utility::ParameterName name>
  auto makeParameterOption() {
    return ad_utility::ParameterToProgramOption<name, Parameters>{_parameters};
  }

  static auto makePoValue(auto* parameterOption) {
    using P = std::decay_t<decltype(*parameterOption)>;
    return boost::program_options::value<P>(parameterOption)
        ->default_value(*parameterOption);
  }
};

}  // namespace ad_utility

#endif  // QLEVER_PROGRAMOPTIONSHELPERS_H
