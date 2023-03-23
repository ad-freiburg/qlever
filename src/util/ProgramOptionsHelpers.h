//  Copyright 2021, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_PROGRAMOPTIONSHELPERS_H
#define QLEVER_PROGRAMOPTIONSHELPERS_H

#include <util/Concepts.h>
#include <util/Parameters.h>

#include <boost/program_options.hpp>
#include <vector>
namespace ad_utility {

// An implicit wrapper that can be implicitly converted to and from `size_t`.
// When using it as a target value in `boost::program_options` it will only
// accept positive values because of the `validate` function below.
class NonNegative {
 public:
  operator size_t() const { return _value; }
  NonNegative(size_t value) : _value{value} {}
  NonNegative() = default;

 private:
  size_t _value;
};

template <typename Stream, ad_utility::SimilarTo<NonNegative> NN>
Stream& operator<<(Stream& stream, NN&& nonNegative) {
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
  AD_CONTRACT_CHECK(!v.empty());
  v = std::optional<T>(boost::any_cast<T>(v));
}

/// Create `boost::program_options::value`s (command-line options) from
/// `ad_utility::Parameters`
template <typename Parameters>
class ParameterToProgramOptionFactory {
 private:
  Parameters* _parameters;

 public:
  // Construct from a pointer to `ad_utility::Parameters`
  explicit ParameterToProgramOptionFactory(Parameters* parameters)
      : _parameters{parameters} {
    AD_CONTRACT_CHECK(_parameters != nullptr);
  }

  /**
   * Return a `boost::program_option::value` that is connected to the parameter
   * with the given `name`.
   * @tparam name The name of a parameter that is contained in the `Parameters`
   * @return A `boost::program_options::value` with the parameter's current
   * value as the default value. When that value is parsed, the parameter is set
   * to the parsed value.
   */
  template <ad_utility::ParameterName name>
  auto getProgramOption() {
    // Get the current value of the parameter, it will become the default
    // value of the command-line option.
    auto defaultValue = _parameters->template get<name>();

    // The underlying type for the parameter.
    using Type = decltype(defaultValue);

    // The function that is called when the command-line option is called.
    // It sets the parameter to the parsed value.
    auto setParameterToValue{
        [this](const Type& value) { _parameters->template set<name>(value); }};

    return boost::program_options::value<Type>()
        ->default_value(defaultValue)
        ->notifier(setParameterToValue);
  }
};

}  // namespace ad_utility

#endif  // QLEVER_PROGRAMOPTIONSHELPERS_H
