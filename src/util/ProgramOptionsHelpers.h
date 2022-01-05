//  Copyright 2021, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_PROGRAMOPTIONSHELPERS_H
#define QLEVER_PROGRAMOPTIONSHELPERS_H

#include <boost/program_options.hpp>
#include <vector>
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
  if (s[0] == '-') {
    throw std::runtime_error("Expected a positive number but got " + s + ".");
  }

  v = NonNegative{boost::lexical_cast<size_t>(s)};
}

// This function is required  to use `std::optional` in
// `boost::program_options`. It throws an exception when parsing a negative
// value.
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

}  // namespace ad_utility

#endif  // QLEVER_PROGRAMOPTIONSHELPERS_H
