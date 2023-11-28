//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_STREAMUTILS_H
#define QLEVER_STREAMUTILS_H

#include <ctre/ctre.h>

#include <boost/lexical_cast.hpp>

namespace ad_utility {

// Wrapper type for std::chrono::duration<> to avoid having to declare
// this in the std::chrono namespace.
template <typename DurationType>
class ParseableDuration {
  DurationType duration_{};

  template <typename CharT>
  friend inline std::basic_istream<CharT>& operator>>(
      std::basic_istream<CharT>& is, ParseableDuration<DurationType>& result) {
    using namespace std::chrono;
    std::basic_string<CharT> arg;

    is >> arg;

    if (auto matcher = ctre::match<"(-?\\d+)(ns|us|ms|s|min|h)">(arg)) {
      auto amount = matcher.template get<1>().to_view();
      auto unit = matcher.template get<2>().to_view();

      auto toDuration = []<typename OriginalDuration>(std::string_view amount) {
        return duration_cast<DurationType>(OriginalDuration{
            boost::lexical_cast<typename OriginalDuration::rep>(amount)});
      };

      if (unit == "ns") {
        result = toDuration.template operator()<nanoseconds>(amount);
      } else if (unit == "us") {
        result = toDuration.template operator()<microseconds>(amount);
      } else if (unit == "ms") {
        result = toDuration.template operator()<milliseconds>(amount);
      } else if (unit == "s") {
        result = toDuration.template operator()<seconds>(amount);
      } else if (unit == "min") {
        result = toDuration.template operator()<minutes>(amount);
      } else if (unit == "h") {
        result = toDuration.template operator()<hours>(amount);
      } else {
        // Unsupported suffix
        AD_FAIL();
      }
    } else {
      is.setstate(std::ios_base::failbit);
    }

    return is;
  }

  template <typename CharT>
  friend inline std::basic_ostream<CharT>& operator<<(
      std::basic_ostream<CharT>& os,
      const ParseableDuration<DurationType>& duration) {
    using namespace std::chrono;
    using period = DurationType::period;

    os << duration.duration_.count();

    if constexpr (std::is_same_v<period, std::nano>) {
      os << "ns";
    } else if constexpr (std::is_same_v<period, std::micro>) {
      os << "us";
    } else if constexpr (std::is_same_v<period, std::milli>) {
      os << "ms";
    } else if constexpr (std::is_same_v<period, std::ratio<1>>) {
      os << "s";
    } else if constexpr (std::is_same_v<period, std::ratio<60>>) {
      os << "min";
    } else if constexpr (std::is_same_v<period, std::ratio<3600>>) {
      os << "h";
    } else {
      static_assert(ad_utility::alwaysFalse<period>,
                    "Unsupported std::chrono::duration period");
    }

    return os;
  }

 public:
  // Implicit conversion is on purpose!
  ParseableDuration() = default;
  ParseableDuration(DurationType duration) : duration_{duration} {}
  template <typename... Args>
  ParseableDuration(Args&&... args) : duration_{AD_FWD(args)...} {}
  operator DurationType() const { return duration_; }
  auto operator<=>(const ParseableDuration&) const noexcept = default;
};
}  // namespace ad_utility

#endif  // QLEVER_STREAMUTILS_H
