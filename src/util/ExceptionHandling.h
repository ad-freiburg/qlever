//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include <concepts>
#include <exception>
#include <iostream>
#include <stdexcept>
#include <type_traits>

#include "absl/strings/str_cat.h"
#include "util/Forward.h"
#include "util/Log.h"
#include "util/SourceLocation.h"

namespace ad_utility {
namespace detail {
[[maybe_unused]] static constexpr auto callStdTerminate = []() noexcept {
  std::terminate();
};
}
// Call `f()`. If this call throws, catch the exception and log it, but do not
// propagate it. Can be used to make destructors `noexcept` when the destructor
// has to perform actions that might throw, but when handling these exceptions
// is not important.
template <typename F>
requires std::invocable<std::remove_cvref_t<F>>
void ignoreExceptionIfThrows(F&& f,
                             std::string_view additionalNote = "") noexcept {
  if constexpr (std::is_nothrow_invocable_v<std::remove_cvref_t<F>>) {
    std::invoke(AD_FWD(f));
    return;
  }
  try {
    std::invoke(AD_FWD(f));
  } catch (const std::exception& e) {
    LOG(INFO) << "Ignored an exception. The exception message was:\""
              << e.what() << "\". " << additionalNote << std::endl;
  } catch (...) {
    LOG(INFO) << "Ignored an exception of an unknown type. " << additionalNote
              << std::endl;
  }
}

// Call `f()`. If this call throws, then terminate the program after logging an
// error message that includes the `message`, information on the thrown
// exception, and the location of the call. This can be used to make destructors
// `noexcept()` that have to perform some non-trivial logic (e.g. writing a
// trailer to a file), when such a failure should never occur in practice and
// also is not easily recoverable. For an example usage see `PatternCreator.h`.
// The actual termination call can be configured for testing purposes. Note that
// this function must never throw an exception.
template <typename F,
          typename TerminateAction = decltype(detail::callStdTerminate)>
requires(std::invocable<std::remove_cvref_t<F>> &&
         std::is_nothrow_invocable_v<TerminateAction>)
void terminateIfThrows(F&& f, std::string_view message,
                       TerminateAction terminateAction = {},
                       ad_utility::source_location l =
                           ad_utility::source_location::current()) noexcept {
  auto getErrorMessage =
      [&message, &l](const auto&... additionalMessages) -> std::string {
    return absl::StrCat(
        "A function that should never throw has thrown an exception",
        additionalMessages..., ". The function was called in file ",
        l.file_name(), " on line ", l.line(),
        ". Additional information: ", message,
        ". Please report this. Terminating");
  };

  auto logAndTerminate = [&terminateAction](std::string_view msg) {
    try {
      LOG(ERROR) << msg << std::endl;
      std::cerr << msg << std::endl;
    } catch (...) {
      std::cerr << msg << std::endl;
    }
    terminateAction();
  };
  try {
    std::invoke(AD_FWD(f));
  } catch (const std::exception& e) {
    auto msg = getErrorMessage(" with message \"", e.what(), "\"");
    logAndTerminate(msg);
  } catch (...) {
    auto msg = getErrorMessage();
    logAndTerminate(msg);
  }
}
}  // namespace ad_utility
