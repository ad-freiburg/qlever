// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/KernelBugWorkaround.h"

#include <absl/strings/str_cat.h>
#include <sys/types.h>

#include <cstdlib>
#include <ctre.hpp>
#include <string>

#include "util/Log.h"

#ifdef __linux__
#include <sys/utsname.h>

// The control interface of `jemalloc`. It is declared weak, so that it is a
// null pointer if QLever doesn't use `jemalloc`. Only declare it on Linux,
// because the bug only exists there, and because the linker of macOS doesn't
// accept an undefined weak symbol.
extern "C" int mallctl(const char* name, void* oldp, size_t* oldlenp,
                       void* newp, size_t newlen) __attribute__((weak));
#endif

namespace ad_utility::kernelBugWorkaround {

namespace {
// The release of a kernel has the form `major.minor[.patch][-rcN][...]`, for
// example `7.0.0-1017-azure` or `7.2.0-rc5`. The regex is defined outside of
// `mayHaveBug` for C++17 compatibility.
constexpr ctll::fixed_string releaseRegex =
    R"((\d+)\.(\d+)(?:\.(\d+))?(?:-rc(\d+))?.*)";

// Return the `mallctl` function of `jemalloc`, or a null pointer if QLever
// doesn't use `jemalloc` or doesn't run on Linux.
auto getMallctl() {
  using Mallctl = int (*)(const char*, void*, size_t*, void*, size_t);
#ifdef __linux__
  return static_cast<Mallctl>(mallctl);
#else
  return static_cast<Mallctl>(nullptr);
#endif
}

}  // namespace

// _____________________________________________________________________________
bool mayHaveBug(std::string_view machine, std::string_view release) {
  if (machine != "aarch64" && machine != "arm64") {
    return false;
  }
  auto match = ctre::match<releaseRegex>(release);
  if (!match || match.get<1>().to_number<int>() != 7) {
    return false;
  }
  int minor = match.get<2>().to_number<int>();
  int patch = match.get<3>() ? match.get<3>().to_number<int>() : 0;
  int releaseCandidate = match.get<4>() ? match.get<4>().to_number<int>() : -1;
  if (minor == 0) {
    return true;
  }
  if (minor == 1) {
    return patch < 9;
  }
  if (minor == 2) {
    return patch == 0 && releaseCandidate >= 0 && releaseCandidate < 7;
  }
  return false;
}

// _____________________________________________________________________________
Status applyIfNeeded(std::string_view machine, std::string_view release,
                     bool disabledByUser) {
  if (!mayHaveBug(machine, release)) {
    return Status::NotNeeded;
  }
  std::string_view fix =
      "https://github.com/torvalds/linux/commit/"
      "478a1c3abebfc717db0d1281a9cdd7befafee542";
  if (disabledByUser) {
    AD_LOG_WARN << "The Linux kernel " << release
                << " may have a bug that causes random stalls, crashes, and "
                   "corrupt data on arm64, unless it contains the fix "
                << fix << ". The workaround is disabled via " << disableEnvVar
                << std::endl;
    return Status::DisabledByUser;
  }
  auto mallctlFunction = getMallctl();
  if (mallctlFunction == nullptr) {
    AD_LOG_WARN << "The Linux kernel " << release
                << " may have a bug that causes random stalls, crashes, and "
                   "corrupt data on arm64, unless it contains the fix "
                << fix
                << ". QLever doesn't use `jemalloc`, so the workaround is not "
                   "available. Please use a kernel with the fix"
                << std::endl;
    return Status::NoJemalloc;
  }
  // The default for arenas that are created later (in particular the arena
  // for huge allocations, which otherwise returns memory immediately).
  ssize_t decay = -1;
  if (mallctlFunction("arenas.dirty_decay_ms", nullptr, nullptr, &decay,
                      sizeof(decay)) != 0) {
    return Status::NoJemalloc;
  }
  // The arenas that already exist. Indices of arenas that are not initialized
  // yield an error, which can be ignored.
  unsigned numArenas = 0;
  size_t size = sizeof(numArenas);
  if (mallctlFunction("arenas.narenas", &numArenas, &size, nullptr, 0) == 0) {
    for (unsigned i = 0; i < numArenas; ++i) {
      std::string name = absl::StrCat("arena.", i, ".dirty_decay_ms");
      mallctlFunction(name.c_str(), nullptr, nullptr, &decay, sizeof(decay));
    }
  }
  AD_LOG_WARN << "The Linux kernel " << release
              << " may have a bug that causes random stalls, crashes, and "
                 "corrupt data on arm64, unless it contains the fix "
              << fix
              << ". As a workaround, QLever doesn't return freed memory to the "
                 "operating system. On a kernel with the fix, you can disable "
                 "the workaround by setting the environment variable "
              << disableEnvVar << std::endl;
  return Status::Applied;
}

// _____________________________________________________________________________
Status applyIfNeeded() {
#ifndef __linux__
  // The bug only exists in the Linux kernel.
  return Status::NotNeeded;
#else
  struct utsname name;
  if (uname(&name) != 0) {
    return Status::NotNeeded;
  }
  bool disabledByUser = std::getenv(std::string{disableEnvVar}.c_str());
  return applyIfNeeded(name.machine, name.release, disabledByUser);
#endif
}

}  // namespace ad_utility::kernelBugWorkaround
