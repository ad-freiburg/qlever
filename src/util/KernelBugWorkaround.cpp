// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/KernelBugWorkaround.h"

#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/strip.h>
#include <sys/types.h>
#include <sys/utsname.h>

#include <cstdlib>
#include <string>
#include <vector>

#include "util/Log.h"

// The control interface of `jemalloc`. It is declared weak, so that it is a
// null pointer if QLever doesn't use `jemalloc`.
extern "C" int mallctl(const char* name, void* oldp, size_t* oldlenp,
                       void* newp, size_t newlen) __attribute__((weak));

namespace ad_utility::kernelBugWorkaround {

namespace {
// Parse the leading decimal number of `s` and remove it from `s`. Return -1 if
// `s` doesn't start with a digit.
int consumeNumber(std::string_view& s) {
  size_t length = 0;
  while (length < s.size() && s[length] >= '0' && s[length] <= '9') {
    ++length;
  }
  int result = -1;
  if (length == 0 || !absl::SimpleAtoi(s.substr(0, length), &result)) {
    return -1;
  }
  s.remove_prefix(length);
  return result;
}

}  // namespace

// _____________________________________________________________________________
bool mayHaveBug(std::string_view machine, std::string_view release) {
  if (machine != "aarch64" && machine != "arm64") {
    return false;
  }
  // The release has the form `major.minor[.patch][-rcN][...]`, for example
  // `7.0.0-1017-azure` or `7.2.0-rc5`.
  int major = consumeNumber(release);
  if (major != 7 || !absl::ConsumePrefix(&release, ".")) {
    return false;
  }
  int minor = consumeNumber(release);
  int patch = absl::ConsumePrefix(&release, ".") ? consumeNumber(release) : 0;
  int releaseCandidate =
      absl::ConsumePrefix(&release, "-rc") ? consumeNumber(release) : -1;
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
  if (mallctl == nullptr) {
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
  if (mallctl("arenas.dirty_decay_ms", nullptr, nullptr, &decay,
              sizeof(decay)) != 0) {
    return Status::NoJemalloc;
  }
  // The arenas that already exist. Indices of arenas that are not initialized
  // yield an error, which can be ignored.
  unsigned numArenas = 0;
  size_t size = sizeof(numArenas);
  if (mallctl("arenas.narenas", &numArenas, &size, nullptr, 0) == 0) {
    for (unsigned i = 0; i < numArenas; ++i) {
      std::string name = absl::StrCat("arena.", i, ".dirty_decay_ms");
      mallctl(name.c_str(), nullptr, nullptr, &decay, sizeof(decay));
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
  struct utsname name;
  if (uname(&name) != 0) {
    return Status::NotNeeded;
  }
  bool disabledByUser = std::getenv(std::string{disableEnvVar}.c_str());
  return applyIfNeeded(name.machine, name.release, disabledByUser);
}

}  // namespace ad_utility::kernelBugWorkaround
