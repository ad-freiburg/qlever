// Copyright 2026, The QLever Authors, in particular:
//
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_TEST_UTIL_PMRSTRINGSSOTESTHELPERS_H
#define QLEVER_TEST_UTIL_PMRSTRINGSSOTESTHELPERS_H

#include <absl/strings/str_cat.h>

#include <cstddef>
#include <memory_resource>
#include <string>

#include "backports/memory_resource.h"
#include "util/Log.h"

// There is no `ql::pmr::string`: under the C++17 backports `ql::pmr` aliases
// `boost::container::pmr`, which provides no `string` member. Spelling the
// type via `ql::pmr::polymorphic_allocator` (which exists in both modes) keeps
// this helper compilable with and without the backports. The SSO behavior is
// the STL's in both modes, only the allocator type differs.
using PmrSsoProbeString =
    std::basic_string<char, std::char_traits<char>,
                      ql::pmr::polymorphic_allocator<char>>;

// _____________________________________________________________________________
// Return the largest number of characters that a `PmrSsoProbeString` is
// guaranteed by this helper to store inside its own object storage (SSO).
// See also `clobberStack` in `util/DanglingViewTestHelpers.h`. The SSO
// capacity of `std::basic_string` is implementation-defined (e.g. 15 characters
// for libstdc++ and 22 for libc++), so it is determined here by probing rather
// than hardcoded.
inline size_t pmrStringSsoCapacity() {
  // A counting memory resource lets us detect an allocation directly instead of
  // guessing from pointer addresses: a string uses SSO exactly when
  // constructing it performs no allocation through its allocator.
  struct CountingMemoryResource : public ql::pmr::memory_resource {
   private:
    ql::pmr::memory_resource* upstream_ = ql::pmr::get_default_resource();
    size_t numAllocations_ = 0;

    void* do_allocate(size_t bytes, size_t alignment) override {
      ++numAllocations_;
      return upstream_->allocate(bytes, alignment);
    }
    void do_deallocate(void* ptr, size_t bytes, size_t alignment) override {
      upstream_->deallocate(ptr, bytes, alignment);
    }
    bool do_is_equal(
        const ql::pmr::memory_resource& other) const noexcept override {
      return this == &other;
    }

   public:
    size_t numAllocations() const { return numAllocations_; }
  };
  const std::string sample(sizeof(PmrSsoProbeString), 's');
  for (size_t size = sample.size(); size > 0; --size) {
    CountingMemoryResource resource;
    PmrSsoProbeString pmrSample{sample.data(), size, &resource};
    if (resource.numAllocations() == 0) {
      return size;
    }
  }
  return 0;
}

// _____________________________________________________________________________
// Check the explicit platform premise that `PmrSsoProbeString` stores strings
// of up to `maxSize` characters inside its own object storage (Small String
// Optimization), i.e. that constructing such a string performs no allocation
// through its allocator. Tests whose logic depends on short strings keeping
// their content inline (e.g. dangling-view regression tests) should state
// exactly the sizes they rely on by passing `maxSize`; the failure message
// then points at the platform premise rather than at the test's own logic.
// Preconditions:
// - `maxSize > 0`: there are callers only for non-empty test words.
// NOTE: There is deliberately no default for `maxSize`: the SSO capacity of
// `std::pmr::string` is implementation-defined (e.g. 15 characters for
// libstdc++ and 22 for libc++), so every caller must state exactly the size
// it relies on instead of silently depending on one STL's limit.
inline void requirePmrStringInlineStorage(size_t maxSize) {
  AD_CONTRACT_CHECK(maxSize > 0);
  const size_t capacity = pmrStringSsoCapacity();
  AD_CORRECTNESS_CHECK(
      capacity >= maxSize,
      absl::StrCat("Platform premise violated: PmrSsoProbeString does not "
                   "store ",
                   maxSize,
                   " characters on this platform (capacity: ", capacity, ")"));
}

#endif  // QLEVER_TEST_UTIL_PMRSTRINGSSOTESTHELPERS_H
