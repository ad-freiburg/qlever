// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)

#pragma once

namespace ad_utility {
// A mutex that can be "copied". The semantics are that copying will create a
// new mutex. This is useful when we just want to make a `const` member function
// that modifies a `mutable` member threadsafe. Note that a copy-constructed
// `CopyableMutex` will be unlocked, even if the source was locked, and that
// copy assignment is a noop.
struct CopyableMutex : std::mutex {
  using std::mutex::mutex;
  CopyableMutex(const CopyableMutex&) noexcept {}
  CopyableMutex& operator=(const CopyableMutex&) noexcept { return *this; }
};

// A `std::atomic` that can be "copied". The semantics are, that copying will
// create a new atomic that is initialized with the value being copied. This is
// useful if we want to store an atomic as a class member, but still want the
// class to be copyable.
template <typename T>
class CopyableAtomic : public std::atomic<T> {
  using Base = std::atomic<T>;

 public:
  using Base::Base;
  CopyableAtomic(const CopyableAtomic& rhs) : Base{rhs.load()} {}
  CopyableAtomic& operator=(const CopyableAtomic& rhs) {
    static_cast<Base&>(*this) = rhs.load();
    return *this;
  }

  CopyableAtomic(CopyableAtomic&& rhs) noexcept : Base{rhs.load()} {}
  CopyableAtomic& operator=(CopyableAtomic&& rhs) noexcept {
    static_cast<Base&>(*this) = rhs.load();
    return *this;
  }
};
}  // namespace ad_utility
