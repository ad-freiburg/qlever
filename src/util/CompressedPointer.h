//  Copyright 2025, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_UTIL_COMPRESSEDPOINTER_H
#define QLEVER_SRC_UTIL_COMPRESSEDPOINTER_H

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <type_traits>

#include "util/Exception.h"

namespace ad_utility {

// A compressed pointer that uses the lower bits of an aligned pointer to store
// a boolean flag. The pointed-to type `T` must have an alignment of at least 2
// (which gives us 1 bit to use for the flag).
//
// This class is thread-safe when used with atomic operations (via the
// CopyableAtomic wrapper).
template <typename T, size_t Alignment = alignof(T)>
class CompressedPointer {
 private:
  static_assert(Alignment >= 2,
                "Alignment must be at least 2 to have a free bit");
  static_assert((Alignment & (Alignment - 1)) == 0,
                "Alignment must be a power of 2");

  // The mask for extracting the flag bit (LSB)
  static constexpr uintptr_t kFlagMask = 1;
  // The mask for extracting the pointer
  static constexpr uintptr_t kPointerMask = ~kFlagMask;

  uintptr_t data_ = 0;

 public:
  // Default constructor: null pointer with flag = false
  constexpr CompressedPointer() = default;

  // Construct from a pointer and a flag
  constexpr CompressedPointer(T* ptr, bool flag = false) {
    set(ptr, flag);
  }

  // Get the pointer
  T* getPointer() const {
    return reinterpret_cast<T*>(data_ & kPointerMask);
  }

  // Get the flag
  bool getFlag() const { return (data_ & kFlagMask) != 0; }

  // Set both pointer and flag
  void set(T* ptr, bool flag) {
    uintptr_t ptrValue = reinterpret_cast<uintptr_t>(ptr);
    // Verify that the pointer is properly aligned
    AD_CONTRACT_CHECK((ptrValue & kFlagMask) == 0,
                      "Pointer is not properly aligned");
    data_ = ptrValue | (flag ? kFlagMask : 0);
  }

  // Set only the pointer (keep the flag unchanged)
  void setPointer(T* ptr) { set(ptr, getFlag()); }

  // Set only the flag (keep the pointer unchanged)
  void setFlag(bool flag) { set(getPointer(), flag); }

  // Comparison operators
  bool operator==(const CompressedPointer& other) const {
    return data_ == other.data_;
  }
  bool operator!=(const CompressedPointer& other) const {
    return !(*this == other);
  }

  // For use with std::atomic
  CompressedPointer(const CompressedPointer& other) = default;
  CompressedPointer& operator=(const CompressedPointer& other) = default;
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_COMPRESSEDPOINTER_H
