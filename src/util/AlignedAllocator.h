// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_ALIGNEDALLOCATOR_H
#define QLEVER_ALIGNEDALLOCATOR_H

#include <cstddef>
#include <cstdint>
#include <memory>

namespace ad_utility {

// An allocator adaptor that wraps an underlying `Allocator` and guarantees
// that every returned pointer is aligned to at least `MinAlignment` bytes.
// `MinAlignment` must be a power of two between 1 and 256.
//
// When `alignof(T) >= MinAlignment`, the underlying allocator is used directly
// (a conforming allocator already guarantees `alignof(T)` alignment). When
// `alignof(T) < MinAlignment`, `MinAlignment` extra bytes are allocated to
// provide alignment padding and one byte of metadata that lets `deallocate`
// recover the start of the raw allocation.
//
// The `rebind<U>` struct correctly propagates `MinAlignment` through
// `std::vector`'s internal rebind mechanism.
template <typename T, typename Allocator = std::allocator<T>,
          size_t MinAlignment = alignof(std::max_align_t)>
class AlignedAllocator {
  static_assert(MinAlignment > 0 && MinAlignment <= 256,
                "`MinAlignment` must be between 1 and 256.");
  static_assert((MinAlignment & (MinAlignment - 1)) == 0,
                "`MinAlignment` must be a power of two.");

  using Traits = std::allocator_traits<Allocator>;
  using ByteAlloc = typename Traits::template rebind_alloc<char>;
  static constexpr bool needsOveralignment = alignof(T) < MinAlignment;
  // Store `ByteAlloc` on the over-allocation path (the only allocator used
  // there), or the raw `Allocator` on the direct path, so no rebind is needed
  // at call time.
  using StoredAlloc =
      std::conditional_t<needsOveralignment, ByteAlloc, Allocator>;
  StoredAlloc allocator_;

 public:
  using value_type = T;
  using size_type = typename Traits::size_type;
  using difference_type = typename Traits::difference_type;
  using propagate_on_container_copy_assignment =
      typename Traits::propagate_on_container_copy_assignment;
  using propagate_on_container_move_assignment =
      typename Traits::propagate_on_container_move_assignment;
  using propagate_on_container_swap =
      typename Traits::propagate_on_container_swap;

  // The `rebind` struct specifies how to cast an allocator to a different value
  // type. Since `AlignedAllocator` has three template arguments we need to make
  // explicit here how to do this.
  template <typename U>
  struct rebind {
    using other = AlignedAllocator<U, typename Traits::template rebind_alloc<U>,
                                   MinAlignment>;
  };

  AlignedAllocator() = default;
  explicit AlignedAllocator(const Allocator& a) : allocator_(a) {}
  explicit AlignedAllocator(Allocator&& a) : allocator_(std::move(a)) {}

  AlignedAllocator(const AlignedAllocator&) = default;
  AlignedAllocator(AlignedAllocator&&) noexcept = default;
  AlignedAllocator& operator=(const AlignedAllocator&) = default;
  AlignedAllocator& operator=(AlignedAllocator&&) noexcept = default;

  // Converting constructor used during `rebind` (e.g. when `std::vector`
  // internally reuses the allocator for a different element type).
  template <typename U, typename OtherAlloc>
  explicit AlignedAllocator(
      const AlignedAllocator<U, OtherAlloc, MinAlignment>& other)
      : allocator_(other.allocator_) {}

  // Allocate `n` objects of type `T` with at least `MinAlignment` byte
  // alignment. When `alignof(T) >= MinAlignment` the underlying allocator is
  // used directly. Otherwise over-allocation is used to guarantee alignment.
  T* allocate(std::size_t n) {
    if constexpr (!needsOveralignment) {
      return Traits::allocate(allocator_, n);
    } else {
      std::size_t totalBytes = n * sizeof(T) + MinAlignment;
      char* raw =
          std::allocator_traits<ByteAlloc>::allocate(allocator_, totalBytes);

      // Round up to the next `MinAlignment`-aligned address, leaving at least
      // one byte before it to store the offset metadata.
      auto rawAddr = reinterpret_cast<std::uintptr_t>(raw);

      // `bitMask` has zeros in the lower bits where a properly aligned address
      // needs to have zeros and ones in all other bits. A bitwise and with this
      // mask will round down to the nearest aligned address.
      static constexpr auto bitmask = ~(std::uintptr_t{MinAlignment - 1});

      // We have to add at least one byte to our address for the offset storage.
      // Adding `MinAlignment` and then rounding down to an aligned address
      // gives us the smallest address that is 1. larger than `rawAddr` and 2.
      // properly aligned.
      auto alignedAddr = (rawAddr + MinAlignment) & bitmask;
      T* result = reinterpret_cast<T*>(alignedAddr);

      // Store `offset - 1` as a `uint8_t` in the byte immediately before the
      // aligned result, so that `deallocate` can recover the original start.
      auto offsetMinus1 = static_cast<uint8_t>(alignedAddr - rawAddr - 1);
      static_assert(sizeof(offsetMinus1 == 1));
      auto addrBefore = reinterpret_cast<char*>(result) - 1;
      memcpy(addrBefore, &offsetMinus1, 1);

      return result;
    }
  }

  // Free a block previously returned by `allocate(n)`.
  void deallocate(T* p, std::size_t n) noexcept {
    if constexpr (!needsOveralignment) {
      Traits::deallocate(allocator_, p, n);
    } else {
      std::size_t totalBytes = n * sizeof(T) + MinAlignment;

      // Recover the original allocation start from the offset byte stored by
      // `allocate`.
      auto addrBefore = reinterpret_cast<const char*>(p) - 1;
      auto storedOffset = absl::bit_cast<std::uint8_t>(*addrBefore);
      char* raw = reinterpret_cast<char*>(p) -
                  (static_cast<std::size_t>(storedOffset) + 1);

      std::allocator_traits<ByteAlloc>::deallocate(allocator_, raw, totalBytes);
    }
  }

  bool operator==(const AlignedAllocator& other) const {
    return allocator_ == other.allocator_;
  }
  bool operator!=(const AlignedAllocator& other) const {
    return !(*this == other);
  }

  // Grant access to `allocator_` for the converting constructor above.
  template <typename U, typename OtherAlloc, size_t M>
  friend class AlignedAllocator;
};

}  // namespace ad_utility

#endif  // QLEVER_ALIGNEDALLOCATOR_H
