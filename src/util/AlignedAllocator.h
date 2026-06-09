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
#include <cstring>
#include <memory>

namespace ad_utility {

// An allocator adaptor that wraps an underlying `Allocator` and guarantees
// that every returned pointer is aligned to at least `MinAlignment` bytes.
// `MinAlignment` must be a power of two.
//
// When `alignof(T) >= MinAlignment`, the underlying allocator is used directly
// (a conforming allocator already guarantees `alignof(T)` alignment). When
// `alignof(T) < MinAlignment`, over-allocation is used: a slightly larger raw
// block is requested from the underlying allocator (rebound to `char`), the
// pointer is advanced to the next `MinAlignment`-aligned address, and the
// original raw pointer is stored in the bytes just before the aligned block so
// that `deallocate` can recover and free the original allocation.
//
// Memory layout for the over-allocation path:
//   [raw]...[raw + sizeof(void*)]...[aligned = next MinAlignment
//   boundary]...[data: n*sizeof(T)]
//                     ↑
//         stored "raw" pointer (via memcpy)
//
// The `rebind<U>` struct correctly propagates `MinAlignment` through
// `std::vector`'s internal rebind mechanism.
template <typename T, typename Allocator = std::allocator<T>,
          size_t MinAlignment = alignof(std::max_align_t)>
class AlignedAllocator {
  static_assert((MinAlignment & (MinAlignment - 1)) == 0,
                "`MinAlignment` must be a power of two.");

  using Traits = std::allocator_traits<Allocator>;
  using ByteAlloc = typename Traits::template rebind_alloc<char>;
  static constexpr bool needsOveralignment = alignof(T) < MinAlignment;
  static constexpr std::size_t overhead = sizeof(void*) + MinAlignment;
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
      // Over-allocate: reserve enough extra bytes to (a) store the original raw
      // pointer in the `sizeof(void*)` bytes immediately preceding the aligned
      // result, and (b) absorb up to `MinAlignment - 1` bytes of alignment
      // padding.
      std::size_t totalBytes = n * sizeof(T) + overhead;
      char* raw =
          std::allocator_traits<ByteAlloc>::allocate(allocator_, totalBytes);

      // Advance past the `sizeof(void*)` header region, then round up to the
      // next `MinAlignment`-aligned address.
      auto rawAddr = reinterpret_cast<std::uintptr_t>(raw);
      auto startAddr = rawAddr + sizeof(void*);
      auto alignedAddr =
          (startAddr + MinAlignment - 1) & ~(std::uintptr_t{MinAlignment - 1});
      T* result = reinterpret_cast<T*>(alignedAddr);

      // Store the original raw pointer in the `sizeof(void*)` bytes before the
      // aligned result so that `deallocate` can recover and free it. Using
      // `memcpy` avoids strict-aliasing UB.
      void* rawVoid = raw;
      std::memcpy(reinterpret_cast<char*>(result) - sizeof(void*), &rawVoid,
                  sizeof(void*));

      return result;
    }
  }

  // Free a block previously returned by `allocate(n)`.
  void deallocate(T* p, std::size_t n) noexcept {
    if constexpr (!needsOveralignment) {
      Traits::deallocate(allocator_, p, n);
    } else {
      std::size_t totalBytes = n * sizeof(T) + overhead;

      // Recover the original raw pointer that was stored by `allocate`.
      void* rawVoid{};
      std::memcpy(&rawVoid, reinterpret_cast<const char*>(p) - sizeof(void*),
                  sizeof(void*));
      char* raw = static_cast<char*>(rawVoid);

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
