// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#pragma once

#include <cstddef>
#include <limits>
#include <memory>
#include <new>
#include <type_traits>

#include "backports/span.h"
#include "util/Exception.h"

namespace qlever::export_pipeline {

// _____________________________________________________________________________
// 64-byte Cache-Line Aligned Batch Buffer.
// Enforces strict 64-byte alignment on ID vectors so sequential batch lookups
// cleanly trigger CPU hardware L2 stream prefetchers and avoid split-cache-line
// penalties.
//
// Not thread-safe: all methods must be called from a single thread unless
// externally synchronized. Call `reserve` before `push_back`; the template
// parameters are checked by `static_assert` (`Alignment` is a power of two
// with `alignof(T) <= sizeof(T) <= Alignment`, `T` is trivially copyable).
template <typename T, size_t Alignment = 64>
class AlignedBatchBuffer {
  static_assert((Alignment & (Alignment - 1)) == 0,
                "Alignment must be a power of two");
  static_assert(Alignment >= alignof(T),
                "Alignment must be at least alignof(T)");
  static_assert(sizeof(T) <= Alignment, "T size must not exceed alignment");
  // `reserve` relocates elements via placement-new copy construction and
  // `push_back` constructs into raw `::operator new[]` storage, both of
  // which are only valid for trivially copyable types.
  static_assert(std::is_trivially_copyable_v<T>,
                "AlignedBatchBuffer requires trivially copyable types");

 public:
  static constexpr size_t kAlignment = Alignment;

 private:
  struct AlignedDeleter {
    void operator()(T* ptr) const noexcept {
      if (ptr != nullptr) {
        ::operator delete[](ptr, std::align_val_t{Alignment});
      }
    }
  };

  std::unique_ptr<T[], AlignedDeleter> data_{nullptr};
  size_t capacity_{0};
  size_t size_{0};

 public:
  AlignedBatchBuffer() noexcept = default;

  explicit AlignedBatchBuffer(size_t capacity) { reserve(capacity); }

  void reserve(size_t newCapacity) {
    if (newCapacity <= capacity_) {
      return;
    }
    // Round the capacity up to whole cache lines. The bit-mask form is only
    // correct when `sizeof(T)` divides `Alignment`, so use division.
    constexpr size_t elementsPerLine = Alignment / sizeof(T);
    // Neither the rounding nor the byte size of the allocation may overflow.
    constexpr size_t maxSize = std::numeric_limits<size_t>::max();
    AD_CONTRACT_CHECK(newCapacity <= maxSize - (elementsPerLine - 1));
    const size_t alignedCapacity =
        (newCapacity + elementsPerLine - 1) / elementsPerLine * elementsPerLine;
    AD_CONTRACT_CHECK(alignedCapacity <= maxSize / sizeof(T));
    T* raw = static_cast<T*>(::operator new[](alignedCapacity * sizeof(T),
                                              std::align_val_t{Alignment}));
    std::unique_ptr<T[], AlignedDeleter> newData(raw);

    // Copy-construct into the fresh storage. `T` is statically constrained to
    // trivially copyable types (see above), so this compiles to a `memcpy`
    // while also starting the lifetime of each element, which plain
    // `std::memcpy` into raw storage does not do in C++17 (implicit object
    // creation is C++20-only).
    std::uninitialized_copy_n(data_.get(), size_, newData.get());
    data_ = std::move(newData);
    capacity_ = alignedCapacity;
  }

  void clear() noexcept { size_ = 0; }

  void push_back(const T& val) {
    AD_CORRECTNESS_CHECK(size_ < capacity_);
    // Placement new starts the element lifetime in the raw storage (see
    // `reserve`); assignment through `T*` would not in C++17. This cannot
    // throw because `T` is trivially copyable (see `static_assert` above).
    ::new (static_cast<void*>(data_.get() + size_)) T(val);
    ++size_;
  }

  [[nodiscard]] ql::span<const T> span() const noexcept {
    return ql::span<const T>(data_.get(), size_);
  }

  [[nodiscard]] ql::span<T> mutableSpan() noexcept {
    return ql::span<T>(data_.get(), size_);
  }

  [[nodiscard]] size_t size() const noexcept { return size_; }
  [[nodiscard]] size_t capacity() const noexcept { return capacity_; }
  [[nodiscard]] const T* data() const noexcept { return data_.get(); }
  [[nodiscard]] T* data() noexcept { return data_.get(); }

  [[nodiscard]] const T& operator[](size_t idx) const noexcept {
    return data_[idx];
  }

  [[nodiscard]] T& operator[](size_t idx) noexcept { return data_[idx]; }
};

}  // namespace qlever::export_pipeline
