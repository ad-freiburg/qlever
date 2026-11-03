// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_STABLEHASHSET_H
#define QLEVER_SRC_UTIL_STABLEHASHSET_H

#include <absl/container/flat_hash_set.h>

#include <cstddef>
#include <exception>
#include <iterator>
#include <new>
#include <type_traits>
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "backports/memory_resource.h"
#include "util/Exception.h"
#include "util/NoCopyNoMove.h"

namespace ad_utility {

namespace stableHashSet::detail {

// The state that is shared by all copies of an `IndexAllocator` (see below).
struct IndexAllocationState {
  // The resource from which the index is allocated.
  ql::pmr::memory_resource* resource_;
  // The first exception that was thrown by the `resource_` since it was last
  // reset (see `StableHashSet::insertWith`).
  std::exception_ptr exception_;
  // The blocks that were allocated from the default resource instead of the
  // `resource_`, because the `resource_` threw. This is (almost) always empty.
  std::vector<void*> fallbackBlocks_;
};

// The hash sets of `absl` are left in a corrupted state when their allocator
// throws. This allocator therefore never throws (unless the default resource
// does, i.e. unless the process runs out of memory): if the `resource_` throws
// (e.g. because a memory limit is exceeded), the memory is allocated from the
// default resource instead, and the exception is stored, such that it can be
// rethrown as soon as the hash set is in a consistent state again.
template <typename T>
class IndexAllocator {
  template <typename>
  friend class IndexAllocator;
  IndexAllocationState* state_;

 public:
  using value_type = T;
  explicit IndexAllocator(IndexAllocationState* state) : state_{state} {}
  template <typename U>
  IndexAllocator(const IndexAllocator<U>& other) : state_{other.state_} {}

  T* allocate(size_t n) {
    try {
      return static_cast<T*>(
          state_->resource_->allocate(n * sizeof(T), alignof(T)));
    } catch (...) {
      if (state_->exception_ == nullptr) {
        state_->exception_ = std::current_exception();
      }
    }
    void* block =
        ql::pmr::get_default_resource()->allocate(n * sizeof(T), alignof(T));
    state_->fallbackBlocks_.push_back(block);
    return static_cast<T*>(block);
  }

  void deallocate(T* p, size_t n) {
    auto& fallback = state_->fallbackBlocks_;
    auto it = fallback.empty()
                  ? fallback.end()
                  : ql::ranges::find(fallback, static_cast<void*>(p));
    if (it == fallback.end()) {
      state_->resource_->deallocate(p, n * sizeof(T), alignof(T));
    } else {
      fallback.erase(it);
      ql::pmr::get_default_resource()->deallocate(p, n * sizeof(T), alignof(T));
    }
  }

  template <typename U>
  bool operator==(const IndexAllocator<U>& other) const {
    return state_ == other.state_;
  }
  template <typename U>
  bool operator!=(const IndexAllocator<U>& other) const {
    return !(*this == other);
  }
};

}  // namespace stableHashSet::detail

// A hash set the elements of which never move, so pointers to them remain
// valid until the set is destroyed. Elements can't be erased. All the memory
// is allocated from a single `memory_resource` (the upstream), which may throw
// (e.g. when a memory limit is exceeded): then the insertion that caused the
// allocation has no effect (strong exception guarantee).
//
// The elements are stored in a monotonic arena (which is also available for
// additional data of the elements, e.g. the characters of a string view, see
// `insertWith`), and the index is an `absl::flat_hash_set` of pointers to
// them. This is faster and needs less memory than a node-based set (like
// `std::unordered_set` or `absl::node_hash_set`).
//
// `Hash` and `Equal` must accept `T`s. To look up elements via other key types
// (see `find` and `insertWith`), they also have to accept those (in any
// combination with `T` for `Equal`) and define `is_transparent`.
template <typename T, typename Hash, typename Equal>
class StableHashSet : ad_utility::NoCopyNoMove {
 private:
  using IndexAllocationState = stableHashSet::detail::IndexAllocationState;
  using IndexAllocator = stableHashSet::detail::IndexAllocator<const T*>;

  // Dereference the pointers of the index, and leave all other keys as is.
  static const T& deref(const T* element) { return *element; }
  template <typename Key>
  static const Key& deref(const Key& key) {
    return key;
  }
  struct IndexHash {
    using is_transparent = void;
    template <typename Key>
    size_t operator()(const Key& key) const {
      return Hash{}(deref(key));
    }
  };
  struct IndexEqual {
    using is_transparent = void;
    template <typename A, typename B>
    bool operator()(const A& a, const B& b) const {
      return Equal{}(deref(a), deref(b));
    }
  };

  // The arena that stores the elements (and their additional data). It is
  // declared before the `index_`, which points into it. The arena only frees
  // its memory when it is destroyed, which is fine because elements are never
  // erased.
  ql::pmr::monotonic_buffer_resource arena_;
  IndexAllocationState indexAllocationState_;
  absl::flat_hash_set<const T*, IndexHash, IndexEqual, IndexAllocator> index_;

 public:
  // Create an empty set, all the memory of which is allocated from `upstream`,
  // which has to outlive the set.
  explicit StableHashSet(
      ql::pmr::memory_resource* upstream = ql::pmr::get_default_resource())
      : arena_{upstream},
        indexAllocationState_{upstream, nullptr, {}},
        index_{0, IndexHash{}, IndexEqual{},
               IndexAllocator{&indexAllocationState_}} {}

  ~StableHashSet() {
    if constexpr (!std::is_trivially_destructible_v<T>) {
      for (const T* element : index_) {
        element->~T();
      }
    }
  }

  // Return the element that is equal to `key`, or `nullptr` if there is none.
  template <typename Key>
  const T* find(const Key& key) const {
    auto it = index_.find(key);
    return it == index_.end() ? nullptr : *it;
  }

  // If an element that is equal to `key` is contained, return it and `false`.
  // Else insert `makeElement(arena)` and return the inserted element and
  // `true`. `makeElement` gets the `memory_resource&` of the arena, from which
  // it may allocate additional data that has to live as long as the element.
  // `makeElement(arena)` has to be equal to `key`.
  //
  // If an exception is thrown (by `makeElement` or because an allocation
  // fails), the set remains unchanged. The memory that was already allocated
  // from the arena remains (unreachable) in it.
  template <typename Key, typename MakeElement>
  std::pair<const T*, bool> insertWith(const Key& key,
                                       MakeElement&& makeElement) {
    if (const T* existing = find(key); existing != nullptr) {
      return {existing, false};
    }
    // Everything that can throw happens before the `index_` is modified.
    ql::pmr::memory_resource& arena = arena_;
    T value = std::forward<MakeElement>(makeElement)(arena);
    void* memory = arena_.allocate(sizeof(T), alignof(T));
    const T* element = new (memory) T(std::move(value));
    AD_EXPENSIVE_CHECK(IndexEqual{}(element, key));
    index_.insert(element);
    // If the upstream threw while the `index_` grew, the `index_` is still
    // consistent (see `IndexAllocator`), but the insertion has to be undone.
    if (auto exception =
            std::exchange(indexAllocationState_.exception_, nullptr);
        exception != nullptr) {
      index_.erase(element);
      element->~T();
      std::rethrow_exception(exception);
    }
    return {element, true};
  }

  // Insert a copy of `value` if no equal element is contained, see
  // `insertWith`.
  std::pair<const T*, bool> insert(const T& value) {
    return insertWith(
        value, [&value](ql::pmr::memory_resource&) -> T { return value; });
  }

  size_t size() const { return index_.size(); }
  bool empty() const { return index_.empty(); }

  // Iterate over the elements (as `const T&`) in no particular order.
  class Iterator {
    using Base = typename decltype(index_)::const_iterator;
    Base it_;

   public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = T;
    using difference_type = std::ptrdiff_t;
    using pointer = const T*;
    using reference = const T&;
    Iterator() = default;
    explicit Iterator(Base it) : it_{it} {}
    reference operator*() const { return **it_; }
    pointer operator->() const { return *it_; }
    Iterator& operator++() {
      ++it_;
      return *this;
    }
    Iterator operator++(int) {
      auto copy = *this;
      ++it_;
      return copy;
    }
    bool operator==(const Iterator& other) const { return it_ == other.it_; }
    bool operator!=(const Iterator& other) const { return it_ != other.it_; }
  };
  Iterator begin() const { return Iterator{index_.begin()}; }
  Iterator end() const { return Iterator{index_.end()}; }
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_STABLEHASHSET_H
