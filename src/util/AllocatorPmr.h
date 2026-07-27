// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Bayerische Motoren Werke Aktiengesellschaft (BMW AG)
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.
//
// A `ql::pmr`-based allocator backend for QLever. This mirrors the public
// surface of `ad_utility::AllocatorWithLimit` (see `AllocatorWithLimit.h`) so
// that it can be selected as a drop-in replacement behind the
// `qlever::Allocator<T>` seam (see `Allocator.h`).
//
// The design goals (see `foresight-notes/qlever-allocator`):
//   * Route all allocations through a `ql::pmr::memory_resource` so the target
//     platform can inject its own memory pools (e.g. "Hi" / "Lo").
//   * Keep QLever's existing *memory-limit* semantics by default: a
//     `LimitedMemoryResource` counts bytes and throws when a configured limit
//     is exceeded, exactly like `AllocatorWithLimit`.
//   * Allow the platform to instead supply a plain upstream resource with no
//     limit enforcement.

#ifndef QLEVER_SRC_UTIL_ALLOCATORPMR_H
#define QLEVER_SRC_UTIL_ALLOCATORPMR_H

#include <cstddef>
#include <memory>

#include "backports/memory_resource.h"
#include "util/MemoryLimitTracker.h"
#include "util/MemorySize/MemorySize.h"

namespace ad_utility {

// A `ql::pmr::memory_resource` that forwards all allocations to an *upstream*
// resource while enforcing a global memory limit. When an allocation would
// exceed the remaining budget, an optional `ClearOnAllocation` callback is
// invoked (e.g. to evict caches) and, if that does not free enough memory, an
// `AllocationExceedsLimitException` is thrown - identical behaviour to
// `AllocatorWithLimit`.
//
// Instances are shared (via `std::shared_ptr`, see the factory functions
// below) so that all allocators referring to the same resource count against
// the same budget. Access to the counter is synchronized.
class LimitedMemoryResource : public ql::pmr::memory_resource {
 private:
  detail::MemoryLimitTracker tracker_;
  ql::pmr::memory_resource* upstream_;

 public:
  LimitedMemoryResource(
      MemorySize limit, ql::pmr::memory_resource* upstream,
      ClearOnAllocation clearOnAllocation = noClearOnAllocation)
      : tracker_{limit, std::move(clearOnAllocation)},
        upstream_{upstream == nullptr ? ql::pmr::get_default_resource()
                                      : upstream} {}

  // Number of bytes still available for allocation.
  [[nodiscard]] MemorySize amountMemoryLeft() const {
    return tracker_.amountMemoryLeft();
  }

  ql::pmr::memory_resource* upstream() const { return upstream_; }

 protected:
  void* do_allocate(std::size_t bytes, std::size_t alignment) override {
    tracker_.reserveOrThrow(MemorySize::bytes(bytes));
    // If the upstream fails to satisfy the request (e.g. an injected bounded
    // arena that throws when exhausted), release the bytes we just reserved so
    // the tracker does not stay permanently over-counted.
    try {
      return upstream_->allocate(bytes, alignment);
    } catch (...) {
      tracker_.release(MemorySize::bytes(bytes));
      throw;
    }
  }

  void do_deallocate(void* p, std::size_t bytes,
                     std::size_t alignment) override {
    upstream_->deallocate(p, bytes, alignment);
    tracker_.release(MemorySize::bytes(bytes));
  }

  bool do_is_equal(
      const ql::pmr::memory_resource& other) const noexcept override {
    return this == &other;
  }
};

// A `ql::pmr`-based allocator that mirrors the public surface of
// `AllocatorWithLimit<T>` so it can be swapped in behind `qlever::Allocator`.
//
// It behaves like a stateful, propagating allocator over a
// `ql::pmr::memory_resource` (it holds a *pointer* to the resource and, on
// copy/move/swap, that pointer travels with the container - matching
// `AllocatorWithLimit`), and additionally
//   * keeps the referenced resource alive via an optional owning
//     `shared_ptr` (so factory-created resources are not dangling), and
//   * exposes `amountMemoryLeft()` and `.as<U>()`, which parts of the engine
//     rely on.
template <typename T>
class PmrAllocator {
 private:
  // The allocator is represented by two members that intentionally play
  // different roles:
  //
  //   * `resource_` (raw pointer) is the resource actually used for every
  //     `allocate`/`deallocate`. This mirrors `ql::pmr::polymorphic_allocator`,
  //     which stores only a non-owning `memory_resource*` and relies on the
  //     caller to keep the resource alive.
  //
  //   * `owner_` (shared pointer) *optionally* co-owns that resource, purely to
  //     manage its lifetime. Ownership is a per-instance (runtime) property, not
  //     a separate type, because the engine bakes a single allocator type into
  //     its container types (e.g. `IdTable`'s `DefaultAllocator`): an
  //     owning and a non-owning allocator must remain interchangeable as the
  //     same `qlever::Allocator<T>`.
  //
  // Two cases:
  //
  //   1. QLever-created resources (via the `makePmrAllocator*` factories, e.g.
  //      the default construction path). These `LimitedMemoryResource`s have no
  //      external owner, yet the allocator is copied into many long-lived
  //      containers that outlive the factory call. Here `owner_` holds the
  //      `shared_ptr` so the resource stays alive for as long as any copy of the
  //      allocator (and therefore any buffer allocated from it) exists. This is
  //      the PMR equivalent of how `AllocatorWithLimit` shares its
  //      `AllocationMemoryLeft` state via a `shared_ptr`.
  //
  //   2. Platform-provided resources (via `makePmrAllocatorFromResource`, e.g.
  //      an externally injected pool). Their lifetime is owned and guaranteed by
  //      the platform (typically static), so QLever must NOT take ownership: in
  //      this case `owner_ == nullptr` and only the raw `resource_` pointer is
  //      used - exactly the standard non-owning `polymorphic_allocator`
  //      contract.
  std::shared_ptr<ql::pmr::memory_resource> owner_;
  ql::pmr::memory_resource* resource_;

  template <typename U>
  friend class PmrAllocator;

 public:
  using value_type = T;

  // Match `AllocatorWithLimit`'s propagation semantics: the allocator travels
  // with the container on copy-assignment, move-assignment and swap. This is
  // required because QLever's engine freely swaps and move-assigns `IdTable`s
  // (and other allocator-aware containers) that may hold *different* resources;
  // if the allocator did not propagate, a container swap with unequal resources
  // would be undefined behaviour (a buffer allocated by one resource would be
  // deallocated through another), which manifests as segfaults on destruction.
  // Propagating keeps every buffer paired with the resource that allocated it,
  // which is exactly what we need for injected platform pools too.
  using propagate_on_container_copy_assignment = std::true_type;
  using propagate_on_container_move_assignment = std::true_type;
  using propagate_on_container_swap = std::true_type;

  // Construct from a non-owning resource pointer: the resource is owned and
  // kept alive elsewhere (e.g. a platform-provided pool). `owner_` stays
  // null and QLever never deletes the resource.
  explicit PmrAllocator(ql::pmr::memory_resource* resource)
      : owner_{nullptr}, resource_{resource} {}

  // Construct from an owning resource: QLever co-owns it via `owner_` so it is
  // kept alive for all copies of this allocator (used by the factories that
  // create a `LimitedMemoryResource` on the fly).
  explicit PmrAllocator(std::shared_ptr<ql::pmr::memory_resource> resource)
      : owner_{std::move(resource)}, resource_{owner_.get()} {}

  PmrAllocator() = delete;

  // Rebinding copy constructor (STL requirement + `default_init_allocator`).
  template <typename U>
  PmrAllocator(const PmrAllocator<U>& other) noexcept
      : owner_{other.owner_}, resource_{other.resource_} {}

  PmrAllocator(const PmrAllocator&) = default;
  PmrAllocator& operator=(const PmrAllocator&) = default;

  // Move construction/assignment deliberately *copy* (rather than steal) the
  // owning `shared_ptr`. Stealing it would leave the moved-from allocator with
  // a dangling `resource_` but a null `owner_`; if that moved-from allocator is
  // subsequently reused to allocate (e.g. an emptied `IdTable` column that is
  // filled again) it would produce a buffer whose deallocation no longer keeps
  // the resource alive, leading to a use-after-free of the `memory_resource`
  // when the buffer is finally freed. By keeping `owner_` intact, *every*
  // allocator that ever referred to an owned resource keeps that resource alive
  // for its whole lifetime. This can only extend a resource's lifetime, never
  // shorten it, so the resource always outlives all buffers allocated from it.
  PmrAllocator(PmrAllocator&& other) noexcept
      : owner_{other.owner_}, resource_{other.resource_} {}
  PmrAllocator& operator=(PmrAllocator&& other) noexcept {
    owner_ = other.owner_;
    resource_ = other.resource_;
    return *this;
  }

  // Obtain an allocator for another type sharing the same resource.
  template <typename U>
  PmrAllocator<U> as() const { return PmrAllocator<U>{*this}; }

  T* allocate(std::size_t n) {
    return static_cast<T*>(
        resource_->allocate(n * sizeof(T), alignof(T)));
  }

  void deallocate(T* p, std::size_t n) {
    resource_->deallocate(p, n * sizeof(T), alignof(T));
  }

  // Bytes still available. If the underlying resource is a
  // `LimitedMemoryResource` its budget is reported, otherwise "unlimited".
  [[nodiscard]] MemorySize amountMemoryLeft() const {
    if (auto* limited = dynamic_cast<LimitedMemoryResource*>(resource_)) {
      return limited->amountMemoryLeft();
    }
    return MemorySize::max();
  }

  ql::pmr::memory_resource* resource() const { return resource_; }

  // Two allocators are equal iff they refer to the same resource.
  template <typename V>
  bool operator==(const PmrAllocator<V>& v) const {
    return resource_ == v.resource_;
  }
  template <typename V>
  bool operator!=(const PmrAllocator<V>& v) const {
    return !(*this == v);
  }

  // Unlimited: wrap the (non-owning) default resource directly. No limit
  // tracking and no extra allocation for a LimitedMemoryResource.
  static PmrAllocator makeUnlimited() {
    return PmrAllocator{ql::pmr::get_default_resource()};
  }
  // Limited: create a LimitedMemoryResource over the default upstream.
  static PmrAllocator makeLimited(
      MemorySize limit,
      ClearOnAllocation clearOnAllocation = noClearOnAllocation) {
    // The `LimitedMemoryResource` control block is intentionally sourced from
    // the global heap via `make_shared` rather than from the upstream via
    // `allocate_shared`: the upstream here is always the default resource, and
    // the control block is a few tens of bytes of engine bookkeeping that must
    // not be charged against the memory limit. See `makePmrAllocatorWithLimit`
    // for the (also deliberately global-heap) custom-upstream case.
    auto resource = std::make_shared<LimitedMemoryResource>(
        limit, ql::pmr::get_default_resource(), std::move(clearOnAllocation));
    return PmrAllocator{
        std::static_pointer_cast<ql::pmr::memory_resource>(std::move(resource))};
  }
};

// Create a `PmrAllocator` backed by a fresh `LimitedMemoryResource`
// with the given limit over the given upstream (default: new/delete).
template <typename T>
PmrAllocator<T> makePmrAllocatorWithLimit(
    MemorySize limit,
    ql::pmr::memory_resource* upstream = ql::pmr::get_default_resource(),
    ClearOnAllocation clearOnAllocation = noClearOnAllocation) {
  // The control block is deliberately sourced from the global heap (not from
  // `upstream` via `allocate_shared`): its few tens of bytes are engine
  // bookkeeping that should live and die independently of the (possibly
  // bounded) upstream arena and must not be charged against `limit`.
  auto resource = std::make_shared<LimitedMemoryResource>(
      limit, upstream, std::move(clearOnAllocation));
  return PmrAllocator<T>{
      std::static_pointer_cast<ql::pmr::memory_resource>(std::move(resource))};
}

// Create an unlimited `PmrAllocator` over the default resource.
template <typename T>
PmrAllocator<T> makeUnlimitedPmrAllocator() {
  return PmrAllocator<T>::makeUnlimited();
}

// Create a `PmrAllocator` from a platform-provided resource without any limit
// enforcement (non-owning). QLever never takes ownership of `resource`; the
// caller must keep it alive for at least as long as any container using the
// returned allocator (and any buffer allocated from it).
//
// IMPORTANT - memory accounting: an allocator created this way reports
// `amountMemoryLeft() == MemorySize::max()` ("unlimited"). This is unavoidable
// because `ql::pmr::memory_resource` exposes no way to query a resource's
// remaining capacity - only our own `LimitedMemoryResource` can report a
// budget. Consequently QLever's memory-aware planning (sort-size estimation,
// cache sizing, ...) will treat the pool as infinite and may over-commit if the
// pool is in fact bounded. Use this factory only for effectively-unbounded
// upstreams. For a *bounded* arena, wrap it with `makePmrAllocatorWithLimit`
// (passing the arena as `upstream` and its size as `limit`) so that QLever's
// accounting matches the real capacity.
template <typename T>
PmrAllocator<T> makePmrAllocatorFromResource(
    ql::pmr::memory_resource* resource) {
  return PmrAllocator<T>{resource};
}

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_ALLOCATORPMR_H
