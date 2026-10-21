// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Pascal Keßler <kesslerp@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_IDTABLE_SPLITLAYOUT_IDCOLUMNVECTOR_H
#define QLEVER_SRC_ENGINE_IDTABLE_SPLITLAYOUT_IDCOLUMNVECTOR_H

#include <memory>
#include <vector>

#include "engine/idTable/splitLayout/IdColumn.h"
#include "engine/idTable/splitLayout/IdRef.h"
#include "global/Id.h"
#include "util/Exception.h"

namespace columnBasedIdTable::splitLayout {

// An owning, growable column of `Id`s in the experimental split layout: two
// separate, contiguous arrays (payload words, datatype bytes) instead of one
// array of packed legacy `Id` objects. The split-layout counterpart of the
// `std::vector<Id>` in `IdTable`.
//
// `Allocator` is `IdTable`'s own `Id` allocator; the two underlying arrays
// use it rebound to `uint64_t`/`uint8_t`, so both count towards the same
// memory limit.
template <typename Allocator>
class IdColumnVector {
 public:
  using PayloadAllocator =
      std::allocator_traits<Allocator>::template rebind_alloc<uint64_t>;
  using DatatypeAllocator =
      std::allocator_traits<Allocator>::template rebind_alloc<uint8_t>;

  using value_type = Id;
  using reference = IdRef;
  using const_reference = ConstIdRef;
  using iterator = IdColumnIterator;
  using const_iterator = ConstIdColumnIterator;

 private:
  // Kept separately (rather than reconstructed from the two vectors' rebound
  // allocators) to preserve the passed-in allocator's exact state (e.g. a
  // shared memory limit).
  Allocator allocator_;
  std::vector<uint64_t, PayloadAllocator> payloads_;
  std::vector<uint8_t, DatatypeAllocator> datatypes_;

 public:
  explicit IdColumnVector(Allocator allocator = Allocator{})
      : allocator_{allocator},
        payloads_{PayloadAllocator{allocator}},
        datatypes_{DatatypeAllocator{allocator}} {}

  IdColumnVector(size_t size, Allocator allocator)
      : allocator_{allocator},
        payloads_(size, PayloadAllocator{allocator}),
        datatypes_(size, DatatypeAllocator{allocator}) {}

  // Construct from a range of elements that are convertible to `Id` (e.g.
  // `Id` itself, or `IdRef`/`ConstIdRef` as yielded by another column's
  // iterators). Needed e.g. for `IdTable::clone()` on this storage.
  //
  // TODO<pas-kes>: This element-wise interface is inefficient as soon as the
  // iterators point into another split column. Check its performance and
  // replace it where needed.
  template <typename InputIt>
  IdColumnVector(InputIt first, InputIt last, Allocator allocator)
      : IdColumnVector(allocator) {
    for (; first != last; ++first) {
      push_back(*first);
    }
  }

  [[nodiscard]] Allocator get_allocator() const { return allocator_; }

  [[nodiscard]] size_t size() const noexcept { return payloads_.size(); }
  [[nodiscard]] bool empty() const noexcept { return payloads_.empty(); }

  void reserve(size_t n) {
    payloads_.reserve(n);
    datatypes_.reserve(n);
  }
  void resize(size_t n) {
    payloads_.resize(n);
    datatypes_.resize(n);
  }
  void clear() {
    payloads_.clear();
    datatypes_.clear();
  }
  void shrink_to_fit() {
    payloads_.shrink_to_fit();
    datatypes_.shrink_to_fit();
  }

  void push_back(const Id id) {
    auto [datatype, payload] = getBitsCompat(id);
    payloads_.push_back(payload);
    datatypes_.push_back(datatype);
  }

  void emplace_back() {
    payloads_.emplace_back();
    datatypes_.emplace_back();
  }

  [[nodiscard]] reference operator[](size_t i) {
    return {&payloads_[i], &datatypes_[i]};
  }
  [[nodiscard]] const_reference operator[](size_t i) const {
    return {&payloads_[i], &datatypes_[i]};
  }
  [[nodiscard]] reference at(size_t i) {
    AD_CONTRACT_CHECK(i < size());
    return (*this)[i];
  }
  [[nodiscard]] const_reference at(size_t i) const {
    AD_CONTRACT_CHECK(i < size());
    return (*this)[i];
  }

  [[nodiscard]] iterator begin() { return asView().begin(); }
  [[nodiscard]] iterator end() { return asView().end(); }
  [[nodiscard]] const_iterator begin() const { return asConstView().begin(); }
  [[nodiscard]] const_iterator end() const { return asConstView().end(); }

  // Both overloads needed (unlike `std::vector`, which gets away with a
  // single `const_iterator` one): `IdColumnIterator`/`ConstIdColumnIterator`
  // are unrelated types with no conversion between them.
  void erase(const const_iterator first, const const_iterator last) {
    eraseImpl(first - asConstView().begin(), last - asConstView().begin());
  }
  void erase(const iterator first, const iterator last) {
    eraseImpl(first - asView().begin(), last - asView().begin());
  }
  void erase(const const_iterator pos) { erase(pos, pos + 1); }
  void erase(const iterator pos) { erase(pos, pos + 1); }

  // Insert the elements from `[first, last)` before `pos`. Two overloads for
  // `pos`, for the same reason as for `erase` above.
  template <typename InputIt>
  void insert(const const_iterator pos, InputIt first, InputIt last) {
    insertImpl(pos - asConstView().begin(), first, last);
  }
  template <typename InputIt>
  void insert(const iterator pos, InputIt first, InputIt last) {
    insertImpl(pos - asView().begin(), first, last);
  }

 private:
  void eraseImpl(ptrdiff_t beginOffset, ptrdiff_t endOffset) {
    payloads_.erase(payloads_.begin() + beginOffset,
                    payloads_.begin() + endOffset);
    datatypes_.erase(datatypes_.begin() + beginOffset,
                     datatypes_.begin() + endOffset);
  }
  template <typename InputIt>
  void insertImpl(ptrdiff_t offset, InputIt first, InputIt last) {
    std::vector<uint64_t> newPayloads;
    std::vector<uint8_t> newDatatypes;
    for (; first != last; ++first) {
      auto [datatype, payload] = getBitsCompat(static_cast<Id>(*first));
      newPayloads.push_back(payload);
      newDatatypes.push_back(datatype);
    }
    payloads_.insert(payloads_.begin() + offset, newPayloads.begin(),
                     newPayloads.end());
    try {
      datatypes_.insert(datatypes_.begin() + offset, newDatatypes.begin(),
                        newDatatypes.end());
    } catch (...) {
      // Roll back the insert into `payloads_`, so that the two arrays stay
      // consistent.
      payloads_.erase(payloads_.begin() + offset,
                      payloads_.begin() + offset + newPayloads.size());
      throw;
    }
  }

 public:
  [[nodiscard]] IdColumnRef asView() {
    return {payloads_.data(), datatypes_.data(), payloads_.size()};
  }
  [[nodiscard]] ConstIdColumnRef asConstView() const {
    return {payloads_.data(), datatypes_.data(), payloads_.size()};
  }
  // Implicit conversions to the views, like `std::vector<T>` -> `ql::span<T>`.
  // Generic `IdTable` code relies on this to treat the legacy and the split
  // layout alike.
  /*implicit*/ operator IdColumnRef() { return asView(); }  // NOSONAR
  /*implicit*/ operator ConstIdColumnRef() const {
    return asConstView();
  }  // NOSONAR
};

}  // namespace columnBasedIdTable::splitLayout

#endif  // QLEVER_SRC_ENGINE_IDTABLE_SPLITLAYOUT_IDCOLUMNVECTOR_H
