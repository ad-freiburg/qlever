// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_IDTABLE_IDCOLUMN_H
#define QLEVER_SRC_ENGINE_IDTABLE_IDCOLUMN_H

#include <cstdint>
#include <iterator>
#include <memory>
#include <vector>

#include "backports/algorithm.h"
#include "backports/span.h"
#include "global/Id.h"
#include "util/Enums.h"
#include "util/Exception.h"
#include "util/TypeTraits.h"

// This file contains the types that implement the storage-efficient columns
// of `Id`s that are used by the `IdTable` class. A materialized `ValueId`
// consists of a full 64-bit word of payload and a single datatype byte, and
// therefore occupies 16 bytes (including 7 padding bytes) when stored
// directly. To avoid wasting the padding memory, a column of `Id`s is stored
// as two separate arrays: one contiguous array for the 64-bit payloads and
// one contiguous array for the datatype bytes (structure-of-arrays layout,
// 9 bytes per entry).
//
// As a consequence, there is no contiguous array of materialized `ValueId`s
// anymore, so (similar to `std::vector<bool>` and to the row references of
// the `IdTable`) proxy types are required:
//
// - `IdRef`/`ConstIdRef`: a reference to a single `Id` inside a split
//   column. It can be converted to and assigned from a materialized
//   `ValueId` and mirrors the read-only interface of `ValueId`.
// - `IdColumnIterator`: a random-access proxy iterator over a split column.
// - `IdColumnSpan`/`ConstIdColumnSpan`: the replacement for
//   `ql::span<Id>`/`ql::span<const Id>`, with `subspan` etc.
// - `IdColumnVector`: the owning, dynamically growing split column (the
//   replacement for `std::vector<Id>` as the column storage of an
//   `IdTable`).
//
// Note: This file deliberately uses plain C++20 `requires` clauses (and not
// the `CPP_...` backport macros), because the proxy machinery fundamentally
// requires C++20 (customization points `iter_move`/`iter_swap`, constrained
// special member functions).

namespace columnBasedIdTable {

// A proxy reference to a single `Id` that is stored inside a split column
// (see above). It stores a pointer to the payload bits and a pointer to the
// datatype byte.
template <ad_utility::IsConst isConstTag>
class IdRefBase {
 public:
  static constexpr bool isConst = isConstTag == ad_utility::IsConst::True;
  using PayloadPtr = std::conditional_t<isConst, const uint64_t*, uint64_t*>;
  using DatatypePtr = std::conditional_t<isConst, const uint8_t*, uint8_t*>;

 private:
  PayloadPtr payload_ = nullptr;
  DatatypePtr datatype_ = nullptr;

 public:
  IdRefBase() = default;
  constexpr IdRefBase(PayloadPtr payload, DatatypePtr datatype)
      : payload_{payload}, datatype_{datatype} {}

  // Implicit conversion from a mutable to a const reference. Note: This
  // must be a template, s.t. it never collides with the copy constructor.
  template <ad_utility::IsConst C>
  requires(isConst && C == ad_utility::IsConst::False)
  constexpr IdRefBase(const IdRefBase<C>& other)
      : payload_{other.payloadPtr()}, datatype_{other.datatypePtr()} {}

  // Access to the underlying pointers (mostly needed internally and for the
  // conversion between the const and mutable variant).
  constexpr PayloadPtr payloadPtr() const { return payload_; }
  constexpr DatatypePtr datatypePtr() const { return datatype_; }

  // Materialize the `ValueId` that this reference points to. The conversion
  // is implicit on purpose, s.t. the proxy reference can be used like an
  // `Id` in most places.
  constexpr operator ValueId() const {
    return ValueId::fromBits({*datatype_, *payload_});
  }
  // Explicitly materialize the `ValueId` (more readable than a
  // `static_cast` at some call sites).
  constexpr ValueId materialize() const { return static_cast<ValueId>(*this); }

  // Assignment from a materialized `ValueId` writes through to the
  // underlying column. Note: The assignment operator is `const`-qualified
  // because it does not change the proxy itself, but only the underlying
  // storage. This is also required to fulfill `std::indirectly_writable`.
  constexpr const IdRefBase& operator=(const ValueId& id) const
      requires(!isConst) {
    auto bits = id.getBits();
    *payload_ = bits.payload_;
    *datatype_ = bits.datatype_;
    return *this;
  }

  // The copy and move constructors copy the pointers (a copy of a reference
  // refers to the same `Id`).
  IdRefBase(const IdRefBase&) = default;
  IdRefBase(IdRefBase&&) noexcept = default;

  // For mutable proxy references, ALL assignments from another reference
  // must write the referenced value through to the underlying storage (they
  // must never rebind the pointers, which the implicitly generated
  // assignment operators would do).
  constexpr IdRefBase& operator=(const IdRefBase& other) requires(!isConst) {
    *this = static_cast<ValueId>(other);
    return *this;
  }
  constexpr IdRefBase& operator=(IdRefBase&& other) noexcept requires(!isConst)
  {
    *this = static_cast<ValueId>(other);
    return *this;
  }
  constexpr const IdRefBase& operator=(const IdRefBase& other) const
      requires(!isConst) {
    return *this = static_cast<ValueId>(other);
  }
  // Assignment from a const reference to a mutable reference. Note: This
  // must be a template, else it would collide with the copy assignment
  // operator in the `isConst` instantiation.
  template <ad_utility::IsConst C>
  requires(!isConst && C == ad_utility::IsConst::True)
  constexpr const IdRefBase& operator=(const IdRefBase<C>& other) const {
    return *this = static_cast<ValueId>(other);
  }
  // Const proxy references cannot be assigned through, so the default
  // (pointer-copying) assignment is fine for them.
  IdRefBase& operator=(const IdRefBase& other) requires(isConst) = default;
  IdRefBase& operator=(IdRefBase&& other) noexcept requires(isConst) = default;

  // Swap the two referenced `Id`s (not the references themselves). Note:
  // The arguments are taken by value because the proxy references are
  // typically obtained as prvalues (e.g. from dereferencing an iterator).
  friend constexpr void swap(IdRefBase a, IdRefBase b) requires(!isConst) {
    std::swap(*a.payload_, *b.payload_);
    std::swap(*a.datatype_, *b.datatype_);
  }

  // ___________________________________________________________________________
  // Mirror the read-only interface of `ValueId`, s.t. most code that operates
  // on `Id` values can simply use the proxy reference instead. The
  // implementations materialize the `ValueId`, which is cheap (at most two
  // loads).
  constexpr Datatype getDatatype() const {
    return static_cast<Datatype>(*datatype_);
  }
  constexpr ValueId::BitRepresentation getBits() const {
    return {*datatype_, *payload_};
  }
  constexpr uint64_t getPayloadBits() const { return *payload_; }
  constexpr bool isUndefined() const { return materialize().isUndefined(); }
  ValueId::UndefinedType getUndefined() const { return {}; }
  constexpr bool getBool() const { return materialize().getBool(); }
  std::string_view getBoolLiteral() const {
    return materialize().getBoolLiteral();
  }
  double getDouble() const { return materialize().getDouble(); }
  int64_t getInt() const { return materialize().getInt(); }
  constexpr VocabIndex getVocabIndex() const {
    return materialize().getVocabIndex();
  }
  constexpr uint64_t getEncodedVal() const {
    return materialize().getEncodedVal();
  }
  constexpr TextRecordIndex getTextRecordIndex() const {
    return materialize().getTextRecordIndex();
  }
  LocalVocabIndex getLocalVocabIndex() const {
    return materialize().getLocalVocabIndex();
  }
  constexpr WordVocabIndex getWordVocabIndex() const {
    return materialize().getWordVocabIndex();
  }
  constexpr BlankNodeIndex getBlankNodeIndex() const {
    return materialize().getBlankNodeIndex();
  }
  DateYearOrDuration getDate() const { return materialize().getDate(); }
  GeoPoint getGeoPoint() const { return materialize().getGeoPoint(); }
  constexpr bool isTrivial() const { return materialize().isTrivial(); }
  constexpr bool canBeComparedBitwise() const {
    return materialize().canBeComparedBitwise();
  }
  auto compareWithoutLocalVocab(const ValueId& other) const {
    return materialize().compareWithoutLocalVocab(other);
  }
  template <typename Visitor>
  decltype(auto) visit(Visitor&& visitor) const {
    return materialize().visit(AD_FWD(visitor));
  }

  // Comparisons between two (possibly differently const) proxy references
  // and between a proxy reference and a `ValueId`. They compare the
  // materialized `ValueId`s. Note: The reversed and heterogeneous variants
  // are synthesized by the C++20 rules.
  template <ad_utility::IsConst C>
  friend constexpr bool operator==(const IdRefBase& a, const IdRefBase<C>& b) {
    return static_cast<ValueId>(a) == static_cast<ValueId>(b);
  }
  friend constexpr bool operator==(const IdRefBase& a, const ValueId& b) {
    return static_cast<ValueId>(a) == b;
  }
  template <ad_utility::IsConst C>
  friend constexpr auto operator<=>(const IdRefBase& a, const IdRefBase<C>& b) {
    return ql::compareThreeWay(static_cast<ValueId>(a),
                               static_cast<ValueId>(b));
  }
  friend constexpr auto operator<=>(const IdRefBase& a, const ValueId& b) {
    return ql::compareThreeWay(static_cast<ValueId>(a), b);
  }

  // Support for `operator<<` (only used for debugging and testing).
  friend std::ostream& operator<<(std::ostream& ostr, const IdRefBase& ref) {
    return ostr << static_cast<ValueId>(ref);
  }

  // Hashing of a proxy reference hashes the referenced `Id`.
  template <typename H>
  friend H AbslHashValue(H h, const IdRefBase& ref) {
    return AbslHashValue(std::move(h), static_cast<ValueId>(ref));
  }
};

using IdRef = IdRefBase<ad_utility::IsConst::False>;
using ConstIdRef = IdRefBase<ad_utility::IsConst::True>;

// A random-access proxy iterator over a split column of `Id`s. Dereferencing
// yields an `IdRef`/`ConstIdRef` (see above), the `value_type` is the
// materialized `ValueId`.
template <ad_utility::IsConst isConstTag>
class IdColumnIterator {
 public:
  static constexpr bool isConst = isConstTag == ad_utility::IsConst::True;
  using PayloadPtr = std::conditional_t<isConst, const uint64_t*, uint64_t*>;
  using DatatypePtr = std::conditional_t<isConst, const uint8_t*, uint8_t*>;

  using value_type = ValueId;
  using reference = IdRefBase<isConstTag>;
  using pointer = void;
  using difference_type = std::ptrdiff_t;
  using iterator_category = std::random_access_iterator_tag;
  using iterator_concept = std::random_access_iterator_tag;

 private:
  PayloadPtr payload_ = nullptr;
  DatatypePtr datatype_ = nullptr;

 public:
  IdColumnIterator() = default;
  constexpr IdColumnIterator(PayloadPtr payload, DatatypePtr datatype)
      : payload_{payload}, datatype_{datatype} {}

  IdColumnIterator(const IdColumnIterator&) = default;
  IdColumnIterator(IdColumnIterator&&) noexcept = default;
  IdColumnIterator& operator=(const IdColumnIterator&) = default;
  IdColumnIterator& operator=(IdColumnIterator&&) noexcept = default;

  // Implicit conversion from a mutable to a const iterator. Note: This must
  // be a template, s.t. it never collides with the copy constructor (which
  // it would otherwise suppress in the mutable instantiation).
  template <ad_utility::IsConst C>
  requires(isConst && C == ad_utility::IsConst::False)
  constexpr IdColumnIterator(const IdColumnIterator<C>& other)
      : payload_{other.payloadPtr()}, datatype_{other.datatypePtr()} {}

  constexpr PayloadPtr payloadPtr() const { return payload_; }
  constexpr DatatypePtr datatypePtr() const { return datatype_; }

  constexpr reference operator*() const { return {payload_, datatype_}; }
  constexpr reference operator[](difference_type n) const {
    return {payload_ + n, datatype_ + n};
  }

  constexpr IdColumnIterator& operator++() {
    ++payload_;
    ++datatype_;
    return *this;
  }
  constexpr IdColumnIterator operator++(int) {
    auto copy = *this;
    ++*this;
    return copy;
  }
  constexpr IdColumnIterator& operator--() {
    --payload_;
    --datatype_;
    return *this;
  }
  constexpr IdColumnIterator operator--(int) {
    auto copy = *this;
    --*this;
    return copy;
  }
  constexpr IdColumnIterator& operator+=(difference_type n) {
    payload_ += n;
    datatype_ += n;
    return *this;
  }
  constexpr IdColumnIterator& operator-=(difference_type n) {
    payload_ -= n;
    datatype_ -= n;
    return *this;
  }
  friend constexpr IdColumnIterator operator+(IdColumnIterator it,
                                              difference_type n) {
    it += n;
    return it;
  }
  friend constexpr IdColumnIterator operator+(difference_type n,
                                              IdColumnIterator it) {
    return it + n;
  }
  friend constexpr IdColumnIterator operator-(IdColumnIterator it,
                                              difference_type n) {
    it -= n;
    return it;
  }

  // The difference and the comparisons are also defined between const and
  // mutable iterators.
  template <ad_utility::IsConst C>
  friend constexpr difference_type operator-(const IdColumnIterator& a,
                                             const IdColumnIterator<C>& b) {
    return a.payloadPtr() - b.payloadPtr();
  }
  template <ad_utility::IsConst C>
  friend constexpr bool operator==(const IdColumnIterator& a,
                                   const IdColumnIterator<C>& b) {
    return a.payloadPtr() == b.payloadPtr();
  }
  template <ad_utility::IsConst C>
  friend constexpr auto operator<=>(const IdColumnIterator& a,
                                    const IdColumnIterator<C>& b) {
    return a.payloadPtr() <=> b.payloadPtr();
  }

  // Customization points for the C++20 ranges machinery: moving out of a
  // proxy reference materializes the `ValueId`, swapping through iterators
  // swaps the underlying values.
  friend constexpr ValueId iter_move(const IdColumnIterator& it) {
    return static_cast<ValueId>(*it);
  }
  friend constexpr void iter_swap(const IdColumnIterator& a,
                                  const IdColumnIterator& b) requires(!isConst)
  {
    swap(*a, *b);
  }
};

// The replacement for `ql::span<Id>`/`ql::span<const Id>` on a split column:
// a non-owning view consisting of a pointer to the payloads, a pointer to
// the datatype bytes, and a size.
template <ad_utility::IsConst isConstTag>
class IdColumnSpan {
 public:
  static constexpr bool isConst = isConstTag == ad_utility::IsConst::True;
  using PayloadPtr = std::conditional_t<isConst, const uint64_t*, uint64_t*>;
  using DatatypePtr = std::conditional_t<isConst, const uint8_t*, uint8_t*>;

  using value_type = ValueId;
  using reference = IdRefBase<isConstTag>;
  using iterator = IdColumnIterator<isConstTag>;
  using const_iterator = IdColumnIterator<ad_utility::IsConst::True>;
  using size_type = size_t;
  using difference_type = std::ptrdiff_t;

 private:
  PayloadPtr payload_ = nullptr;
  DatatypePtr datatype_ = nullptr;
  size_t size_ = 0;

 public:
  IdColumnSpan() = default;
  constexpr IdColumnSpan(PayloadPtr payload, DatatypePtr datatype, size_t size)
      : payload_{payload}, datatype_{datatype}, size_{size} {}
  constexpr IdColumnSpan(iterator begin, size_t size)
      : payload_{begin.payloadPtr()},
        datatype_{begin.datatypePtr()},
        size_{size} {}
  constexpr IdColumnSpan(iterator begin, iterator end)
      : IdColumnSpan{begin, static_cast<size_t>(end - begin)} {}

  IdColumnSpan(const IdColumnSpan&) = default;
  IdColumnSpan(IdColumnSpan&&) noexcept = default;
  IdColumnSpan& operator=(const IdColumnSpan&) = default;
  IdColumnSpan& operator=(IdColumnSpan&&) noexcept = default;

  // Implicit conversion from a mutable to a const span. Note: This must be a
  // template, s.t. it never collides with the copy constructor (which it
  // would otherwise suppress in the mutable instantiation).
  template <ad_utility::IsConst C>
  requires(isConst && C == ad_utility::IsConst::False)
  constexpr IdColumnSpan(const IdColumnSpan<C>& other)
      : payload_{other.payloadData()},
        datatype_{other.datatypeData()},
        size_{other.size()} {}

  constexpr size_t size() const { return size_; }
  constexpr bool empty() const { return size_ == 0; }

  // Direct access to the underlying split storage. This is important for
  // performance-critical code like compression and block I/O that wants to
  // operate on contiguous memory.
  constexpr PayloadPtr payloadData() const { return payload_; }
  constexpr DatatypePtr datatypeData() const { return datatype_; }
  constexpr ql::span<std::remove_pointer_t<PayloadPtr>> payloadSpan() const {
    return {payload_, size_};
  }
  constexpr ql::span<std::remove_pointer_t<DatatypePtr>> datatypeSpan() const {
    return {datatype_, size_};
  }

  constexpr iterator begin() const { return {payload_, datatype_}; }
  constexpr iterator end() const {
    return {payload_ + size_, datatype_ + size_};
  }
  constexpr const_iterator cbegin() const { return begin(); }
  constexpr const_iterator cend() const { return end(); }

  constexpr reference operator[](size_t i) const {
    return {payload_ + i, datatype_ + i};
  }
  constexpr reference front() const { return (*this)[0]; }
  constexpr reference back() const { return (*this)[size_ - 1]; }

  // The usual `subspan`, `first`, and `last` members, analogous to
  // `ql::span`.
  // Note: The following functions are not `constexpr`, because the check
  // macros call non-`constexpr` functions (GCC rejects this with
  // `-Winvalid-constexpr`).
  IdColumnSpan subspan(size_t offset, size_t count = size_t(-1)) const {
    AD_EXPENSIVE_CHECK(offset <= size_);
    auto actualCount = count == size_t(-1) ? size_ - offset : count;
    AD_EXPENSIVE_CHECK(offset + actualCount <= size_);
    return {payload_ + offset, datatype_ + offset, actualCount};
  }
  IdColumnSpan first(size_t count) const { return subspan(0, count); }
  IdColumnSpan last(size_t count) const {
    return subspan(size_ - count, count);
  }
};

using MutableIdColumnSpan = IdColumnSpan<ad_utility::IsConst::False>;
using ConstIdColumnSpan = IdColumnSpan<ad_utility::IsConst::True>;

}  // namespace columnBasedIdTable

// Opt into the C++20 ranges machinery: an `IdColumnSpan` is a cheaply
// copyable view, and its iterators may outlive the span object itself
// (borrowed range), exactly like `std::span`.
template <ad_utility::IsConst C>
inline constexpr bool
    std::ranges::enable_view<columnBasedIdTable::IdColumnSpan<C>> = true;
template <ad_utility::IsConst C>
inline constexpr bool
    std::ranges::enable_borrowed_range<columnBasedIdTable::IdColumnSpan<C>> =
        true;

// The same opt-ins for `range-v3`.
template <ad_utility::IsConst C>
inline constexpr bool ranges::enable_view<columnBasedIdTable::IdColumnSpan<C>> =
    true;
template <ad_utility::IsConst C>
inline constexpr bool
    ranges::enable_borrowed_range<columnBasedIdTable::IdColumnSpan<C>> = true;

namespace columnBasedIdTable {

// The owning, dynamically growing split column of `Id`s: the drop-in
// replacement for `std::vector<Id>` as the column storage of an `IdTable`.
// It is templated on the two underlying containers for the payloads and the
// datatype bytes, s.t. it can also be instantiated with other vector-like
// containers (e.g. `BufferedVector` for the external `IdTable`s).
template <typename PayloadStorage, typename TypeStorage>
class IdColumnVectorImpl {
 public:
  using value_type = ValueId;
  using reference = IdRef;
  using const_reference = ConstIdRef;
  using iterator = IdColumnIterator<ad_utility::IsConst::False>;
  using const_iterator = IdColumnIterator<ad_utility::IsConst::True>;
  using size_type = size_t;
  using difference_type = std::ptrdiff_t;

 private:
  // Helper to detect whether the underlying storages have allocators.
  template <typename S>
  static constexpr bool hasAllocator =
      requires(const S& s) { s.get_allocator(); };

 public:
  // The allocator type that is exposed to the outside is the `Id`-typed
  // allocator; it is rebound internally to the allocators of the two
  // underlying storages.
  template <typename A, typename U>
  using Rebound = typename std::allocator_traits<A>::template rebind_alloc<U>;

 private:
  PayloadStorage payloads_;
  TypeStorage types_;

 public:
  IdColumnVectorImpl() = default;

  // Helper concept: `Allocator` is a type for which the rebinding to the
  // allocators of the underlying storages may be attempted. In particular,
  // this must not be `IdColumnVectorImpl` itself (which is probed e.g. by
  // `std::is_constructible` checks of surrounding containers and for which
  // computing `rebind_alloc` triggers a hard error inside
  // `std::allocator_traits`).
  template <typename Allocator>
  static constexpr bool isPotentialAllocator =
      !std::is_same_v<std::remove_cvref_t<Allocator>, IdColumnVectorImpl>;

  // Constructors from an `Id`-typed allocator (rebound internally). These
  // only participate if the underlying storages are allocator-aware.
  template <typename Allocator>
  requires(isPotentialAllocator<Allocator> &&
           std::is_constructible_v<PayloadStorage,
                                   Rebound<Allocator, uint64_t>> &&
           std::is_constructible_v<TypeStorage, Rebound<Allocator, uint8_t>>)
  explicit IdColumnVectorImpl(const Allocator& allocator)
      : payloads_{Rebound<Allocator, uint64_t>{allocator}},
        types_{Rebound<Allocator, uint8_t>{allocator}} {}

  template <typename Allocator>
  requires(isPotentialAllocator<Allocator> &&
           std::is_constructible_v<PayloadStorage, size_t,
                                   Rebound<Allocator, uint64_t>> &&
           std::is_constructible_v<TypeStorage, size_t,
                                   Rebound<Allocator, uint8_t>>)
  IdColumnVectorImpl(size_t size, const Allocator& allocator)
      : payloads_(size, Rebound<Allocator, uint64_t>{allocator}),
        types_(size, Rebound<Allocator, uint8_t>{allocator}) {}

  // Construct directly from the two underlying storages. They must have the
  // same size.
  IdColumnVectorImpl(PayloadStorage payloads, TypeStorage types)
      : payloads_{std::move(payloads)}, types_{std::move(types)} {
    AD_CONTRACT_CHECK(payloads_.size() == types_.size());
  }

  // Construct from an iterator range of `Id`s (or `Id` proxy references) and
  // an `Id`-typed allocator. This is used by `IdTable::clone`.
  template <typename It, typename Allocator>
  requires(isPotentialAllocator<Allocator> &&
           std::is_constructible_v<IdColumnVectorImpl, const Allocator&>)
  IdColumnVectorImpl(It first, It last, const Allocator& allocator)
      : IdColumnVectorImpl{allocator} {
    const auto numElements = static_cast<size_t>(last - first);
    if constexpr (ad_utility::SimilarToAny<
                      It, IdColumnIterator<ad_utility::IsConst::False>,
                      IdColumnIterator<ad_utility::IsConst::True>>) {
      // Fast path for iterators into another split column: directly copy
      // the contiguous payload and datatype arrays (`memcpy` speed). This
      // is in particular the path taken by `IdTable::clone()`.
      payloads_.insert(payloads_.end(), first.payloadPtr(),
                       first.payloadPtr() + numElements);
      types_.insert(types_.end(), first.datatypePtr(),
                    first.datatypePtr() + numElements);
    } else {
      reserve(numElements);
      for (; first != last; ++first) {
        push_back(*first);
      }
    }
  }

  IdColumnVectorImpl(const IdColumnVectorImpl&) = default;
  IdColumnVectorImpl(IdColumnVectorImpl&&) noexcept = default;
  IdColumnVectorImpl& operator=(const IdColumnVectorImpl&) = default;
  IdColumnVectorImpl& operator=(IdColumnVectorImpl&&) noexcept = default;

  // Return the `Id`-typed allocator (rebound from the allocator of the
  // payload storage).
  auto get_allocator() const requires hasAllocator<PayloadStorage> {
    using A = std::decay_t<decltype(payloads_.get_allocator())>;
    return Rebound<A, ValueId>{payloads_.get_allocator()};
  }

  constexpr size_t size() const { return payloads_.size(); }
  constexpr bool empty() const { return payloads_.empty(); }
  size_t capacity() const { return payloads_.capacity(); }

  void resize(size_t newSize) {
    payloads_.resize(newSize);
    types_.resize(newSize);
  }
  void reserve(size_t newCapacity) {
    payloads_.reserve(newCapacity);
    types_.reserve(newCapacity);
  }
  void clear() {
    payloads_.clear();
    types_.clear();
  }
  void shrink_to_fit() {
    payloads_.shrink_to_fit();
    types_.shrink_to_fit();
  }

  void push_back(const ValueId& id) {
    auto bits = id.getBits();
    payloads_.push_back(bits.payload_);
    types_.push_back(bits.datatype_);
  }
  // Append a new (uninitialized or zeroed, depending on the underlying
  // storages) `Id` at the end.
  void emplace_back() {
    payloads_.emplace_back();
    types_.emplace_back();
  }

  reference operator[](size_t i) {
    return {payloads_.data() + i, types_.data() + i};
  }
  const_reference operator[](size_t i) const {
    return {payloads_.data() + i, types_.data() + i};
  }
  reference at(size_t i) {
    AD_CONTRACT_CHECK(i < size());
    return (*this)[i];
  }
  const_reference at(size_t i) const {
    AD_CONTRACT_CHECK(i < size());
    return (*this)[i];
  }
  reference front() { return (*this)[0]; }
  const_reference front() const { return (*this)[0]; }
  reference back() { return (*this)[size() - 1]; }
  const_reference back() const { return (*this)[size() - 1]; }

  iterator begin() { return {payloads_.data(), types_.data()}; }
  iterator end() { return {payloads_.data() + size(), types_.data() + size()}; }
  const_iterator begin() const { return {payloads_.data(), types_.data()}; }
  const_iterator end() const {
    return {payloads_.data() + size(), types_.data() + size()};
  }
  const_iterator cbegin() const { return begin(); }
  const_iterator cend() const { return end(); }

  // Erase the elements in `[first, last)`, analogous to `std::vector::erase`.
  void erase(const_iterator first, const_iterator last) {
    auto beginIdx = static_cast<size_t>(first - cbegin());
    auto endIdx = static_cast<size_t>(last - cbegin());
    payloads_.erase(payloads_.begin() + beginIdx, payloads_.begin() + endIdx);
    types_.erase(types_.begin() + beginIdx, types_.begin() + endIdx);
  }

  // Insert the range `[first, last)` (iterators into another split column)
  // before `pos`. This directly copies the underlying payload and datatype
  // arrays, which is much faster than an elementwise copy.
  template <ad_utility::IsConst C>
  void insert(const_iterator pos, IdColumnIterator<C> first,
              IdColumnIterator<C> last) {
    auto posIdx = static_cast<size_t>(pos - cbegin());
    payloads_.insert(payloads_.begin() + posIdx, first.payloadPtr(),
                     last.payloadPtr());
    types_.insert(types_.begin() + posIdx, first.datatypePtr(),
                  last.datatypePtr());
  }

  // Direct access to the underlying split storage.
  uint64_t* payloadData() { return payloads_.data(); }
  const uint64_t* payloadData() const { return payloads_.data(); }
  uint8_t* datatypeData() { return types_.data(); }
  const uint8_t* datatypeData() const { return types_.data(); }
  PayloadStorage& payloads() { return payloads_; }
  const PayloadStorage& payloads() const { return payloads_; }
  TypeStorage& types() { return types_; }
  const TypeStorage& types() const { return types_; }

  // Conversion to the non-owning span types.
  operator MutableIdColumnSpan() {
    return {payloads_.data(), types_.data(), size()};
  }
  operator ConstIdColumnSpan() const {
    return {payloads_.data(), types_.data(), size()};
  }

  bool operator==(const IdColumnVectorImpl& other) const {
    return payloads_ == other.payloads_ && types_ == other.types_;
  }
};

// The default owning split column, templated on an `Id`-typed allocator.
// Note: If default initialization (no zeroing on `resize`) is desired, then
// an allocator that is already wrapped in
// `ad_utility::default_init_allocator` must be passed (the rebinding
// preserves the wrapper).
template <typename Allocator = std::allocator<Id>>
using IdColumnVector = IdColumnVectorImpl<
    std::vector<uint64_t, typename std::allocator_traits<
                              Allocator>::template rebind_alloc<uint64_t>>,
    std::vector<uint8_t, typename std::allocator_traits<
                             Allocator>::template rebind_alloc<uint8_t>>>;

}  // namespace columnBasedIdTable

#endif  // QLEVER_SRC_ENGINE_IDTABLE_IDCOLUMN_H
