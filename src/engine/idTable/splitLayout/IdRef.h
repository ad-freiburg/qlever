// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Pascal Keßler <kesslerp@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_IDTABLE_SPLITLAYOUT_IDREF_H
#define QLEVER_SRC_ENGINE_IDTABLE_SPLITLAYOUT_IDREF_H

#include <ostream>
#include <utility>

#include "backports/concepts.h"
#include "global/Id.h"
#include "util/IsProxy.h"

namespace columnBasedIdTable::splitLayout {
template <bool IsConst>
class BasicIdRef;

// A proxy reference to a single `Id` stored in the experimental split layout
// (a payload word + datatype byte in two separate arrays, instead of one
// contiguous legacy `Id`). This class mirrors the legacy `Id`'s full read API,
// so generic code written for `Id&`/`const Id&` (e.g. `column[i].method()`)
// also compiles for this proxy. It can only be written to if it is an rvalue,
// like `RowReferenceWithRestrictedAccess` for rows in `IdTableRow.h`. This is
// what `auto x = column[i];` should deduce: `x` looks like a value, but is a
// reference, so `x = id;` must not silently change the column. To write via a
// variable, request the full `BasicIdRef` (`IdRef`) explicitly. A thin,
// cheap-to-copy pair of pointers, not polymorphic, like `Id` itself.
//
// TODO<pas-kes>: Most of the read API below first converts to an `Id` via
// `toId()`. Assess in the future whether this is necessary, or whether the
// generated code should be avoided.
class BasicIdRefImpl {
  template <bool IsConst>
  friend class BasicIdRef;
  template <bool IsConst>
  friend class BasicIdColumnIterator;
  template <bool IsConst>
  friend class BasicIdColumnView;

  template <bool IsConst>
  class BasicIdRefWithRestrictedAccess {
   public:
    using PayloadPointer =
        std::conditional_t<IsConst, const Id::PayloadType*, Id::PayloadType*>;
    using DatatypePointer =
        std::conditional_t<IsConst, const Id::DatatypeType*, Id::DatatypeType*>;

   protected:
    PayloadPointer payload_;
    DatatypePointer datatype_;

   public:
    // The full proxy in which this restricted reference is stored when it is
    // kept by value, for example by `ad_utility::tie` (see
    // `qlever::FullProxy`), so that it can be written to like a reference.
    using FullProxyType = BasicIdRef<IsConst>;

    BasicIdRefWithRestrictedAccess(PayloadPointer payload,
                                   DatatypePointer datatype)
        : payload_{payload}, datatype_{datatype} {}

    // TODO<pas-kes> With `SplitLayoutId`: implicit constructor from
    // `SplitLayoutId&` (points to its members, rvalues deleted).

    // Explicitly defaulted, because the copy-assignment operator below is
    // user-provided (for good reason, see there), which would otherwise make
    // the implicit generation of these two deprecated-but-not-removed.
    BasicIdRefWithRestrictedAccess(const BasicIdRefWithRestrictedAccess&) =
        default;

    BasicIdRefWithRestrictedAccess(BasicIdRefWithRestrictedAccess&&) noexcept =
        default;

    // Implicit conversion to the legacy `Id`, e.g. to store the referenced
    // value in a variable, or to pass it to a function that expects a real
    // `Id`.
    QL_EXPLICIT(false) operator Id() const {
      return Id::fromBits(*datatype_, *payload_);
    }

    // Assign a new `Id` to the referenced slot (not for `ConstIdRef`), but only
    // for rvalues: `column[i] = id;` writes, while `auto ref = column[i]; ref =
    // id;` does not compile.
    CPP_template(typename = void)(requires(!IsConst))
    BasicIdRefWithRestrictedAccess& operator=(const Id id) && {
      *payload_ = id.getPayloadBits();
      *datatype_ = id.getDatatypeBits();
      return *this;
    }

    // Assigning another reference copies the referenced value to the referenced
    // slot (it does not rebind the pointers), again only for rvalues. This has
    // to be a copy-assignment operator (and not a template), otherwise the
    // implicit one, which would rebind the pointers, is used.
    BasicIdRefWithRestrictedAccess& operator=(
        const BasicIdRefWithRestrictedAccess& other) && {
      static_assert(!IsConst, "`ConstIdRef` is not assignable.");
      *payload_ = *other.payload_;
      *datatype_ = *other.datatype_;
      return *this;
    }

    // This overload only needs to be declared to fulfill the concept
    // `std::indirectly_writable` (and thus to use `ranges::sort`), where a
    // reference is assigned to via a `const` rvalue. It is never called. For
    // details see the note at the end of
    // `https://en.cppreference.com/w/cpp/iterator/indirectly_writable` and the
    // same overload for rows in `IdTableRow.h`.
    CPP_template(typename = void)(requires(!IsConst))
    const BasicIdRefWithRestrictedAccess& operator=(const Id id) const&&;

    // Swap the referenced values (not the pointers), but only for rvalues, like
    // for rows in `IdTableRow.h`. This is what `std::iter_swap` and thus
    // `std::reverse` or `ranges::sort` call with the result of `*it`.
    CPP_template(typename = void)(requires(!IsConst))
    friend void swap(BasicIdRefWithRestrictedAccess&& lhs,
                     BasicIdRefWithRestrictedAccess&& rhs) noexcept {
      std::swap(*lhs.payload_, *rhs.payload_);
      std::swap(*lhs.datatype_, *rhs.datatype_);
    }

    // The following functions all just forward to the corresponding function
    // of the legacy `Id`, see `global/FoldedValueId.h` for their documentation.
    [[nodiscard]] auto compareThreeWay(const Id& other) const {
      return toId().compareThreeWay(other);
    }

    [[nodiscard]] auto compareWithoutLocalVocab(const Id& other) const {
      return toId().compareWithoutLocalVocab(other);
    }

    [[nodiscard]] Id::T getBits() const noexcept { return toId().getBits(); }

    [[nodiscard]] Id::PayloadType getPayloadBits() const noexcept {
      return *payload_;
    }

    [[nodiscard]] Datatype getDatatype() const noexcept {
      return static_cast<Datatype>(*datatype_);
    }

    [[nodiscard]] Id::UndefinedType getUndefined() const noexcept { return {}; }

    [[nodiscard]] bool isUndefined() const noexcept {
      return toId().isUndefined();
    }

    [[nodiscard]] double getDouble() const noexcept {
      return toId().getDouble();
    }

    [[nodiscard]] int64_t getInt() const noexcept { return toId().getInt(); }
    [[nodiscard]] bool getBool() const noexcept { return toId().getBool(); }

    [[nodiscard]] std::string_view getBoolLiteral() const noexcept {
      return toId().getBoolLiteral();
    }

    [[nodiscard]] VocabIndex getVocabIndex() const noexcept {
      return toId().getVocabIndex();
    }

    [[nodiscard]] Id::PayloadType getEncodedVal() const noexcept {
      return toId().getEncodedVal();
    }

    [[nodiscard]] TextRecordIndex getTextRecordIndex() const noexcept {
      return toId().getTextRecordIndex();
    }

    [[nodiscard]] LocalVocabIndex getLocalVocabIndex() const noexcept {
      return toId().getLocalVocabIndex();
    }

    [[nodiscard]] WordVocabIndex getWordVocabIndex() const noexcept {
      return toId().getWordVocabIndex();
    }

    [[nodiscard]] BlankNodeIndex getBlankNodeIndex() const noexcept {
      return toId().getBlankNodeIndex();
    }

    [[nodiscard]] SecondaryVocabIndex getSecondaryVocabIndex() const noexcept {
      return toId().getSecondaryVocabIndex();
    }

    [[nodiscard]] DateYearOrDuration getDate() const noexcept {
      return toId().getDate();
    }

    [[nodiscard]] GeoPoint getGeoPoint() const { return toId().getGeoPoint(); }
    [[nodiscard]] bool isTrivial() const { return toId().isTrivial(); }

    [[nodiscard]] bool canBeComparedBitwise() const {
      return toId().canBeComparedBitwise();
    }

    template <typename Visitor>
    decltype(auto) visit(Visitor&& visitor) const {
      return toId().visit(AD_FWD(visitor));
    }

    // Comparisons. Only one side needs to be a `BasicIdRefWithRestrictedAccess`
    // for these to be found via ADL; the other side is implicitly converted to
    // `Id`.
    friend bool operator==(const BasicIdRefWithRestrictedAccess a, const Id b) {
      return a.toId() == b;
    }

    template <bool OtherConst>
    friend bool operator==(const BasicIdRefWithRestrictedAccess a,
                           const BasicIdRefWithRestrictedAccess<OtherConst> b) {
      return a.toId() == b.operator Id();
    }
#ifdef QLEVER_CPP_17
    friend bool operator==(const Id a, const BasicIdRefWithRestrictedAccess b) {
      return a == b.toId();
    }
    friend bool operator!=(const BasicIdRefWithRestrictedAccess a, const Id b) {
      return !(a == b);
    }
    friend bool operator!=(const Id a, const BasicIdRefWithRestrictedAccess b) {
      return !(a == b);
    }
    template <bool OtherConst>
    friend bool operator!=(const BasicIdRefWithRestrictedAccess a,
                           const BasicIdRefWithRestrictedAccess<OtherConst> b) {
      return !(a == b);
    }

    friend bool operator<(const BasicIdRefWithRestrictedAccess a, const Id b) {
      return a.toId() < b;
    }
    friend bool operator<(const Id a, const BasicIdRefWithRestrictedAccess b) {
      return a < b.toId();
    }
    template <bool OtherConst>
    friend bool operator<(const BasicIdRefWithRestrictedAccess a,
                          const BasicIdRefWithRestrictedAccess<OtherConst> b) {
      return a.toId() < b.operator Id();
    }
    friend bool operator<=(const BasicIdRefWithRestrictedAccess a, const Id b) {
      return a.toId() <= b;
    }
    friend bool operator<=(const Id a, const BasicIdRefWithRestrictedAccess b) {
      return a <= b.toId();
    }
    template <bool OtherConst>
    friend bool operator<=(const BasicIdRefWithRestrictedAccess a,
                           const BasicIdRefWithRestrictedAccess<OtherConst> b) {
      return a.toId() <= b.operator Id();
    }
    friend bool operator>(const BasicIdRefWithRestrictedAccess a, const Id b) {
      return a.toId() > b;
    }
    friend bool operator>(const Id a, const BasicIdRefWithRestrictedAccess b) {
      return a > b.toId();
    }
    template <bool OtherConst>
    friend bool operator>(const BasicIdRefWithRestrictedAccess a,
                          const BasicIdRefWithRestrictedAccess<OtherConst> b) {
      return a.toId() > b.operator Id();
    }
    friend bool operator>=(const BasicIdRefWithRestrictedAccess a, const Id b) {
      return a.toId() >= b;
    }
    friend bool operator>=(const Id a, const BasicIdRefWithRestrictedAccess b) {
      return a >= b.toId();
    }
    template <bool OtherConst>
    friend bool operator>=(const BasicIdRefWithRestrictedAccess a,
                           const BasicIdRefWithRestrictedAccess<OtherConst> b) {
      return a.toId() >= b.operator Id();
    }
#else
    friend auto operator<=>(const BasicIdRefWithRestrictedAccess a,
                            const Id b) {
      return a.compareThreeWay(b);
    }

    template <bool OtherConst>
    friend auto operator<=>(
        const BasicIdRefWithRestrictedAccess a,
        const BasicIdRefWithRestrictedAccess<OtherConst> b) {
      return a.compareThreeWay(b.operator Id());
    }
#endif

    friend std::ostream& operator<<(std::ostream& ostr,
                                    const BasicIdRefWithRestrictedAccess ref) {
      return ostr << ref.toId();
    }

   private:
    [[nodiscard]] Id toId() const { return *this; }
  };
};

// The full proxy reference, which can also be written to via lvalues. Use it
// explicitly where a reference to an `Id` in a column is needed (`IdRef` to
// change the referenced `Id`, `ConstIdRef` to read it). It can be constructed
// from the restricted reference, so `IdRef ref = column[i]; ref = id;` writes
// through to the column. It inherits the read API.
template <bool IsConst>
class BasicIdRef
    : public BasicIdRefImpl::BasicIdRefWithRestrictedAccess<IsConst> {
 public:
  using Base = BasicIdRefImpl::BasicIdRefWithRestrictedAccess<IsConst>;
  using Base::Base;

  /*implicit*/
  BasicIdRef(Base base) : Base{std::move(base)} {}

  // Explicitly defaulted, because the copy-assignment operator below is
  // user-provided (for good reason, see there), which would otherwise make
  // the implicit generation of these two deprecated-but-not-removed.
  BasicIdRef(const BasicIdRef&) = default;

  BasicIdRef(BasicIdRef&&) noexcept = default;

  // Assign a new `Id` to the referenced slot (`IdRef` only). Like for any proxy
  // reference, this writes through to the referenced slot and does not rebind
  // the proxy.
  CPP_template(typename = void)(requires(!IsConst))
  BasicIdRef& operator=(const Id id) {
    *this->payload_ = id.getPayloadBits();
    *this->datatype_ = id.getDatatypeBits();
    return *this;
  }

  BasicIdRef& operator=(const BasicIdRef& other) {
    static_assert(!IsConst, "`ConstIdRef` is not assignable.");
    *this->payload_ = *other.payload_;
    *this->datatype_ = *other.datatype_;
    return *this;
  }

  // Swaps the referenced values, not the pointers. Takes `const&` so that it
  // also binds to the prvalues `*it` that `ranges::iter_swap` passes.
  friend void swap(const BasicIdRef& lhs, const BasicIdRef& rhs) noexcept {
    static_assert(!IsConst, "`ConstIdRef` is not swappable.");
    std::swap(*lhs.payload_, *rhs.payload_);
    std::swap(*lhs.datatype_, *rhs.datatype_);
  }
};

using IdRef = BasicIdRef<false>;
using ConstIdRef = BasicIdRef<true>;
}  // namespace columnBasedIdTable::splitLayout

// The `IdRef` and the `ConstIdRef` are cheap to copy and refer to an element of
// a column, so they may be stored by value, also in place of the restricted
// references returned by `column[i]` (see `FullProxyType` there).
template <bool IsConst>
inline constexpr bool
    qlever::isProxy<columnBasedIdTable::splitLayout::BasicIdRef<IsConst> > =
        true;

#endif  // QLEVER_SRC_ENGINE_IDTABLE_SPLITLAYOUT_IDREF_H
