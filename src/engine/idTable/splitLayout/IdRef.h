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
#include "engine/idTable/splitLayout/SplitLayoutIdBitRepresentation.h"
#include "global/Id.h"

namespace columnBasedIdTable::splitLayout {

// TEMPORARY bridge between the legacy packed `Id` (`ValueId`) and the split
// layout: converts the single-word `Id::getBits()`/`Id::fromBits(T)` from/to
// `SplitLayoutIdBitRepresentation`'s (datatype, payload) shape. Only needed
// as long as the split layout's element type is the legacy `Id`.
inline SplitLayoutIdBitRepresentation getBitsCompat(const Id id) {
  const auto bits = id.getBits();
  return {static_cast<uint8_t>(bits >> Id::numDataBits),
          bits & ((Id::T{1} << Id::numDataBits) - 1)};
}
inline Id idFromBitsCompat(const SplitLayoutIdBitRepresentation bits) {
  return Id::fromBits((static_cast<Id::T>(bits.datatype_) << Id::numDataBits) |
                      bits.payload_);
}

// A proxy reference to a single `Id` stored in the experimental split layout
// (a payload word + datatype byte in two separate arrays, instead of one
// contiguous legacy `Id`). `BasicIdRef<IsConst>` mirrors the legacy `Id`'s
// full read API, so generic code written for `Id&`/`const Id&` (e.g.
// `column[i].method()`) also compiles for this proxy. `IdRef` additionally
// supports assigning a new `Id` to the referenced slot. A thin,
// cheap-to-copy pair of pointers, not polymorphic, like `Id` itself.
//
// TODO<pas-kes>: Most of the read API below first converts to an `Id` via
// `toId()`. Assess in the future whether this is necessary, or whether the
// generated code should be avoided.
template <bool IsConst>
class BasicIdRef {
 public:
  using PayloadPointer =
      std::conditional_t<IsConst, const uint64_t*, uint64_t*>;
  using DatatypePointer = std::conditional_t<IsConst, const uint8_t*, uint8_t*>;

 private:
  PayloadPointer payload_;
  DatatypePointer datatype_;

 public:
  BasicIdRef(PayloadPointer payload, DatatypePointer datatype)
      : payload_{payload}, datatype_{datatype} {}

  // Explicitly defaulted, because the copy-assignment operator below is
  // user-provided (for good reason, see there), which would otherwise make
  // the implicit generation of these two deprecated-but-not-removed.
  BasicIdRef(const BasicIdRef&) = default;
  BasicIdRef(BasicIdRef&&) noexcept = default;

  // Implicit conversion to the legacy `Id`, e.g. to store the referenced value
  // in a variable, or to pass it to a function that expects a real `Id`.
  /*implicit*/ operator Id() const {
    return idFromBitsCompat({*datatype_, *payload_});
  }

  // Assign a new `Id` to the referenced slot (`IdRef` only). `const`, like
  // any proxy reference (e.g. `std::vector<bool>::reference`): it writes
  // through to the referenced slot, not to the proxy's own state, which is
  // also what `std::indirectly_writable` (and thus `ranges::sort`) requires.
  CPP_template(typename = void)(requires(!IsConst)) const BasicIdRef& operator=(
      const Id id) const {
    auto [datatype, payload] = getBitsCompat(id);
    *payload_ = payload;
    *datatype_ = datatype;
    return *this;
  }

  BasicIdRef& operator=(const BasicIdRef& other) {
    static_assert(!IsConst, "`ConstIdRef` is not assignable.");
    *payload_ = *other.payload_;
    *datatype_ = *other.datatype_;
    return *this;
  }

  // Swaps the referenced values, not the pointers. Takes `const&` so that it
  // also binds to the prvalues `*it` that `ranges::iter_swap` passes.
  friend void swap(const BasicIdRef& lhs, const BasicIdRef& rhs) noexcept {
    static_assert(!IsConst, "`ConstIdRef` is not swappable.");
    std::swap(*lhs.payload_, *rhs.payload_);
    std::swap(*lhs.datatype_, *rhs.datatype_);
  }

  // The following functions all just forward to the corresponding function
  // of the legacy `Id`, see `global/ValueId.h` for their documentation.
  [[nodiscard]] auto compareThreeWay(const Id& other) const {
    return toId().compareThreeWay(other);
  }
  [[nodiscard]] auto compareWithoutLocalVocab(const Id& other) const {
    return toId().compareWithoutLocalVocab(other);
  }
  [[nodiscard]] SplitLayoutIdBitRepresentation getBits() const noexcept {
    return {*datatype_, *payload_};
  }
  [[nodiscard]] Id::T getPayloadBits() const noexcept { return *payload_; }
  [[nodiscard]] Datatype getDatatype() const noexcept {
    return static_cast<Datatype>(*datatype_);
  }
  [[nodiscard]] Id::UndefinedType getUndefined() const noexcept { return {}; }
  [[nodiscard]] bool isUndefined() const noexcept {
    return toId().isUndefined();
  }
  [[nodiscard]] double getDouble() const noexcept { return toId().getDouble(); }
  [[nodiscard]] int64_t getInt() const noexcept { return toId().getInt(); }
  [[nodiscard]] bool getBool() const noexcept { return toId().getBool(); }
  [[nodiscard]] std::string_view getBoolLiteral() const noexcept {
    return toId().getBoolLiteral();
  }
  [[nodiscard]] VocabIndex getVocabIndex() const noexcept {
    return toId().getVocabIndex();
  }
  [[nodiscard]] uint64_t getEncodedVal() const noexcept {
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

  // Comparisons. Only one side needs to be a `BasicIdRef` for these to be
  // found via ADL; the other side is implicitly converted to `Id`.
  friend bool operator==(const BasicIdRef a, const Id b) {
    return a.toId() == b;
  }
  template <bool OtherConst>
  friend bool operator==(const BasicIdRef a, const BasicIdRef<OtherConst> b) {
    return a.toId() == b.operator Id();
  }
#ifdef QLEVER_CPP_17
  friend bool operator==(const Id a, const BasicIdRef b) {
    return a == b.toId();
  }
  friend bool operator!=(const BasicIdRef a, const Id b) { return !(a == b); }
  friend bool operator!=(const Id a, const BasicIdRef b) { return !(a == b); }
  template <bool OtherConst>
  friend bool operator!=(const BasicIdRef a, const BasicIdRef<OtherConst> b) {
    return !(a == b);
  }

  friend bool operator<(const BasicIdRef a, const Id b) { return a.toId() < b; }
  friend bool operator<(const Id a, const BasicIdRef b) { return a < b.toId(); }
  template <bool OtherConst>
  friend bool operator<(const BasicIdRef a, const BasicIdRef<OtherConst> b) {
    return a.toId() < b.operator Id();
  }
  friend bool operator<=(const BasicIdRef a, const Id b) {
    return a.toId() <= b;
  }
  friend bool operator<=(const Id a, const BasicIdRef b) {
    return a <= b.toId();
  }
  template <bool OtherConst>
  friend bool operator<=(const BasicIdRef a, const BasicIdRef<OtherConst> b) {
    return a.toId() <= b.operator Id();
  }
  friend bool operator>(const BasicIdRef a, const Id b) { return a.toId() > b; }
  friend bool operator>(const Id a, const BasicIdRef b) { return a > b.toId(); }
  template <bool OtherConst>
  friend bool operator>(const BasicIdRef a, const BasicIdRef<OtherConst> b) {
    return a.toId() > b.operator Id();
  }
  friend bool operator>=(const BasicIdRef a, const Id b) {
    return a.toId() >= b;
  }
  friend bool operator>=(const Id a, const BasicIdRef b) {
    return a >= b.toId();
  }
  template <bool OtherConst>
  friend bool operator>=(const BasicIdRef a, const BasicIdRef<OtherConst> b) {
    return a.toId() >= b.operator Id();
  }
#else
  friend auto operator<=>(const BasicIdRef a, const Id b) {
    return a.compareThreeWay(b);
  }
  template <bool OtherConst>
  friend auto operator<=>(const BasicIdRef a, const BasicIdRef<OtherConst> b) {
    return a.compareThreeWay(b.operator Id());
  }
#endif

  friend std::ostream& operator<<(std::ostream& ostr, const BasicIdRef ref) {
    return ostr << ref.toId();
  }

 private:
  [[nodiscard]] Id toId() const { return *this; }
};

using IdRef = BasicIdRef<false>;
using ConstIdRef = BasicIdRef<true>;

}  // namespace columnBasedIdTable::splitLayout

#endif  // QLEVER_SRC_ENGINE_IDTABLE_SPLITLAYOUT_IDREF_H
