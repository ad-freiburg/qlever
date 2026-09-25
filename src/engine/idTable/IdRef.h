// Copyright 2026, University of Freiburg,
// Chair of Algorithms and Data Structures.

#ifndef QLEVER_SRC_ENGINE_IDTABLE_IDREF_H
#define QLEVER_SRC_ENGINE_IDTABLE_IDREF_H

#include <ostream>

#include "backports/concepts.h"
#include "global/Id.h"
#include "util/Forward.h"

namespace columnBasedIdTable {

// A reference to a single `Id` stored in split-column storage (a payload
// word + datatype byte in two separate arrays, not a contiguous `Id`).
// `BasicIdRef<IsConst>` mirrors `Id`'s full read API, so `column[i].method()`
// keeps compiling once a column's element type changes from `Id&`/`const
// Id&` to this proxy. `IdRef` additionally supports assigning a new `Id` to
// the referenced slot. A thin, cheap-to-copy pair of pointers, not
// polymorphic, like `Id` itself.
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
  BasicIdRef(BasicIdRef&&) = default;

  // Implicit conversion to `Id`, e.g. to store the referenced value in a
  // variable, or to pass it to a function that expects a real `Id`.
  /*implicit*/ operator Id() const {
    return Id::fromBits({*datatype_, *payload_});
  }

  // Assign a new `Id` to the referenced slot (`IdRef` only). `const`, like
  // any proxy reference (e.g. `std::vector<bool>::reference`): it writes
  // through to the referenced slot, not to the proxy's own state, which is
  // also what `std::indirectly_writable` (and thus `ranges::sort`) requires.
  CPP_template(typename = void)(requires(!IsConst)) const BasicIdRef&
  operator=(Id id) const {
    auto bits = id.getBits();
    *payload_ = bits.payload_;
    *datatype_ = bits.datatype_;
    return *this;
  }

  // NOT redundant with `operator=(Id)` above: without this, `a = b` (used
  // internally by e.g. `ranges::sort` to swap elements) would resolve to the
  // compiler-generated copy-assignment instead -- an identity match always
  // beats our user-defined `BasicIdRef` -> `Id` conversion -- which would
  // just rebind `a`'s pointers instead of writing through them, silently
  // corrupting data.
  const BasicIdRef& operator=(const BasicIdRef& other) const {
    static_assert(!IsConst, "`ConstIdRef` is not assignable.");
    return *this = static_cast<Id>(other);
  }

  // The following functions all just forward to the corresponding function
  // of `Id`, see `global/ValueId.h` for their documentation.
  [[nodiscard]] auto compareThreeWay(const Id& other) const {
    return toId().compareThreeWay(other);
  }
  [[nodiscard]] auto compareWithoutLocalVocab(const Id& other) const {
    return toId().compareWithoutLocalVocab(other);
  }
  [[nodiscard]] Id::BitRepresentation getBits() const noexcept {
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
  friend bool operator==(BasicIdRef a, Id b) { return a.toId() == b; }
  friend bool operator==(Id a, BasicIdRef b) { return a == b.toId(); }
  template <bool OtherConst>
  friend bool operator==(BasicIdRef a, BasicIdRef<OtherConst> b) {
    return a.toId() == b.operator Id();
  }
  friend bool operator!=(BasicIdRef a, Id b) { return !(a == b); }
  friend bool operator!=(Id a, BasicIdRef b) { return !(a == b); }
  template <bool OtherConst>
  friend bool operator!=(BasicIdRef a, BasicIdRef<OtherConst> b) {
    return !(a == b);
  }

  // Relational operators, e.g. needed for `ql::ranges::equal_range`/`sort`
  // on an `Id` column. Forwarded to `Id`'s own operators (datatype-major
  // bitwise comparison, see `ValueId::compareThreeWay`).
  friend bool operator<(BasicIdRef a, Id b) { return a.toId() < b; }
  friend bool operator<(Id a, BasicIdRef b) { return a < b.toId(); }
  template <bool OtherConst>
  friend bool operator<(BasicIdRef a, BasicIdRef<OtherConst> b) {
    return a.toId() < b.operator Id();
  }
  friend bool operator<=(BasicIdRef a, Id b) { return a.toId() <= b; }
  friend bool operator<=(Id a, BasicIdRef b) { return a <= b.toId(); }
  template <bool OtherConst>
  friend bool operator<=(BasicIdRef a, BasicIdRef<OtherConst> b) {
    return a.toId() <= b.operator Id();
  }
  friend bool operator>(BasicIdRef a, Id b) { return a.toId() > b; }
  friend bool operator>(Id a, BasicIdRef b) { return a > b.toId(); }
  template <bool OtherConst>
  friend bool operator>(BasicIdRef a, BasicIdRef<OtherConst> b) {
    return a.toId() > b.operator Id();
  }
  friend bool operator>=(BasicIdRef a, Id b) { return a.toId() >= b; }
  friend bool operator>=(Id a, BasicIdRef b) { return a >= b.toId(); }
  template <bool OtherConst>
  friend bool operator>=(BasicIdRef a, BasicIdRef<OtherConst> b) {
    return a.toId() >= b.operator Id();
  }

  friend std::ostream& operator<<(std::ostream& ostr, BasicIdRef ref) {
    return ostr << ref.toId();
  }

 private:
  [[nodiscard]] Id toId() const { return *this; }
};

// `IdRef` allows both reading and (whole-value) writing of the referenced
// `Id`, `ConstIdRef` only reading.
using IdRef = BasicIdRef<false>;
using ConstIdRef = BasicIdRef<true>;

}  // namespace columnBasedIdTable

#endif  // QLEVER_SRC_ENGINE_IDTABLE_IDREF_H
