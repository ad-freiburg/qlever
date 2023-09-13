// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_IDTABLEITERATOR_H
#define QLEVER_IDTABLEITERATOR_H

#include "engine/idTable/IdTableRow.h"
#include "util/Enums.h"
#include "util/Iterators.h"
#include "util/TypeTraits.h"

namespace columnBasedIdTable {

// Iterators for the `IdTable` class, there is one implementation for the
// dynamic case (`NumCols == 0`) and one for the (more efficient) static case.
template <typename Reference, typename RestrictedReference>
struct IdTableIterator;

// Helper `struct` that stores a pointer to this table and has an `operator()`
// that can be called with a reference to an `IdTable` and the index of a row
// and then returns a `row_reference_restricted` to that row. This struct is
// used to automatically create random access iterators using the
// `ad_utility::IteratorForAccessOperator` template.
template <typename ReferenceType>
struct IteratorHelper {
  auto operator()(auto&& idTable, size_t rowIdx) const {
    return ReferenceType{&idTable, rowIdx};
  }
};

// Implementation for the dynamic case. Is directly derived from
// `IteratorForAccessOperator`, but needs to use the more involved mixin variant
// because we want a unique interface to the iterator classes in the `IdTable`
// to reduce the complexity there.
template <typename Reference, typename RestrictedReference>
requires(Reference::numStaticColumns == 0)
struct IdTableIterator<Reference, RestrictedReference>
    : public ad_utility::IteratorForAccessOperatorMixin<
          IdTableIterator<Reference, RestrictedReference>,
          typename Reference::table_type, IteratorHelper<RestrictedReference>,
          RestrictedReference::isConstTag,
          typename RestrictedReference::row_type, Reference> {
  using Base = ad_utility::IteratorForAccessOperatorMixin<
      IdTableIterator<Reference, RestrictedReference>,
      typename Reference::table_type, IteratorHelper<RestrictedReference>,
      RestrictedReference::isConstTag, typename RestrictedReference::row_type,
      Reference>;
  using Base::Base;
  IdTableIterator() = default;
  IdTableIterator(const Base& b) : Base{b} {}
  IdTableIterator(Base&& b) : Base{std::move(b)} {}
};

// A random access iterator for static `IdTables`  (NumCols != 0). Needs to be
// implemented manually.
template <typename Reference, typename RestrictedReference>
requires(RestrictedReference::numStaticColumns > 0)
class IdTableIterator<Reference, RestrictedReference> {
 public:
  // Most of these typedefs are needed by the STL to properly accept this class
  // as a random access iterator.
  using reference = Reference;
  using T = typename reference::value_type;
  static constexpr size_t N = reference::numStaticColumns;
  using value_type = Row<T, N>;
  using pointer = value_type*;
  static constexpr bool isConst = reference::isConst;
  using iterator_category = std::random_access_iterator_tag;
  using difference_type = int64_t;

 private:
  RestrictedReference ref_;

 public:
  // Default constructor.
  IdTableIterator() = default;

  // Construct from a pointer to an `IdTable` and a row index.
  // Note that all copy and assignment operations on iterators need to set the
  // pointers of the underlying `RowReference`, not the values that those
  // point to.
  template <ad_utility::SimilarTo<typename Reference::table_type> Table>
  IdTableIterator(Table* table, size_t index) {
    for (size_t i : std::views::iota(0u, N)) {
      ref_.ptrs_[i] = table->getColumn(i).data() + index;
    }
  }

  // Copy constructor and copy assignment.
  IdTableIterator(const IdTableIterator& other) {
    for (size_t i = 0; i < N; ++i) {
      ref_.ptrs_[i] = other.ref_.ptrs_[i];
    }
  }

  IdTableIterator& operator=(const IdTableIterator& other) {
    for (size_t i = 0; i < N; ++i) {
      ref_.ptrs_[i] = other.ref_.ptrs_[i];
    }
    return *this;
  }

  // Comparison is done by comparing the first pointer.
  auto operator<=>(const IdTableIterator& rhs) const {
    return ref_.ptrs_[0] <=> rhs.ref_.ptrs_[0];
  }

  bool operator==(const IdTableIterator& rhs) const {
    return ref_.ptrs_[0] == rhs.ref_.ptrs_[0];
  }

  // All the arithmetic operators on iterators. All of them are implemented by
  // adding a certain offset to the underlying pointers.
  IdTableIterator& operator+=(difference_type n) {
    ref_.increase(n);
    return *this;
  }
  IdTableIterator operator+(difference_type n) const {
    IdTableIterator result{*this};
    result += n;
    return result;
  }

  IdTableIterator& operator++() {
    ref_.increase(1);
    return *this;
  }
  IdTableIterator operator++(int) & {
    IdTableIterator result{*this};
    ++result;
    return result;
  }

  IdTableIterator& operator--() {
    ref_.increase(-1);
    return *this;
  }
  IdTableIterator operator--(int) & {
    IdTableIterator result{*this};
    --result;
    return result;
  }

  friend IdTableIterator operator+(difference_type n,
                                   const IdTableIterator& it) {
    return it + n;
  }

  IdTableIterator& operator-=(difference_type n) {
    ref_.increase(-n);
    return *this;
  }

  IdTableIterator operator-(difference_type n) const {
    IdTableIterator result{*this};
    result -= n;
    return result;
  }

  difference_type operator-(const IdTableIterator& rhs) const {
    return ref_.ptrs_[0] - rhs.ref_.ptrs_[0];
  }

  // Dereferencing operators.
  // Note: We currently violate the logical constness here, as we would need to
  // return `ConstRestrictedReferences` for the const overloads.
  // TODO<joka921> Implement the necessary machinery and test that it works
  // correctly.
  RestrictedReference operator*() const { return ref_; }
  RestrictedReference operator*() requires(!isConst) { return ref_; }

  RestrictedReference operator[](difference_type n) const {
    auto ref = ref_;
    ref.increase(n);
    return ref;
  }
};
}  // namespace columnBasedIdTable

#endif  // QLEVER_IDTABLEITERATOR_H
