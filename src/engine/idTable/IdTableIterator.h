// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_IDTABLEITERATOR_H
#define QLEVER_IDTABLEITERATOR_H

namespace columnBasedIdTable {

template <typename Reference>
concept RowRefC =
    std::is_integral_v<decltype(Reference::numStaticColumns)> &&
    ad_utility::SimilarTo<ad_utility::IsConst, decltype(Reference::isConstTag)>;

template <RowRefC Reference, typename RestrictedReference>
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

template <RowRefC Reference, typename RestrictedReference>
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

// A random access iterator for static `IdTables` that uses the
// `StaticRowReference` from above.
template <RowRefC Reference, typename RestrictedReference>
requires(RestrictedReference::numStaticColumns > 0)
struct IdTableIterator<Reference, RestrictedReference> {
  using reference = Reference;
  using T = typename reference::value_type;
  static constexpr size_t N = reference::numStaticColumns;
  static constexpr bool isConst = reference::isConst;
  using value_type = Row<T, N>;
  using pointer = value_type*;
  RestrictedReference ref_;
  using iterator_category = std::random_access_iterator_tag;
  using difference_type = int64_t;

  explicit IdTableIterator(const reference::Ptrs& arr) : ref_{arr} {}

  template <ad_utility::SimilarTo<typename Reference::table_type> Table>
  IdTableIterator(Table* table, size_t index)
      : ref_{[&]() {
          typename RestrictedReference::Ptrs arr;
          for (size_t i : std::views::iota(0u, N)) {
            arr[i] = table->getColumn(i).data() + index;
          }
          return arr;
        }()} {}

 private:
 public:
  IdTableIterator& operator=(const IdTableIterator& other) {
    for (size_t i = 0; i < N; ++i) {
      ref_.ptrs_[i] = other.ref_.ptrs_[i];
    }
    return *this;
  }
  IdTableIterator& operator=(IdTableIterator&& other) {
    for (size_t i = 0; i < N; ++i) {
      ref_.ptrs_[i] = other.ref_.ptrs_[i];
    }
    return *this;
  }

  IdTableIterator(const IdTableIterator& other) {
    for (size_t i = 0; i < N; ++i) {
      ref_.ptrs_[i] = other.ref_.ptrs_[i];
    }
  }
  IdTableIterator(IdTableIterator&& other) {
    for (size_t i = 0; i < N; ++i) {
      ref_.ptrs_[i] = other.ref_.ptrs_[i];
    }
  }

  IdTableIterator() = default;

  auto operator<=>(const IdTableIterator& rhs) const {
    return ref_.ptrs_[0] <=> rhs.ref_.ptrs_[0];
  }

  bool operator==(const IdTableIterator& rhs) const {
    return ref_.ptrs_[0] == rhs.ref_.ptrs_[0];
  }

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

  // TODO<joka921> We have shallow constness here...
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
