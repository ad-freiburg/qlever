// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_VIEWS_ZIPMERGEUNIQUE_H
#define QLEVER_SRC_UTIL_VIEWS_ZIPMERGEUNIQUE_H

#include <functional>

#include "backports/algorithm.h"
#include "backports/concepts.h"
#include "backports/functional.h"
#include "backports/iterator.h"
#include "backports/type_traits.h"
#include "util/Views.h"

namespace ad_utility {

// Lazily merges and deduplicates two sorted ranges into a single sorted
// sequence.
//
// Elements are yielded one at a time. The `Compare` function is applied to the
// projected values (`Projection` applied to each element) to determine order.
// When both ranges contain an element that compares equal under the projection,
// only the element from the *second* range is yielded and the element from the
// *first* range is skipped (last-wins tie-break).
CPP_template(typename V1, typename V2, typename Compare = std::less<>,
             class Projection = ql::identity)(
    requires ql::ranges::input_range<V1>&& ql::ranges::view<V1>&&
        ql::ranges::input_range<V2>&&
            ql::ranges::view<V2>) class ZipMergeUniqueView
    : public ql::ranges::view_interface<
          ZipMergeUniqueView<V1, V2, Compare, Projection>> {
 private:
  V1 v1_;
  V2 v2_;
  [[no_unique_address]] Compare comp_;
  [[no_unique_address]] Projection proj_;

  template <bool IsConst>
  class IteratorImpl {
   private:
    using V1_ = std::conditional_t<IsConst, const V1, V1>;
    using V2_ = std::conditional_t<IsConst, const V2, V2>;

   public:
    using It1 = ql::ranges::iterator_t<V1_>;
    using Se1 = ql::ranges::sentinel_t<V1_>;
    using It2 = ql::ranges::iterator_t<V2_>;
    using Se2 = ql::ranges::sentinel_t<V2_>;

    static constexpr bool IsForwardIterator =
        ql::ranges::forward_range<V1_> && ql::ranges::forward_range<V2_>;
    using iterator_category =
        std::conditional_t<IsForwardIterator, std::forward_iterator_tag,
                           std::input_iterator_tag>;
    using iterator_concept = iterator_category;
    using value_type = std::common_type_t<ql::ranges::range_value_t<V1_>,
                                          ql::ranges::range_value_t<V2_>>;
    using reference =
        ql::common_reference_t<ql::ranges::range_reference_t<V1_>,
                               ql::ranges::range_reference_t<V2_>>;
    using difference_type = ql::ranges::range_difference_t<V1_>;
    using pointer = std::add_pointer_t<reference>;

   private:
    It1 it1_;
    It2 it2_;
    // We also need to store the sentinels of the iterators, because one
    // iterator can be exhausted before the other, and we need to be able to
    // detect that.
    [[no_unique_address]] Se1 end1_;
    [[no_unique_address]] Se2 end2_;
    const Compare* comp_;
    const Projection* proj_;

    enum class Decision {
      UseFirst,           // yield *it1_ and advance it1_
      UseSecond,          // yield *it2_ and advance it2_
      UseSecondSkipFirst  // equal projection: yield *it2_, advance both
    };

    Decision decision_;

    Decision decide() const {
      if (it2_ == end2_) return Decision::UseFirst;
      if (it1_ == end1_) return Decision::UseSecond;
      const auto& p1 = std::invoke(*proj_, *it1_);
      const auto& p2 = std::invoke(*proj_, *it2_);
      if (std::invoke(*comp_, p1, p2)) return Decision::UseFirst;
      if (std::invoke(*comp_, p2, p1)) return Decision::UseSecond;
      return Decision::UseSecondSkipFirst;
    }

   public:
    IteratorImpl() = default;
    IteratorImpl(It1 b1, Se1 e1, It2 b2, Se2 e2, const Compare* comp,
                 const Projection* proj)
        : it1_(std::move(b1)),
          it2_(std::move(b2)),
          end1_(std::move(e1)),
          end2_(std::move(e2)),
          comp_(comp),
          proj_(proj) {
      decision_ = decide();
    }

    reference operator*() const {
      return (decision_ == Decision::UseFirst) ? *it1_ : *it2_;
    }
    pointer operator->() const { return std::addressof(**this); }

    IteratorImpl& operator++() {
      switch (decision_) {
        case Decision::UseFirst:
          ++it1_;
          break;
        case Decision::UseSecond:
          ++it2_;
          break;
        case Decision::UseSecondSkipFirst:
          ++it1_;
          ++it2_;
          break;
      }
      decision_ = decide();
      return *this;
    }
    IteratorImpl operator++(int) requires(IsForwardIterator) {
      IteratorImpl tmp = *this;
      ++(*this);
      return tmp;
    }
    void operator++(int) requires(!IsForwardIterator) { ++(*this); }

    bool operator==(const IteratorImpl& o) const {
      return it1_ == o.it1_ && it2_ == o.it2_;
    }
    bool operator!=(const IteratorImpl& o) const { return !(*this == o); }

    bool operator==(ql::default_sentinel_t) const {
      return it1_ == end1_ && it2_ == end2_;
    }
    bool operator!=(ql::default_sentinel_t s) const { return !(*this == s); }
    friend bool operator==(ql::default_sentinel_t s, const IteratorImpl& it) {
      return it == s;
    }
    friend bool operator!=(ql::default_sentinel_t s, const IteratorImpl& it) {
      return it != s;
    }
  };

  using Iterator = IteratorImpl<false>;
  using ConstIterator = IteratorImpl<true>;

 public:
  using value_type = typename Iterator::value_type;

  ZipMergeUniqueView() = default;
  ZipMergeUniqueView(V1 v1, V2 v2, Compare comp = {}, Projection proj = {})
      : v1_(std::move(v1)),
        v2_(std::move(v2)),
        comp_(std::move(comp)),
        proj_(std::move(proj)) {
    if constexpr (std::is_pointer_v<Compare> ||
                  std::is_member_pointer_v<Compare>) {
      AD_CONTRACT_CHECK(comp_ != nullptr);
    }
    if constexpr (std::is_pointer_v<Projection> ||
                  std::is_member_pointer_v<Projection>) {
      AD_CONTRACT_CHECK(proj_ != nullptr);
    }
  }

  Iterator begin() {
    return Iterator{ql::ranges::begin(v1_),
                    ql::ranges::end(v1_),
                    ql::ranges::begin(v2_),
                    ql::ranges::end(v2_),
                    &comp_,
                    &proj_};
  }

  CPP_member auto begin() const
      -> CPP_ret(ConstIterator)(
          requires ql::ranges::range<const V1>&& ql::ranges::range<const V2>) {
    return ConstIterator{ql::ranges::begin(v1_),
                         ql::ranges::end(v1_),
                         ql::ranges::begin(v2_),
                         ql::ranges::end(v2_),
                         &comp_,
                         &proj_};
  }

  ql::default_sentinel_t end() { return {}; }
  ql::default_sentinel_t end() const { return {}; }
};

// Deduction guides.
template <class R1, class R2>
ZipMergeUniqueView(R1&&, R2&&) -> ZipMergeUniqueView<all_t<R1>, all_t<R2>>;

template <class R1, class R2, class Compare>
ZipMergeUniqueView(R1&&, R2&&, Compare)
    -> ZipMergeUniqueView<all_t<R1>, all_t<R2>, Compare>;

template <class R1, class R2, class Compare, class Projection>
ZipMergeUniqueView(R1&&, R2&&, Compare, Projection)
    -> ZipMergeUniqueView<all_t<R1>, all_t<R2>, Compare, Projection>;

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_VIEWS_ZIPMERGEUNIQUE_H
