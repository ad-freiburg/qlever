#ifndef QLEVER_SRC_UTIL_FILTERBYINDICESVIEW_H
#define QLEVER_SRC_UTIL_FILTERBYINDICESVIEW_H

#include <cstddef>
#include <iterator>
#include <utility>

#include "backports/algorithm.h"
#include "backports/iterator.h"
#include "util/Exception.h"

namespace ad_utility {

// A view that takes an underlying generator/range of values for all rows in a
// block and a sorted range of row indices and yields only the values at those
// indices. The underlying generator is advanced without dereferencing for the
// skipped indices.
template <typename GeneratorRange, typename IndicesRange>
class FilterByIndicesView {
 private:
  GeneratorRange generator_;
  IndicesRange indices_;
  size_t numItems_ = 0;

 public:
  using value_type = ql::ranges::range_value_t<GeneratorRange>;

  FilterByIndicesView(GeneratorRange generator, IndicesRange indices,
                      size_t numItems)
      : generator_{std::move(generator)},
        indices_{std::move(indices)},
        numItems_{numItems} {}

  class iterator {
   private:
    ql::ranges::iterator_t<GeneratorRange> genIt_{};
    ql::ranges::sentinel_t<GeneratorRange> genEnd_{};
    ql::ranges::iterator_t<IndicesRange> idxIt_{};
    ql::ranges::sentinel_t<IndicesRange> idxEnd_{};

    void advanceToTarget(size_t previousTarget, size_t currentTarget) {
      AD_CONTRACT_CHECK(currentTarget >= previousTarget);
      ql::ranges::advance(genIt_, currentTarget - previousTarget, genEnd_);
    }

   public:
    using iterator_category = std::input_iterator_tag;
    using value_type = ql::ranges::range_value_t<GeneratorRange>;
    using difference_type = std::ptrdiff_t;
    using reference = decltype(*genIt_);

    iterator() = default;

    iterator(GeneratorRange* generator, IndicesRange* indices, size_t) {
      genIt_ = ql::ranges::begin(*generator);
      genEnd_ = ql::ranges::end(*generator);
      idxIt_ = ql::ranges::begin(*indices);
      idxEnd_ = ql::ranges::end(*indices);
      if (idxIt_ == idxEnd_) {
        return;
      }
      auto currentTarget = *idxIt_;
      advanceToTarget(0, currentTarget);
    }

    reference operator*() const { return *genIt_; }

    iterator& operator++() {
      auto previousTarget = *idxIt_;
      ++idxIt_;
      if (idxIt_ == idxEnd_) {
        return *this;
      }
      auto currentTarget = *idxIt_;
      advanceToTarget(previousTarget, currentTarget);
      return *this;
    }

    void operator++(int) { (void)++(*this); }

    friend bool operator==(const iterator& it, ql::default_sentinel_t) {
      return it.genIt_ == it.genEnd_ || it.idxIt_ == it.idxEnd_;
    }

    friend bool operator!=(const iterator& it, ql::default_sentinel_t s) {
      return !(it == s);
    }
  };

  iterator begin() { return iterator{&generator_, &indices_, numItems_}; }

  ql::default_sentinel_t end() { return {}; }
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_FILTERBYINDICESVIEW_H
