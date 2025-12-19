// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Benke Hargitai <hargitab@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_SPARQLEXPRESSIONGENERATORSPARSE_H
#define QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_SPARQLEXPRESSIONGENERATORSPARSE_H

#include "engine/sparqlExpressions/SparqlExpressionGenerators.h"

// A wrapper range that takes an underlying generator/range of values for all
// rows in a block and a sorted range of row indices and yields only the
// values at those indices. The underlying generator is advanced without
// dereferencing for the skipped indices.
namespace sparqlExpression::detail {

template <typename GeneratorRange, typename IndicesRange>
class SparseGeneratorRange {
 private:
  GeneratorRange generator_;
  IndicesRange indices_;
  size_t numItems_ = 0;

 public:
  using value_type = ql::ranges::range_value_t<GeneratorRange>;

  SparseGeneratorRange(GeneratorRange generator, IndicesRange indices,
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
    size_t numItems_ = 0;

    void advanceToTarget(size_t previousTarget, size_t currentTarget) {
      AD_CONTRACT_CHECK(currentTarget >= previousTarget &&
                        currentTarget < numItems_);
      ql::ranges::advance(genIt_, currentTarget - previousTarget);
    }

   public:
    using iterator_category = std::input_iterator_tag;
    using value_type = ql::ranges::range_value_t<GeneratorRange>;
    using difference_type = std::ptrdiff_t;
    using reference = decltype(*genIt_);

    iterator() = default;

    iterator(GeneratorRange* generator, IndicesRange* indices, size_t numItems)
        : numItems_{numItems} {
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
      if (idxIt_ == idxEnd_) {
        return *this;
      }
      ++idxIt_;
      if (idxIt_ == idxEnd_) {
        return *this;
      }
      auto previousTarget = *std::prev(idxIt_);
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

/// Return a generator that yields values only for the rows whose indices are
/// contained in `indices`. The indices must be sorted in ascending order and
/// must be strictly smaller than `numItems`. The underlying generator is
/// advanced for all rows, but only the values at the selected indices are
/// exposed.
CPP_template(typename Input, typename Indices,
             typename Transformation = ql::identity)(
    requires SingleExpressionResult<Input> CPP_and ql::ranges::input_range<
        Indices>
        CPP_and ql::concepts::same_as<
            ql::ranges::range_value_t<Indices>,
            size_t>) auto makeGeneratorSparse(Input&& input, size_t numItems,
                                              const EvaluationContext* context,
                                              Indices indices,
                                              Transformation transformation =
                                                  {}) {
  auto denseGenerator = makeGenerator(AD_FWD(input), numItems, context,
                                      std::move(transformation));

  using GeneratorRange = decltype(denseGenerator);
  using IndicesRange = std::decay_t<Indices>;
  SparseGeneratorRange<GeneratorRange, IndicesRange> sparseRange{
      std::move(denseGenerator), std::move(indices), numItems};

  return sparseRange;
}

}  // namespace sparqlExpression::detail

#endif  // QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_SPARQLEXPRESSIONGENERATORSPARSE_H
