// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Benke Hargitai <hargitab@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_SPARQLEXPRESSIONGENERATORSPARSE_H
#define QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_SPARQLEXPRESSIONGENERATORSPARSE_H

#include "engine/sparqlExpressions/SparqlExpressionGenerators.h"
#include "util/FilterByIndicesView.h"

// A wrapper range that takes an underlying generator/range of values for all
// rows in a block and a sorted range of row indices and yields only the
// values at those indices. The underlying generator is advanced without
// dereferencing for the skipped indices.
namespace sparqlExpression::detail {

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
  ad_utility::FilterByIndicesView<GeneratorRange, IndicesRange> sparseRange{
      std::move(denseGenerator), std::move(indices), numItems};

  return sparseRange;
}

}  // namespace sparqlExpression::detail

#endif  // QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_SPARQLEXPRESSIONGENERATORSPARSE_H
