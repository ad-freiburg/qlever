//  Copyright 2020, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "Bind.h"

#include "engine/CallFixedSize.h"
#include "engine/QueryExecutionTree.h"
#include "engine/sparqlExpressions/SparqlExpression.h"
#include "engine/sparqlExpressions/SparqlExpressionGenerators.h"
#include "util/Exception.h"

// BIND adds exactly one new column
size_t Bind::getResultWidth() const { return _subtree->getResultWidth() + 1; }

// BIND doesn't change the number of result rows
uint64_t Bind::getSizeEstimateBeforeLimit() {
  return _subtree->getSizeEstimate();
}

// BIND has cost linear in the size of the input. Note that BIND operations are
// currently always executed at their position in the SPARQL query, so that this
// cost estimate has no effect on query optimization (there is only one
// alternative).
size_t Bind::getCostEstimate() {
  return _subtree->getCostEstimate() + _subtree->getSizeEstimate();
}

float Bind::getMultiplicity(size_t col) {
  // this is the newly added column
  if (col == getResultWidth() - 1) {
    // TODO<joka921> get a better multiplicity estimate for BINDs which are
    // variable renames or constants.
    return 1;
  }

  // one of the columns that was only copied from the input.
  return _subtree->getMultiplicity(col);
}

// _____________________________________________________________________________
string Bind::getDescriptor() const { return _bind.getDescriptor(); }

// _____________________________________________________________________________
[[nodiscard]] vector<ColumnIndex> Bind::resultSortedOn() const {
  // We always append the result column of the BIND at the end and this column
  // is not sorted, so the sequence of indices of the sorted columns do not
  // change.
  return _subtree->resultSortedOn();
}

// _____________________________________________________________________________
bool Bind::knownEmptyResult() { return _subtree->knownEmptyResult(); }

// _____________________________________________________________________________
string Bind::getCacheKeyImpl() const {
  std::ostringstream os;
  os << "BIND ";
  os << _bind._expression.getCacheKey(_subtree->getVariableColumns());
  os << "\n" << _subtree->getCacheKey();
  return std::move(os).str();
}

// _____________________________________________________________________________
VariableToColumnMap Bind::computeVariableToColumnMap() const {
  auto res = _subtree->getVariableColumns();
  // The new variable is always appended at the end.
  // TODO<joka921> This currently pessimistically assumes that all (aggregate)
  // expressions can produce undefined values. This might impact the
  // performance when the result of this GROUP BY is joined on one or more of
  // the aggregating columns. Implement an interface in the expressions that
  // allows to check, whether an expression can never produce an undefined
  // value.
  res[_bind._target] = makePossiblyUndefinedColumn(getResultWidth() - 1);
  return res;
}

// _____________________________________________________________________________
std::vector<QueryExecutionTree*> Bind::getChildren() {
  return {_subtree.get()};
}

// _____________________________________________________________________________
IdTable Bind::cloneSubView(const IdTable& idTable,
                           const std::pair<size_t, size_t>& subrange) {
  IdTable result(idTable.numColumns(), idTable.getAllocator());
  result.resize(subrange.second - subrange.first);
  std::ranges::copy(idTable.begin() + subrange.first,
                    idTable.begin() + subrange.second, result.begin());
  return result;
}

// _____________________________________________________________________________
ProtoResult Bind::computeResult(bool requestLaziness) {
  LOG(DEBUG) << "Get input to BIND operation..." << std::endl;
  std::shared_ptr<const Result> subRes = _subtree->getResult(requestLaziness);
  LOG(DEBUG) << "Got input to Bind operation." << std::endl;

  auto applyBind = [this, subRes](
                       auto&& idTable, LocalVocab* localVocab,
                       std::optional<std::pair<size_t, size_t>> subrange =
                           std::nullopt) {
    return computeExpressionBind(localVocab, AD_FWD(idTable),
                                 subRes->localVocab(),
                                 _bind._expression.getPimpl(), subrange);
  };

  if (subRes->isFullyMaterialized()) {
    if (requestLaziness && subRes->idTable().size() > CHUNK_SIZE) {
      auto localVocab =
          std::make_shared<LocalVocab>(subRes->getCopyOfLocalVocab());
      auto generator = [](std::shared_ptr<LocalVocab> vocab, auto applyBind,
                          std::shared_ptr<const Result> result)
          -> cppcoro::generator<IdTable> {
        size_t size = result->idTable().size();
        for (size_t offset = 0; offset < size; offset += CHUNK_SIZE) {
          co_yield applyBind(result->idTable(), vocab.get(),
                             {{offset, std::min(size, offset + CHUNK_SIZE)}});
        }
      }(localVocab, std::move(applyBind), std::move(subRes));
      return {std::move(generator), resultSortedOn(), std::move(localVocab)};
    }
    // Make a deep copy of the local vocab from `subRes` and then add to it (in
    // case BIND adds a new word or words).
    //
    // TODO: In most BIND operations, nothing is added to the local vocabulary,
    // so it would be more efficient to first share the pointer here (like with
    // `shareLocalVocabFrom`) and only copy it when a new word is about to be
    // added. Same for GROUP BY.
    LocalVocab localVocab = subRes->getCopyOfLocalVocab();
    IdTable result = applyBind(subRes->idTable(), &localVocab);
    LOG(DEBUG) << "BIND result computation done." << std::endl;
    return {std::move(result), resultSortedOn(), std::move(localVocab)};
  }
  auto localVocab = std::make_shared<LocalVocab>();
  auto generator =
      [](std::shared_ptr<LocalVocab> vocab, auto applyBind,
         std::shared_ptr<const Result> result) -> cppcoro::generator<IdTable> {
    for (IdTable& idTable : result->idTables()) {
      co_yield applyBind(idTable, vocab.get());
    }
    std::array<const LocalVocab*, 2> vocabs{vocab.get(), &result->localVocab()};
    *vocab = LocalVocab::merge(std::span{vocabs});
  }(localVocab, std::move(applyBind), std::move(subRes));
  return {std::move(generator), resultSortedOn(), std::move(localVocab)};
}

// _____________________________________________________________________________
IdTable Bind::computeExpressionBind(
    LocalVocab* outputLocalVocab,
    ad_utility::SimilarTo<IdTable> auto&& inputIdTable,
    const LocalVocab& inputLocalVocab,
    sparqlExpression::SparqlExpression* expression,
    std::optional<std::pair<size_t, size_t>> subrange) const {
  sparqlExpression::EvaluationContext evaluationContext(
      *getExecutionContext(), _subtree->getVariableColumns(), inputIdTable,
      getExecutionContext()->getAllocator(), inputLocalVocab,
      cancellationHandle_, deadline_);

  if (subrange.has_value()) {
    auto [beginIndex, endIndex] = subrange.value();
    evaluationContext._beginIndex = beginIndex;
    evaluationContext._endIndex = endIndex;
  }

  sparqlExpression::ExpressionResult expressionResult =
      expression->evaluate(&evaluationContext);

  auto output = subrange.has_value()
                    ? cloneSubView(inputIdTable, subrange.value())
                    : std::move(inputIdTable).cloneOrMove();
  output.addEmptyColumn();
  auto outputColumn = output.getColumn(output.numColumns() - 1);

  auto visitor = [&]<sparqlExpression::SingleExpressionResult T>(
                     T&& singleResult) mutable {
    constexpr static bool isVariable = std::is_same_v<T, ::Variable>;
    constexpr static bool isStrongId = std::is_same_v<T, Id>;

    if constexpr (isVariable) {
      auto columnIndex =
          getInternallyVisibleVariableColumns().at(singleResult).columnIndex_;
      auto inputColumn = output.getColumn(columnIndex);
      for (size_t i = 0; i < outputColumn.size(); ++i) {
        outputColumn[i] = inputColumn[i];
        checkCancellation();
      }
    } else if constexpr (isStrongId) {
      for (size_t i = 0; i < outputColumn.size(); ++i) {
        outputColumn[i] = singleResult;
        checkCancellation();
      }
    } else {
      constexpr bool isConstant = sparqlExpression::isConstantResult<T>;

      auto resultGenerator = sparqlExpression::detail::makeGenerator(
          std::forward<T>(singleResult), outputColumn.size(),
          &evaluationContext);

      if constexpr (isConstant) {
        auto it = resultGenerator.begin();
        if (it != resultGenerator.end()) {
          Id constantId =
              sparqlExpression::detail::constantExpressionResultToId(
                  std::move(*it), *outputLocalVocab);
          for (size_t i = 0; i < outputColumn.size(); ++i) {
            outputColumn[i] = constantId;
            checkCancellation();
          }
        }
      } else {
        size_t i = 0;
        // We deliberately move the values from the generator.
        for (auto& resultValue : resultGenerator) {
          outputColumn[i] =
              sparqlExpression::detail::constantExpressionResultToId(
                  std::move(resultValue), *outputLocalVocab);
          i++;
          checkCancellation();
        }
      }
    }
  };

  std::visit(visitor, std::move(expressionResult));

  return output;
}
