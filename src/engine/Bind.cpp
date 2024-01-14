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
void Bind::setTextLimit(size_t limit) { _subtree->setTextLimit(limit); }

// _____________________________________________________________________________
std::vector<QueryExecutionTree*> Bind::getChildren() {
  return {_subtree.get()};
}

// _____________________________________________________________________________
ResultTable Bind::computeResult() {
  using std::endl;
  LOG(DEBUG) << "Get input to BIND operation..." << endl;
  shared_ptr<const ResultTable> subRes = _subtree->getResult();
  LOG(DEBUG) << "Got input to Bind operation." << endl;
  IdTable idTable{getExecutionContext()->getAllocator()};

  idTable.setNumColumns(getResultWidth());

  // Make a deep copy of the local vocab from `subRes` and then add to it (in
  // case BIND adds a new word or words).
  //
  // TODO: In most BIND operations, nothing is added to the local vocabulary, so
  // it would be more efficient to first share the pointer here (like with
  // `shareLocalVocabFrom`) and only copy it when a new word is about to be
  // added. Same for GROUP BY.
  auto localVocab = subRes->getCopyOfLocalVocab();

  size_t inwidth = subRes->idTable().numColumns();
  size_t outwidth = getResultWidth();

  CALL_FIXED_SIZE((std::array{inwidth, outwidth}), &Bind::computeExpressionBind,
                  this, &idTable, &localVocab, *subRes,
                  _bind._expression.getPimpl());

  LOG(DEBUG) << "BIND result computation done." << endl;
  return {std::move(idTable), resultSortedOn(), std::move(localVocab)};
}

// _____________________________________________________________________________
template <size_t IN_WIDTH, size_t OUT_WIDTH>
void Bind::computeExpressionBind(
    IdTable* outputIdTable, LocalVocab* outputLocalVocab,
    const ResultTable& inputResultTable,
    sparqlExpression::SparqlExpression* expression) const {
  sparqlExpression::EvaluationContext evaluationContext(
      *getExecutionContext(), _subtree->getVariableColumns(),
      inputResultTable.idTable(), getExecutionContext()->getAllocator(),
      inputResultTable.localVocab());

  sparqlExpression::ExpressionResult expressionResult =
      expression->evaluate(&evaluationContext);

  const auto input = inputResultTable.idTable().asStaticView<IN_WIDTH>();
  auto output = std::move(*outputIdTable).toStatic<OUT_WIDTH>();

  // first initialize the first columns (they remain identical)
  const auto inSize = input.size();
  output.reserve(inSize);
  const auto inCols = input.numColumns();
  // copy the input to the first numColumns;
  for (size_t i = 0; i < inSize; ++i) {
    output.emplace_back();
    for (size_t j = 0; j < inCols; ++j) {
      output(i, j) = input(i, j);
    }
  }

  auto visitor = [&]<sparqlExpression::SingleExpressionResult T>(
                     T&& singleResult) mutable {
    constexpr static bool isVariable = std::is_same_v<T, ::Variable>;
    constexpr static bool isStrongId = std::is_same_v<T, Id>;

    if constexpr (isVariable) {
      auto column =
          getInternallyVisibleVariableColumns().at(singleResult).columnIndex_;
      for (size_t i = 0; i < inSize; ++i) {
        output(i, inCols) = output(i, column);
      }
    } else if constexpr (isStrongId) {
      for (size_t i = 0; i < inSize; ++i) {
        output(i, inCols) = singleResult;
      }
    } else {
      bool isConstant = sparqlExpression::isConstantResult<T>;

      auto resultGenerator = sparqlExpression::detail::makeGenerator(
          std::forward<T>(singleResult), inSize, &evaluationContext);

      if (isConstant) {
        auto it = resultGenerator.begin();
        if (it != resultGenerator.end()) {
          Id constantId =
              sparqlExpression::detail::constantExpressionResultToId(
                  std::move(*it), *outputLocalVocab);
          for (size_t i = 0; i < inSize; ++i) {
            output(i, inCols) = constantId;
          }
        }
      } else {
        size_t i = 0;
        // We deliberately move the values from the generator.
        for (auto& resultValue : resultGenerator) {
          output(i, inCols) =
              sparqlExpression::detail::constantExpressionResultToId(
                  std::move(resultValue), *outputLocalVocab);
          i++;
        }
      }
    }
  };

  std::visit(visitor, std::move(expressionResult));

  *outputIdTable = std::move(output).toDynamic();
}
