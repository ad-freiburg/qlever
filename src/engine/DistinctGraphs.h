#ifndef QLEVER_SRC_ENGINE_DISTINCTGRAPHS_H
#define QLEVER_SRC_ENGINE_DISTINCTGRAPHS_H 

#include "engine/Operation.h"
#include "engine/QueryExecutionTree.h"
#include "rdfTypes/Variable.h"

// An Operation that produces a single-column result containing all distinct
// named graph IRIs present in the index. It is used to evaluate SPARQL
// patterns of the form `GRAPH ?g { }` where `?g` is an unbound variable that
// is not already bound by the inner pattern.
//
// The implementation reads graph IDs directly from the block metadata of the
// SPO permutation, falling back to a full block decompression only when a
// block may contain a previously unseen graph ID. This avoids a full table
// scan in the common case.
//
// Example SPARQL query that uses this class:
//
//   SELECT ?g WHERE { GRAPH ?g { } } -> gives all distinct graphs in the dataset
//   SELECT ?g WHERE { GRAPH ?g { VALUES ?x { <something> } } } -> cartesian product 
//      of all distinct graphs with all values of ?x
//
// The query planner injects a `DistinctGraphs` operation whenever it detects
// a `GRAPH ?g { <inner> }` pattern where `?g` is not already bound by
// `<inner>`.
class DistinctGraphs : public Operation {
 public:
  explicit DistinctGraphs(QueryExecutionContext* qec, Variable graphVariable);

  std::vector<QueryExecutionTree*> getChildren() override { return {}; }

  [[nodiscard]] std::string getDescriptor() const override { return "Distinct Graphs"; }

  [[nodiscard]] size_t getResultWidth() const override { return 1; }

  size_t getCostEstimate() override { return 1; }

  float getMultiplicity(size_t col) override { return 1.0f * col; }

  bool knownEmptyResult() override { return false; }

  std::unique_ptr<Operation> cloneImpl() const override;

 protected:
  [[nodiscard]] std::vector<ColumnIndex> resultSortedOn() const override { return {}; }

 private:
  [[nodiscard]] std::string getCacheKeyImpl() const override { return "DistinctGraphs"; }

  uint64_t getSizeEstimateBeforeLimit() override { return 1; }

  Result computeResult(bool requestLaziness) override;

  [[nodiscard]] VariableToColumnMap computeVariableToColumnMap() const override;

  Variable graphVariable_;
};

#endif
