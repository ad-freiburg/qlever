// Copyright 2019 - 2022 The QLever Authors, in particular:
//
// 2019 - 2022 Florian Kramer <kramerf@cs.uni-freiburg.de>, UFR
// 2019 - 2022 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2019 - 2022 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_VALUES_H
#define QLEVER_SRC_ENGINE_VALUES_H

#include "engine/Operation.h"
#include "parser/ParsedQuery.h"

class TripleComponent;

class Values : virtual public Operation {
  using SparqlValues = parsedQuery::SparqlValues;

 private:
  std::vector<float> multiplicities_;
  SparqlValues parsedValues_;

 protected:
  // Accessors for the parsed values.
  SparqlValues& parsedValues() { return parsedValues_; }

 public:
  // Const access to the parsed values (e.g. for the spatial join's geo block
  // prefilter, which inspects a single-row `VALUES` with a fixed geometry).
  const SparqlValues& parsedValues() const { return parsedValues_; }

 protected:
 public:
  // Create operation from parsed values. This calls `sanitizeValues`.
  // and values.
  Values(QueryExecutionContext* qec, SparqlValues parsedValues);

 protected:
  std::string getCacheKeyImpl() const override;

 public:
  virtual std::string getDescriptor() const override;

  virtual size_t getResultWidth() const override;

  virtual std::vector<ColumnIndex> resultSortedOn() const override;

  virtual bool knownEmptyResult() override {
    return parsedValues_._variables.empty() || parsedValues_._values.empty();
  }

  virtual float getMultiplicity(size_t col) override;

 private:
  uint64_t getSizeEstimateBeforeLimit() override;

 public:
  virtual size_t getCostEstimate() override;

  std::vector<QueryExecutionTree*> getChildren() override { return {}; }

 public:
  // These two are also used by class `Service`, hence public.
  virtual Result computeResult([[maybe_unused]] bool requestLaziness) override;

  VariableToColumnMap computeVariableToColumnMap() const override;

 private:
  [[nodiscard]] bool isDeterministicImpl() const override { return true; }

  std::unique_ptr<Operation> cloneImpl() const override;

  // Compute the per-column multiplicity of the parsed values.
  void computeMultiplicities();

  // Write `parsedValues_` to the given result object.
  //
  // NOTE: this moves the values out of `parsedValues_` (to save a string copy
  // for those values that end up in the local vocabulary).
  template <size_t I>
  void writeValues(IdTable* idTablePtr, LocalVocab* localVocab);
};

// Create a one-row `VALUES` clause that binds `value` to `variable`.
std::shared_ptr<QueryExecutionTree> makeValuesForSingleValue(
    QueryExecutionContext* qec, Variable variable, TripleComponent value);

#endif  // QLEVER_SRC_ENGINE_VALUES_H
