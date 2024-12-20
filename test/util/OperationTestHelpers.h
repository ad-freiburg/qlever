//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_OPERATIONTESTHELPERS_H
#define QLEVER_OPERATIONTESTHELPERS_H

#include <chrono>
#include <vector>

#include "engine/Operation.h"
#include "engine/QueryExecutionTree.h"

using namespace std::chrono_literals;

class StallForeverOperation : public Operation {
  std::vector<QueryExecutionTree*> getChildren() override { return {}; }
  string getCacheKeyImpl() const override { return "StallForeverOperation"; }
  string getDescriptor() const override {
    return "StallForeverOperationDescriptor";
  }
  size_t getResultWidth() const override { return 0; }
  size_t getCostEstimate() override { return 0; }
  uint64_t getSizeEstimateBeforeLimit() override { return 0; }
  float getMultiplicity([[maybe_unused]] size_t) override { return 0; }
  bool knownEmptyResult() override { return false; }
  vector<ColumnIndex> resultSortedOn() const override { return {}; }
  VariableToColumnMap computeVariableToColumnMap() const override { return {}; }

 public:
  using Operation::Operation;
  // Do-nothing operation that runs for 100ms without computing anything, but
  // which can be cancelled.
  ProtoResult computeResult([[maybe_unused]] bool requestLaziness) override {
    auto end = std::chrono::steady_clock::now() + 100ms;
    while (std::chrono::steady_clock::now() < end) {
      checkCancellation();
    }
    throw std::runtime_error{"Loop was not interrupted for 100ms, aborting"};
  }

  // Provide public view of remainingTime for tests
  std::chrono::milliseconds publicRemainingTime() const {
    return remainingTime();
  }
};
// _____________________________________________________________________________

// Dummy parent to test recursive application of a function
class ShallowParentOperation : public Operation {
  std::shared_ptr<QueryExecutionTree> child_;

  explicit ShallowParentOperation(std::shared_ptr<QueryExecutionTree> child)
      : child_{std::move(child)} {}
  string getCacheKeyImpl() const override { return "ParentOperation"; }
  string getDescriptor() const override { return "ParentOperationDescriptor"; }
  size_t getResultWidth() const override { return 0; }
  size_t getCostEstimate() override { return 0; }
  uint64_t getSizeEstimateBeforeLimit() override { return 0; }
  float getMultiplicity([[maybe_unused]] size_t) override { return 0; }
  bool knownEmptyResult() override { return false; }
  vector<ColumnIndex> resultSortedOn() const override { return {}; }
  VariableToColumnMap computeVariableToColumnMap() const override { return {}; }

 public:
  template <typename ChildOperation, typename... Args>
  static ShallowParentOperation of(QueryExecutionContext* qec, Args&&... args) {
    return ShallowParentOperation{
        ad_utility::makeExecutionTree<ChildOperation>(qec, args...)};
  }

  std::vector<QueryExecutionTree*> getChildren() override {
    return {child_.get()};
  }

  ProtoResult computeResult([[maybe_unused]] bool requestLaziness) override {
    auto childResult = child_->getResult();
    return {childResult->idTable().clone(), resultSortedOn(),
            childResult->getSharedLocalVocab()};
  }

  // Provide public view of remainingTime for tests
  std::chrono::milliseconds publicRemainingTime() const {
    return remainingTime();
  }
};

// Operation that will throw on `computeResult` for testing.
class AlwaysFailOperation : public Operation {
  std::optional<Variable> variable_ = std::nullopt;

  std::vector<QueryExecutionTree*> getChildren() override { return {}; }
  string getCacheKeyImpl() const override {
    // Because this operation always fails, it should never be cached.
    return "AlwaysFailOperationCacheKey";
  }
  string getDescriptor() const override {
    return "AlwaysFailOperationDescriptor";
  }
  size_t getResultWidth() const override { return 1; }
  size_t getCostEstimate() override { return 0; }
  uint64_t getSizeEstimateBeforeLimit() override { return 0; }
  float getMultiplicity([[maybe_unused]] size_t) override { return 0; }
  bool knownEmptyResult() override { return false; }
  vector<ColumnIndex> resultSortedOn() const override { return {0}; }
  VariableToColumnMap computeVariableToColumnMap() const override {
    if (!variable_.has_value()) {
      return {};
    }
    return {{variable_.value(),
             ColumnIndexAndTypeInfo{
                 0, ColumnIndexAndTypeInfo::UndefStatus::AlwaysDefined}}};
  }

 public:
  using Operation::Operation;
  AlwaysFailOperation(QueryExecutionContext* qec, Variable variable)
      : Operation{qec}, variable_{std::move(variable)} {}
  ProtoResult computeResult(bool requestLaziness) override {
    if (!requestLaziness) {
      throw std::runtime_error{"AlwaysFailOperation"};
    }
    return {[]() -> Result::Generator {
              throw std::runtime_error{"AlwaysFailOperation"};
              // Required so that the exception only occurs within the generator
              co_return;
            }(),
            resultSortedOn()};
  }
};

// Lazy operation that will yield a result with a custom generator you can
// provide via the constructor.
class CustomGeneratorOperation : public Operation {
  Result::Generator generator_;
  std::vector<QueryExecutionTree*> getChildren() override { return {}; }
  string getCacheKeyImpl() const override { AD_FAIL(); }
  string getDescriptor() const override {
    return "CustomGeneratorOperationDescriptor";
  }
  size_t getResultWidth() const override { return 0; }
  size_t getCostEstimate() override { return 0; }
  uint64_t getSizeEstimateBeforeLimit() override { return 0; }
  float getMultiplicity([[maybe_unused]] size_t) override { return 0; }
  bool knownEmptyResult() override { return false; }
  vector<ColumnIndex> resultSortedOn() const override { return {}; }
  VariableToColumnMap computeVariableToColumnMap() const override { return {}; }

 public:
  CustomGeneratorOperation(QueryExecutionContext* context,
                           Result::Generator generator)
      : Operation{context}, generator_{std::move(generator)} {}
  ProtoResult computeResult(bool requestLaziness) override {
    AD_CONTRACT_CHECK(requestLaziness);
    return {std::move(generator_), resultSortedOn()};
  }
};

#endif  // QLEVER_OPERATIONTESTHELPERS_H
