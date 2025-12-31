---
name: cpp-patterns
description: Apply C++ design patterns, understand QLever architecture, implement template metaprogramming, and write idiomatic modern C++. Use when designing new features, refactoring code, or understanding architectural patterns.
---

# C++ Patterns & Architecture Skill for QLever

Comprehensive guide to design patterns, C++ best practices, and architectural patterns used throughout the QLever codebase.

## QLever Core Design Patterns

### 1. Strategy Pattern - Operation Hierarchy

All query execution operations inherit from `Operation` base class:

```cpp
// Base class (Strategy interface)
class Operation {
public:
  virtual ~Operation() = default;
  virtual IdTable execute(const QueryExecutionContext& ctx) = 0;
  virtual std::vector<ColumnIndex> getResultColumns() const = 0;
  virtual size_t estimateCost() const = 0;
  // ... other virtual methods
};

// Concrete strategies
class Join : public Operation { };
class Filter : public Operation { };
class GroupBy : public Operation { };
class OrderBy : public Operation { };
```

**When to use**:
- Multiple execution strategies for same operation
- Runtime algorithm selection
- New operation type additions

**In QLever**:
- ~30+ operation types in `src/engine/`
- Each implements `execute()` method differently
- Query planner selects best strategy

### 2. Cost-Based Query Optimization

The `QueryPlanner` uses cost estimation to select optimal execution plans:

```cpp
class QueryPlanner {
  // Estimate cost of different execution orders
  size_t estimateCost(const Join& join) const;

  // Create optimal execution plan
  std::unique_ptr<Operation> createOptimalPlan(
    const ParsedQuery& query);
};
```

**Key concepts**:
- Each operation estimates its execution cost
- Planner explores alternative plans
- Selects plan with minimum total cost
- Dynamic programming for join ordering

### 3. Pimpl Pattern (Pointer to Implementation)

Hide implementation complexity, reduce compilation dependencies:

```cpp
// Public header - minimal dependencies
class Index {
public:
  std::vector<Triple> scan(const IndexScan& query) const;

private:
  std::unique_ptr<IndexImpl> impl_;  // Opaque implementation
};

// Implementation header - can change freely
class IndexImpl {
  void buildPermutation(Permutation::Type type);
  CompressedRelation relation_;
  // ... complex internals
};
```

**Benefits**:
- Implementation changes don't require recompilation of clients
- Faster incremental builds
- API stability
- Hide internal complexity

**Used in QLever**:
- `Index` / `IndexImpl`
- `Vocabulary` / implementation details
- `IdTable` / internal storage

### 4. Template Metaprogramming for Performance

Compile-time specialization for different result table widths:

```cpp
// Generic template
template<size_t WIDTH>
class IdTableStatic {
  std::array<std::vector<ValueId>, WIDTH> columns_;

  void addRow(const std::array<ValueId, WIDTH>& row) {
    for (size_t i = 0; i < WIDTH; ++i) {
      columns_[i].push_back(row[i]);
    }
  }
};

// Specializations generated at compile-time
IdTableStatic<1> table1;  // Single column
IdTableStatic<3> table3;  // Three columns
IdTableStatic<16> table16; // Many columns

// Dynamic wrapper
class IdTable {
  std::variant<IdTableStatic<1>, IdTableStatic<2>, ...> impl_;
};
```

**Advantages**:
- Zero-runtime overhead for fixed widths
- Compile-time type safety
- Vectorization opportunities
- Cache efficiency

### 5. RAII (Resource Acquisition Is Initialization)

Ensure resources cleaned up automatically:

```cpp
// Lock released automatically when scope exits
{
  std::lock_guard<std::mutex> lock(mutex_);
  // Critical section
  // ~lock_guard automatically releases lock
}

// Memory freed automatically
{
  auto operation = std::make_unique<Join>(...);
  operation->execute();
  // ~unique_ptr automatically deletes operation
}

// Query context cleaned up
{
  QueryExecutionContext ctx(...);
  executeQuery(ctx);
  // ~QueryExecutionContext releases resources
}
```

**Key principle**: Resources tied to object lifetime

### 6. Lazy Evaluation & Streaming

Avoid materializing all intermediate results:

```cpp
// LazyGroupByRange - deferred computation
class LazyGroupByRange {
  // Doesn't compute groups until iteration
  iterator begin() { /* start lazy computation */ }
  iterator end() { /* signal end of lazy iteration */ }
};

// BatchedPipeline - process data in streaming fashion
class BatchedPipeline {
  void processBatch(const IdTable& batch) {
    // Transform batch without materializing entire table
  }
};
```

**Performance benefits**:
- Reduced memory usage
- Better cache locality
- Partial result computation
- Early termination possible

### 7. Visitor Pattern for Recursion

Process tree-structured data (SPARQL AST, expression trees):

```cpp
// Visitor interface
class ExpressionVisitor {
  virtual void visit(const Literal& lit) = 0;
  virtual void visit(const Variable& var) = 0;
  virtual void visit(const FunctionCall& call) = 0;
};

// Concrete visitor - evaluates expressions
class ExpressionEvaluator : public ExpressionVisitor {
  void visit(const Literal& lit) override {
    result_ = lit.value();
  }

  void visit(const FunctionCall& call) override {
    // Evaluate function with visited arguments
  }
};

// Usage
ExpressionEvaluator evaluator;
expression->accept(evaluator);
auto value = evaluator.result();
```

**In QLever**:
- ANTLR-generated parser uses visitor pattern
- Expression tree evaluation
- Graph pattern traversal

### 8. Concurrency Patterns

Thread-safe operations with minimal synchronization overhead:

```cpp
// Synchronized wrapper template
template<typename T>
class Synchronized {
  std::mutex mutex_;
  T data_;

public:
  auto withLock(auto callback) {
    std::lock_guard lock(mutex_);
    return callback(data_);
  }
};

// Usage
Synchronized<std::vector<Task>> tasks;
tasks.withLock([](auto& t) { t.push_back(task); });

// Cancellation handle
std::shared_ptr<CancellationHandle> cancel_handle;
if (cancel_handle->isCancelled()) {
  return;  // Abort operation
}

// Lock-free caching
ConcurrentCache<Key, Value> cache;
```

**Patterns**:
- `Synchronized<T>` for mutually exclusive access
- `SharedCancellationHandle` for query cancellation
- `ConcurrentCache` for lock-free lookups
- Thread-local storage for thread-specific data

### 9. Builder Pattern for Complex Construction

Fluent API for building complex objects:

```cpp
// ConfigManager - fluent configuration
auto config = ConfigManager()
  .withMemoryLimit(4_GB)
  .withThreadCount(8)
  .withCacheSize(1_GB)
  .build();

// ParsedQuery - building query from components
auto query = ParsedQuery::Builder()
  .selectVariables({var1, var2})
  .addGraphPattern(pattern1)
  .addGraphPattern(pattern2)
  .addFilter(filter)
  .setLimit(10)
  .build();
```

**When to use**:
- Complex multi-step object creation
- Optional parameters
- Readable configuration
- Validation during construction

### 10. Memory Management Patterns

Constrained execution model with allocation tracking:

```cpp
// Allocator with limit prevents OOM
class AllocatorWithLimit {
  size_t allocated_;
  size_t limit_;

  void* allocate(size_t size) {
    if (allocated_ + size > limit_) {
      throw std::bad_alloc("Memory limit exceeded");
    }
    allocated_ += size;
    return malloc(size);
  }
};

// Operations track their allocations
class Join {
  void execute(const QueryExecutionContext& ctx) {
    auto allocator = ctx.getAllocatorWithLimit();
    // All allocations checked against limit
  }
};

// Caching memory resource for short-lived allocations
CachingMemoryResource cache;
std::pmr::vector<Item> items(&cache);
// Memory reused when cache is reset
```

**Principles**:
- Every operation respects memory limits
- RAII ensures cleanup
- Allocations tracked for observability
- Caching for temporary allocations

## C++20 Modern Features Used

### Concepts for Type Safety

```cpp
// Define requirements for types
template<typename T>
concept Hashable = requires(T t) {
  { std::hash<T>{}(t) } -> std::convertible_to<size_t>;
};

// Constrain templates
template<Hashable T>
class HashSet {
  // Only accepts hashable types
};
```

### Ranges for Data Processing

```cpp
// Composable data transformation
auto results = values
  | std::views::filter([](int x) { return x > 5; })
  | std::views::transform([](int x) { return x * 2; })
  | std::ranges::to<std::vector>();
```

### Coroutines for Lazy Evaluation

```cpp
// Generator pattern with coroutines
std::generator<int> generateSequence(int start, int end) {
  for (int i = start; i < end; ++i) {
    co_yield i;  // Suspend and resume on demand
  }
}
```

### Three-way Comparison (Spaceship Operator)

```cpp
struct Record {
  int id;
  std::string name;

  auto operator<=>(const Record&) const = default;
};

// Enables all comparison operators automatically
```

## File Organization Conventions

### Header Files (.h)

```cpp
#ifndef ENGINE_JOIN_H
#define ENGINE_JOIN_H

#include "Operation.h"
#include "IdTable.h"

// Forward declarations reduce dependencies
class QueryExecutionContext;

namespace ad_semelang::engine {

/// Join operation implementation
/// \details Performs inner join between two operations
class Join : public Operation {
public:
  Join(/* parameters */);
  IdTable execute(const QueryExecutionContext& ctx) override;

private:
  // Member variables
};

}  // namespace ad_semelang::engine

#endif  // ENGINE_JOIN_H
```

**Guidelines**:
- Include guards: `NAMESPACE_FILENAME_H` format
- Forward declarations instead of includes
- Group includes: standard, external, local
- One class per header file
- Public interfaces clearly marked

### Source Files (.cpp)

```cpp
#include "engine/Join.h"
#include "engine/Operation.h"
#include "IdTable.h"

namespace ad_semelang::engine {

Join::Join(/* parameters */)
  : Operation(...) {
}

IdTable Join::execute(const QueryExecutionContext& ctx) {
  // Implementation
}

}  // namespace ad_semelang::engine
```

## Testing Best Practices

### Test File Structure

```cpp
#include <gtest/gtest.h>
#include "engine/Join.h"

namespace ad_semelang::engine {

class JoinTest : public ::testing::Test {
protected:
  void SetUp() override {
    // Common setup
  }

  void TearDown() override {
    // Common cleanup
  }

  // Helper functions
  IdTable createTestTable(size_t rows, size_t cols) {
    // Implementation
  }
};

// Test basic functionality
TEST_F(JoinTest, BasicInnerJoin) {
  auto left = createTestTable(10, 2);
  auto right = createTestTable(20, 2);

  Join join(left, right, join_columns);
  auto result = join.execute(context_);

  EXPECT_EQ(result.size(), 50);  // Expected rows
}

// Test edge cases
TEST_F(JoinTest, EmptyTable) {
  auto left = createTestTable(0, 2);
  auto right = createTestTable(10, 2);

  Join join(left, right, join_columns);
  auto result = join.execute(context_);

  EXPECT_EQ(result.size(), 0);
}

}  // namespace ad_semelang::engine
```

## Common Patterns in QLever Code

### Null Safety with Optional

```cpp
// Use std::optional instead of pointers for optional values
std::optional<IdTable> maybeJoin(const Operation& left,
                                  const Operation& right) {
  if (/* incompatible */) {
    return std::nullopt;  // No value
  }
  return IdTable(/* result */);
}

// Usage
if (auto result = maybeJoin(left, right)) {
  // Use *result
}
```

### Error Handling with Expected/Result

```cpp
// For operations that may fail
class ParseResult {
  bool success_;
  std::string error_;
  ParsedQuery query_;
};

// Or using exceptions
try {
  auto query = parseQuery(sparql);
} catch (const ParseException& e) {
  LOG(ERROR) << "Parse failed: " << e.what();
}
```

### Efficient String Handling

```cpp
// Use std::string_view for non-owning references
void processString(std::string_view str) {
  // No allocation, just view of existing string
}

// Use move semantics for large objects
IdTable createLargeTable() {
  IdTable table(1000000, 10);
  // ... populate table
  return table;  // Move semantics used automatically
}
```

## Optimization Strategies

### Avoid Virtual Function Calls in Hot Paths

```cpp
// In query planning, use virtual calls (cost is acceptable)
auto cost = operation->estimateCost();

// In execution inner loop, use concrete types
// Avoid: operation->processRow(row);
// Better: static_cast<Join*>(op)->processRow(row);
```

### Use Algorithms Instead of Manual Loops

```cpp
// Avoid
std::vector<int> filtered;
for (const auto& item : items) {
  if (predicate(item)) {
    filtered.push_back(item);
  }
}

// Better - enables compiler optimizations
auto filtered = items
  | std::views::filter(predicate)
  | std::ranges::to<std::vector>();
```

### Cache Efficiency Patterns

```cpp
// Process data in cache-friendly order
for (size_t chunk = 0; chunk < data.size(); chunk += CACHE_LINE) {
  // Process CACHE_LINE elements before memory miss
  processChunk(data, chunk, CACHE_LINE);
}
```

## Architecture Guidelines

### Dependency Direction

Always follow this direction:

```
Core Types (rdfTypes, util)
    ↑
Index Layer (index)
    ↑
Query Engine (engine)
    ↑
Server/API (libqlever)
```

**Never create backward dependencies** - Engine should not depend on libqlever

### Abstraction Levels

1. **Type Level** (`src/rdfTypes/`) - RDF, SPARQL concepts
2. **Storage Level** (`src/index/`) - Triple storage, indexing
3. **Execution Level** (`src/engine/`) - Query planning, execution
4. **API Level** (`src/libqlever/`) - User-facing interface

## When to Apply Patterns

| Situation | Pattern |
|-----------|---------|
| Multiple algorithms for same task | Strategy |
| Recursive data structure | Visitor |
| Complex multi-step construction | Builder |
| Hide implementation details | Pimpl |
| Performance-critical fixed-width data | Template specialization |
| Resource management | RAII |
| Avoid materializing all results | Lazy evaluation |
| Thread-safe shared state | Synchronized<T> |
| Optional values | std::optional |
| Estimated costs | Cost-based optimization |
