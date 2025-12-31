# CLAUDE.md - QLever Codebase Guide for AI Assistants

## Overview

**QLever** is a high-performance graph database implementing RDF (Resource Description Framework) and SPARQL standards. This document provides comprehensive guidance for AI assistants contributing to this codebase.

**Key Facts:**
- **Purpose**: Efficiently load and query very large RDF datasets (billions+ triples)
- **Language**: C++20 (with C++17 compatibility fallback)
- **Build System**: CMake 3.27+ with Ninja/Make
- **Testing**: Google Test framework (~289 test files)
- **License**: Apache 2.0
- **Active Development**: Yes, with regular feature additions and optimizations

---

## Codebase Architecture

### Directory Structure Overview

```
/src/                          # Main source code (199 .cpp, 382 .h files)
├── engine/                    # Query execution engine (CORE COMPONENT)
│   ├── sparqlExpressions/     # SPARQL expression evaluation
│   ├── idTable/               # Memory-efficient result tables
│   └── [30+ operation types]  # Join, Filter, Sort, GroupBy, etc.
├── index/                     # RDF indexing and storage
│   ├── vocabulary/            # ID mapping for IRIs/literals
│   ├── textIndex/             # Full-text search support
│   └── spatial/               # Spatial/geographic queries
├── parser/                    # SPARQL query parsing (ANTLR-based)
├── libqlever/                 # C++ embedding API
├── rdfTypes/                  # Core RDF data types (IRI, Literal, Variable)
├── util/                      # 88 utility headers (concurrency, caching, compression)
├── global/                    # Global constants and types
└── backports/                 # C++17/C++20 compatibility layer

/test/                         # Test suite (organized by module)
/benchmark/                    # Performance benchmarks
/.github/workflows/            # CI/CD pipeline (GitHub Actions)
/examples/                     # Sample datasets and configs
```

### Core Component Responsibilities

#### 1. **Engine** (`src/engine/`) - Query Execution
The largest component responsible for executing SPARQL queries.

**Key Classes:**
- `Operation` - Abstract base for all execution strategies
- `QueryExecutionTree` - Represents execution plan as tree of operations
- `QueryPlanner` - Cost-based optimizer creating execution plans
- `IdTable` / `IdTableStatic` - Memory-efficient result containers
- `QueryExecutionContext` - Manages execution state and resources

**Operation Types** (~30+):
- Scanning: `IndexScan`, `Load`
- Joining: `Join`, `MultiColumnJoin`, `OptionalJoin`, `ExistsJoin`, `CartesianProductJoin`, `SpatialJoin`
- Filtering: `Filter`, `Distinct`
- Aggregation: `GroupBy`, `GroupByImpl`
- Output: `OrderBy`, `Limit`, `Offset`, `Describe`
- Binding: `Bind`, `ExecuteUpdate`
- Neutral: `NeutralElementOperation`

**Design Pattern:** Strategy pattern for interchangeable execution strategies

#### 2. **Index** (`src/index/`) - Data Storage & Retrieval
Manages RDF data indexing and efficient storage.

**Key Components:**
- `Index` / `IndexImpl` - Triple storage wrapper
- `CompressedRelation` - Space-efficient storage with multiple permutations (PSO, POS, OSP, etc.)
- `Vocabulary` / `VocabularyType` - Maps IRIs/literals to internal IDs
- `Permutation` - Manages different triple orderings
- `TextIndex` - Full-text search with BM25 scoring
- `SpatialIndex` - Geographic data (S2 geometry integration)

**Design Pattern:** Pimpl pattern (Index wraps IndexImpl) to hide implementation complexity

#### 3. **Parser** (`src/parser/`) - Query Parsing
Converts SPARQL text into structured data.

**Key Classes:**
- `ParsedQuery` - Main result of parsing
- ANTLR-generated parser (in `sparqlParser/generated/`)
- `GraphPattern`, `GraphPatternOperation`, `LiteralOrIri`, `Variable`
- `PathQuery` - Property path queries
- `MaterializedViewQuery` - Materialized view definitions

**Design Pattern:** Visitor pattern for ANTLR parse tree traversal

#### 4. **RDFTypes** (`src/rdfTypes/`) - Core Data Representations
Fundamental RDF data types.

**Key Classes:**
- `Iri` - RDF IRIs/URIs
- `Literal` - RDF literals with language tags and datatypes
- `Variable` - SPARQL variables
- `GeoPoint` / `GeometryInfo` - Geographic data

#### 5. **Util** (`src/util/`) - Utilities Library
88 headers providing essential functionality.

**Major Categories:**
- **Memory**: `AllocatorWithLimit`, `CachingMemoryResource`, allocation tracking
- **Concurrency**: `CancellationHandle`, `Synchronized<T>`, `ConcurrentCache`
- **Data Structures**: `Cache`, `HashSet`, `BufferedVector`, `MmapVector`
- **Compression**: Zstd integration, FSST compression
- **Configuration**: `ConfigManager`, `RuntimeParameters`, `MemorySize`
- **Text/Strings**: `Date`, string utilities
- **I/O**: HTTP utilities, async I/O helpers
- **Algorithms**: Parallel sort, chunked iteration, streaming pipelines

---

## Technology Stack

### Language & Compiler Requirements
- **C++**: C++20 (mandatory for new code)
- **C++17 Fallback**: Via backports module (`src/backports/`)
- **Compilers**:
  - GCC 11.0+
  - Clang/LLVM 16.0+
- **Build**: CMake 3.27+, Ninja (preferred), or Make

### Critical Dependencies
- **Boost 1.81**: `iostreams`, `program_options`, `url`, `asio`
- **ANTLR 4.13.12**: SPARQL parser generation
- **ICU 60+**: Unicode and collation support
- **OpenSSL 3.1.1**: TLS support
- **Zstandard 1.5.5**: Compression
- **nlohmann/json 3.12.0**: JSON serialization
- **Abseil**: Hash maps and utility libraries
- **Google Test/Mock**: Unit testing
- **S2 Geometry**: Spherical geometry for spatial queries
- **Optional**: jemalloc (high-performance allocator), Google Perftools (profiling)

### Build Output Locations
- **Libraries**: `build/lib/`
- **Executables**: `build/`
- **Main Binaries**:
  - `ServerMain` - SPARQL query server
  - `IndexBuilderMain` - Build indexes from RDF data
  - `VocabularyMergerMain` - Merge multiple vocabularies

---

## Development Workflow

### Git Workflow

**Branch Naming Convention:**
```
claude/<feature-description>-<SESSION_ID>
```
Example: `claude/add-claude-documentation-LP2W5`

**Basic Workflow:**
```bash
# Ensure you're on the correct branch
git checkout claude/add-claude-documentation-LP2W5

# Make changes
git add <files>
git commit -m "Descriptive commit message"

# Push with `-u` flag for new branches
git push -u origin claude/add-claude-documentation-LP2W5
```

**Important Git Rules:**
- Always push to feature branches, never to main/master
- Use `-u origin <branch-name>` when pushing new branches
- Retry pushes with exponential backoff (2s, 4s, 8s, 16s) if network fails
- Fetch specific branches: `git fetch origin <branch-name>`

### Building & Testing

**Setup:**
```bash
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release -GNinja ..
cmake --build .
```

**Running Tests:**
```bash
# All tests
ctest --output-on-failure

# Specific test
ctest -R TestName --output-on-failure

# With verbose output
ctest --verbose --output-on-failure
```

**Important Build Flags:**
- `-DCMAKE_BUILD_TYPE=Release` - Optimize for performance
- `-DCMAKE_BUILD_TYPE=Debug` - Debug symbols and checks
- `-DASAN` - AddressSanitizer for memory errors
- `-DUSE_PARALLEL=true` - Parallel compilation
- `-DUSE_PRECOMPILED_HEADERS=true` - Speed up builds
- `-DLOGLEVEL=INFO` - Set log level

### Code Quality Standards

**Formatting:**
- **Tool**: clang-format 16.0.6
- **Style**: Google C++ style
- **Line Length**: 100 characters
- **Pre-commit Hooks**: Automatically applied
- **Setup**: `pre-commit install` after cloning

**Spell Checking:**
- **Tool**: codespell v2.2.6
- **Integration**: Pre-commit hook
- **Configuration**: `.codespellrc`

**Static Analysis:**
- **SonarCloud**: Cloud-based quality analysis
- **AddressSanitizer**: Memory error detection
- **Undefined Behavior Sanitizer**: UB detection

**CI/CD Pipeline** (GitHub Actions):
- Native builds (multiple OS)
- Docker image publishing
- Code coverage analysis
- SonarCloud integration
- SPARQL conformance testing
- Format verification
- C++17 compatibility testing

### Commit Message Guidelines

**Structure:**
```
<Type>: <Brief description (50 chars max)>

<Detailed explanation (optional)>
```

**Types** (from recent commits):
- `feat:` - New feature (e.g., "materialized views")
- `fix:` - Bug fix (e.g., "blank node handling")
- `optimize:` - Performance improvement
- `refactor:` - Code restructuring
- `docs:` - Documentation changes
- `test:` - Test additions/improvements
- `chore:` - Build system, dependencies

**Examples from codebase:**
```
feat: First functional version of materialized views
fix: Handle blank nodes and local vocab entries correctly
optimize: Check whether to use internal permutation in query planner
```

---

## Key Design Patterns & Conventions

### 1. Strategy Pattern (Operation Hierarchy)
All query operations inherit from `Operation` base class:
- Enables interchangeable execution strategies
- Composed into execution tree via `QueryExecutionTree`
- Example: Different join strategies (MultiColumnJoin, CartesianProductJoin, etc.)

**When Modifying:**
- Add new operation types in `src/engine/`
- Implement `execute()` and virtual methods from `Operation`
- Register in `QueryPlanner` for cost estimation

### 2. Cost-Based Query Optimization
`QueryPlanner` implements:
- Cost estimation for different execution plans
- Join order optimization
- Connected component detection via TripleGraph
- Filter push-down
- **Convention**: Always estimate execution cost; planner selects optimal plan

### 3. Pimpl Pattern (Pointer to Implementation)
Reduces compilation dependencies:
- `Index` wraps `IndexImpl`
- `Vocabulary` wraps implementation details
- **Convention**: Use for complex interfaces to accelerate incremental builds

### 4. Template Metaprogramming
Heavy use of C++20 templates:
- `IdTableStatic<WIDTH>` - Fixed-width result tables (compile-time specialization)
- `IdTable` - Dynamic-width wrapper
- `Synchronized<T>` - Thread-safe wrapper template
- **Convention**: Use concepts from `src/backports/concepts.h` for type safety

### 5. RAII (Resource Acquisition Is Initialization)
Extensive use of destructors for cleanup:
- **Convention**: Resources (memory, files, connections) managed via constructors/destructors
- Prefer unique_ptr over raw pointers
- Use std::make_unique for allocation

### 6. Lazy Evaluation & Streaming
Performance optimization pattern:
- `LazyGroupByRange` - Deferred group-by computation
- `BatchedPipeline` - Streaming result processing
- **Convention**: Avoid materializing all intermediate results; use generators when possible

### 7. Visitor Pattern
Used in parsing and expression evaluation:
- ANTLR visitor pattern for parse tree traversal
- Expression tree visitor for evaluation
- **Convention**: Implement visitor pattern for recursive data structure traversal

### 8. Concurrency Patterns
Thread-safe operations:
- `SharedCancellationHandle` - Query cancellation across threads
- `Synchronized<T>` - Mutex-protected wrapper
- `ConcurrentCache` - Lock-free caching
- **Convention**: Use for thread-safe sharing; document synchronization boundaries

### 9. Builder Pattern
Configuration and complex object construction:
- `ConfigManager` - Fluent configuration API
- `ParsedQuery` - Building query representations
- **Convention**: For complex multi-step constructions, use builder pattern

### 10. Memory Management Patterns
Constrained execution model:
- `AllocatorWithLimit` - Prevent OOM in operations
- Memory tracking throughout execution
- **Convention**: Query operations must respect memory limits; track allocations

---

## Important Conventions for AI Assistants

### Code Modification Guidelines

**Before Making Changes:**
1. Always read the file first using the Read tool
2. Understand existing patterns in the codebase
3. Search for similar implementations to understand conventions
4. Check recent commits for context on similar changes

**When Making Changes:**
1. **Minimal Modifications**: Only change what's necessary
   - Don't refactor surrounding code
   - Don't add unrelated improvements
   - Don't change formatting in untouched lines

2. **Preserve Existing Style**:
   - Match indentation (spaces vs tabs)
   - Follow Google C++ style guide (enforced by clang-format)
   - Keep line lengths under 100 characters
   - Use existing naming conventions

3. **Consistency with Codebase**:
   - Use `IdTable` / `IdTableStatic` for result containers
   - Extend `Operation` for new query operations
   - Use `Synchronized<T>` for thread-safe shared state
   - Respect memory allocation patterns

4. **Testing Requirements**:
   - Add tests in `test/` matching source structure
   - Use Google Test framework (gtest/gmock)
   - Tests should pass locally before committing
   - Aim for high coverage of new code

### File Organization Rules

**Header Files** (`.h`):
- Public interfaces in main headers
- Template implementations in headers (C++ convention)
- Use include guards: `#ifndef NAMESPACE_FILENAME_H` / `#define NAMESPACE_FILENAME_H`
- Prefer forward declarations to reduce compile dependencies

**Source Files** (`.cpp`):
- Implementation of non-template code
- Located in same directory as headers
- Follow header-relative organization

**Test Files**:
- Located in `test/` mirroring `src/` structure
- Named `*Test.cpp` (e.g., `JoinTest.cpp`)
- Include source header: `#include "engine/Join.h"`

### Memory Management Rules

**Pointer Usage:**
- Prefer `std::unique_ptr<T>` for exclusive ownership
- Prefer `std::shared_ptr<T>` for shared ownership
- Raw pointers only for non-owning references
- Avoid `new`/`delete` - use smart pointers

**Memory Allocation Patterns:**
- Operations respect `AllocatorWithLimit` constraints
- Pre-allocate `IdTable` columns with known sizes
- Use `CachingMemoryResource` for short-lived allocations
- Document memory requirements in operation comments

### Concurrency Guidelines

**Thread Safety:**
- Mark thread-safe code with clear comments
- Use `Synchronized<T>` for shared state
- Document which locks protect which data
- Use `SharedCancellationHandle` for query cancellation

**Performance Consideration:**
- Avoid locks in hot paths
- Use `ConcurrentCache` for lock-free caching
- Consider thread-local storage for thread-specific data
- Profile concurrent operations

### Error Handling

**Exception Usage:**
- C++ exceptions used for error propagation
- Catch and re-throw with context when needed
- RAII ensures cleanup even on exception
- Document which functions throw what exceptions

**Assertions & Validation:**
- Use assertions for invariants (preconditions, postconditions)
- Input validation at system boundaries only
- Trust internal code and framework guarantees
- Log errors at appropriate levels (Log.h)

---

## Testing Framework & Conventions

### Google Test (gtest/gmock) Framework

**Basic Test Structure:**
```cpp
#include <gtest/gtest.h>
#include "engine/Operation.h"

class OperationTest : public ::testing::Test {
protected:
  // Setup before each test
  void SetUp() override { }

  // Cleanup after each test
  void TearDown() override { }
};

TEST_F(OperationTest, BasicFunctionality) {
  // Arrange
  // Act
  // Assert
  ASSERT_EQ(expected, actual);
}
```

**Assertion Types:**
- `EXPECT_*` - Non-fatal assertions (test continues)
- `ASSERT_*` - Fatal assertions (test stops)
- Common: `EQ`, `NE`, `LT`, `LE`, `GT`, `GE`, `TRUE`, `FALSE`, `THAT`, `THROW`

### Test Organization

**Location Rules:**
- Test for `src/engine/Join.h` → `test/engine/JoinTest.cpp`
- Test for `src/index/Index.h` → `test/index/IndexTest.cpp`
- Mirrored directory structure

**Test File Conventions:**
- Test all public methods
- Test edge cases and error conditions
- Include integration tests where appropriate
- Mock external dependencies using gmock

### Running Tests

```bash
# All tests
ctest --output-on-failure

# Specific test file
ctest -R JoinTest --output-on-failure

# Specific test case
ctest -R JoinTest.BasicJoin --output-on-failure

# Verbose output
ctest --verbose --output-on-failure

# Parallel execution (use -j)
ctest -j 4 --output-on-failure
```

---

## Configuration & Build System

### CMakeLists.txt Structure

**Key Sections:**
1. **Project Setup** - C++ version, compiler flags
2. **Precompiled Headers** - Optional compilation speedup
3. **Dependencies** - Via FetchContent or find_package
4. **Source Organization** - File grouping
5. **Target Definitions** - Executables and libraries
6. **Test Integration** - ctest configuration

**Important Variables:**
- `CMAKE_BUILD_TYPE` - Debug or Release
- `CMAKE_CXX_STANDARD` - C++ version (20 minimum)
- `LOGLEVEL` - Info/Debug/Warn/Error
- `USE_PRECOMPILED_HEADERS` - Speedup
- `ASAN` - AddressSanitizer
- `USE_PARALLEL` - Parallel compilation

### Dependency Management

**Via Conan** (`conanfile.txt`):
- Boost, ICU, OpenSSL, Zstd, etc.
- Version specification for reproducibility
- Custom build options

**Via CMake FetchContent:**
- Google Test
- nlohmann/json
- ANTLR
- Small libraries

### Common Build Issues

**Clang-Format Related:**
- Pre-commit hooks automatically format code
- If pre-commit fails, run: `clang-format -i <file>`
- Line length: 100 characters

**Dependency Issues:**
- Clean build: `rm -rf build && mkdir build && cd build`
- Update conan dependencies: `conan install`
- Clear CMake cache: `rm CMakeCache.txt`

**Test Failures:**
- Ensure you're on correct branch
- Run tests individually to isolate failures
- Check recent commits for context
- Verify compiler version compatibility

---

## Documentation & Comments

### Comment Guidelines

**When to Add Comments:**
- Non-obvious algorithm logic
- Performance-critical sections with justification
- Complex mathematical operations
- Workarounds for known issues with explanation
- Public API documentation

**When NOT to Add Comments:**
- Self-evident code (function does what name suggests)
- Simple loops and conditions
- Setup/teardown code
- Previously unchanged code

**Comment Style:**
```cpp
// Single-line comments for brief explanations

/* Multi-line comments for longer explanations
   that span multiple lines. Use sparingly. */

/// Doxygen-style comments for public APIs
/// \param operands The input data
/// \return The computed result
```

### Documentation Files

- **README.md** - Project overview and quick start
- **CONTRIBUTING.md** - How to contribute (if exists)
- **docs/** - Additional documentation (architecture, algorithms)
- **docs/development/** - Development guides
- **CLAUDE.md** - This file (AI assistant guide)

---

## Common Tasks & Patterns

### Adding a New Operation Type

1. **Create Header** (`src/engine/NewOperation.h`):
   ```cpp
   #include "Operation.h"

   class NewOperation : public Operation {
   public:
     NewOperation(...);
     ResultTable execute() override;
     // Other virtual methods...
   };
   ```

2. **Create Implementation** (`src/engine/NewOperation.cpp`):
   - Implement execute() and other methods
   - Follow existing operation patterns

3. **Register with Planner** (`src/engine/QueryPlanner.cpp`):
   - Add cost estimation
   - Update plan creation logic

4. **Add Tests** (`test/engine/NewOperationTest.cpp`):
   - Test basic functionality
   - Test edge cases
   - Test with different column widths (IdTableStatic)

5. **Update CMakeLists.txt**:
   - Add source files to executable targets

### Adding a New SPARQL Expression

1. **Create Expression Class** (`src/engine/sparqlExpressions/NewExpression.h`)
2. **Implement Evaluation** - Override `operator()`
3. **Add Type Checking** - Validate input types
4. **Test Thoroughly** - Including error cases
5. **Document** - Explain SPARQL standard compliance

### Optimizing a Query Operation

1. **Profile** - Identify bottleneck (ctest + profiler)
2. **Understand** - Study current implementation thoroughly
3. **Optimize** - Minimal, targeted changes
4. **Benchmark** - Measure improvement
5. **Test** - Ensure correctness, add benchmarks

### Adding RDF Type Support

1. **Update RDFTypes** (`src/rdfTypes/`)
2. **Update Index** - Handle new type in storage
3. **Update Parser** - Parse new type syntax
4. **Update Operations** - Handle in expressions
5. **Add Tests** - Comprehensive test coverage

---

## Troubleshooting

### Build Failures

**Clang-Format Errors:**
```bash
# Auto-fix formatting
clang-format -i src/engine/file.cpp

# Or use pre-commit
pre-commit run --all-files
```

**Compiler Errors:**
- Ensure C++20 compiler (GCC 11+ or Clang 16+)
- Check CMakeLists.txt for correct C++ version
- Verify all dependencies are installed

**Link Errors:**
- Ensure libraries are in CMakeLists.txt
- Check library order in target_link_libraries
- Verify shared library dependencies

### Test Failures

**Flaky Tests:**
- Re-run with `ctest --rerun-failed`
- Check for race conditions (use thread sanitizer)
- Verify test isolation

**Segmentation Faults:**
```bash
# Build with address sanitizer
cmake -DCMAKE_BUILD_TYPE=ASAN ..
cmake --build .
ctest --output-on-failure
```

### Performance Issues

**Profiling:**
```bash
# Build with profiling
cmake -DPERFTOOLS_PROFILER=ON ..
# Run with profiling enabled
# Results in gperf output
```

**Optimization Tips:**
- Use `IdTableStatic<N>` instead of dynamic `IdTable` for fixed-width results
- Avoid unnecessary allocations in hot paths
- Use `Synchronized<T>` sparingly
- Consider `ConcurrentCache` for frequently accessed data

---

## Quick Reference: File Navigation

| Task | Location |
|------|----------|
| Query Execution | `src/engine/` |
| SPARQL Functions | `src/engine/sparqlExpressions/` |
| RDF Storage | `src/index/` |
| Text Search | `src/index/textIndex/` |
| Spatial Queries | `src/index/spatial/` |
| SPARQL Parsing | `src/parser/` |
| Utilities | `src/util/` |
| RDF Types | `src/rdfTypes/` |
| Engine Tests | `test/engine/` |
| Index Tests | `test/index/` |
| Parser Tests | `test/parser/` |
| CI/CD Config | `.github/workflows/` |
| Code Style | `.clang-format`, `.pre-commit-config.yaml` |
| Build Config | `CMakeLists.txt`, `conanfile.txt` |

---

## Useful Commands

```bash
# Clone and setup
git clone <repo>
cd qlever
git checkout claude/add-claude-documentation-LP2W5

# Build
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release -GNinja ..
cmake --build .

# Test
ctest --output-on-failure
ctest -R TestName --output-on-failure

# Format
pre-commit run --all-files
# or
clang-format -i src/engine/file.cpp

# Search for patterns
grep -r "class Operation" src/
grep -r "IdTableStatic" src/

# Git operations
git status
git add <files>
git commit -m "message"
git push -u origin claude/add-claude-documentation-LP2W5
```

---

## Resources & Links

- **Repository**: https://github.com/seanchatmangpt/qlever
- **SPARQL Spec**: https://www.w3.org/TR/sparql11-query/
- **RDF Spec**: https://www.w3.org/RDF/
- **CMake Documentation**: https://cmake.org/cmake/help/latest/
- **Google Test Documentation**: https://google.github.io/googletest/
- **Boost Documentation**: https://www.boost.org/doc/libs/1_81_0/
- **ANTLR**: https://www.antlr.org/

---

## Document Metadata

- **Last Updated**: 2025-12-31
- **Created For**: AI Assistant Development Support
- **Scope**: Comprehensive guide for QLever codebase
- **Status**: Complete and Ready for Use

**Maintenance Notes:**
- Update this document when major architectural changes occur
- Document new design patterns as they emerge
- Keep technology stack section current with dependency updates
- Add new troubleshooting entries as issues are discovered
