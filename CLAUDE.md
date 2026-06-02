# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

QLever is a high-performance SPARQL engine written in C++ for efficiently indexing and querying very large knowledge graphs (100+ billion triples). The project uses CMake as its build system and includes comprehensive testing with GoogleTest.

## Build System & Commands

### Building the Project
```bash
# Configure with CMake (creates build directory)
cmake -B build -DCMAKE_CXX_COMPILER=clang++-18 -DCMAKE_BUILD_TYPE=Debug -GNinja -DCHEAPER_COMPILATION=ON -DCMAKE_EXE_LINKER_FLAGS="-fuse-ld=mold" -DCMAKE_SHARED_LINKER_FLAGS="-fuse-ld=mold"

# Build the project 
cmake --build build

# Alternative: Build specific targets
cmake --build build --target qlever-index
cmake --build build --target qlever-server
```

### Test Commands (Claude Code)
```bash
cmake --build build --target StripColumnsTest -- -j 10

# Run specific test binary
cd build/test && ./QueryPlannerTest --gtest_filter="*SubqueryColumnStripping*"

# Run all tests from specific test binary
cd build/test && ./QueryPlannerTest
```

### Testing (General)
```bash
# Run all tests from build directory
cd build && ctest

# Run tests with verbose output on failure
cd build && env CTEST_OUTPUT_ON_FAILURE=1 ctest

# Run single test binary (if not using SINGLE_TEST_BINARY)
cd build/test && ./SpecificTest

# When SINGLE_TEST_BINARY=ON is used (common in CI):
cd build/test && ./QLeverAllUnitTestsMain
```

### Code Formatting & Quality
```bash
# Check code formatting (requires clang-format-18)
./misc/format-check.sh

# Format all source files automatically  
./format-check.sh

# The format checker expects clang-format-18 specifically
```

### Key Build Options
- `CMAKE_BUILD_TYPE`: Debug, Release, RelWithDebInfo
- `SINGLE_TEST_BINARY=ON`: Links all tests into one executable (recommended for CI)
- `USE_PARALLEL=true`: Enables parallel processing
- `RUN_EXPENSIVE_TESTS=true`: Includes computationally expensive tests
- `ENABLE_EXPENSIVE_CHECKS=true`: Enables runtime checks with performance overhead

## Architecture Overview

### Core Components

**Index System (`src/index/`)**
- `Index.h/cpp`: Main index interface using PIMPL pattern
- `IndexImpl.h/cpp`: Core index implementation
- `Permutation.h/cpp`: Handles different sort orders (SPO, PSO, etc.)
- `Vocabulary.h/cpp`: String-to-ID mapping system
- `CompressedRelation.h/cpp`: Compressed storage for triple relations

**Query Engine (`src/engine/`)**
- `Engine.h/cpp`: Core query execution engine with parallel sorting
- `QueryPlanner.h/cpp`: Converts SPARQL to execution plans
- `QueryExecutionTree.h/cpp`: Represents query execution plans as trees
- `Operation.h/cpp`: Base class for all query operations
- Individual operation classes: `Join.h/cpp`, `Filter.h/cpp`, `IndexScan.h/cpp`, etc.
- `Server.h/cpp`: HTTP server for SPARQL endpoints

**Parser (`src/parser/`)**
- `SparqlParser.h/cpp`: Main SPARQL parser
- `RdfParser.h/cpp`: RDF data parser (Turtle, N-Triples)
- `sparqlParser/`: ANTLR4-generated SPARQL grammar implementation
- `TripleComponent.h/cpp`: Represents RDF triple components

**Utilities (`src/util/`)**
- `HashMap.h`, `HashSet.h`: High-performance hash containers (Abseil-based)
- `Cache.h`, `ConcurrentCache.h`: Caching implementations
- `ConfigManager/`: Configuration system with validation
- `http/`: HTTP client/server utilities using Boost.Beast

### Key Executables

- `ServerMain`: SPARQL endpoint server
- `IndexBuilderMain`: Creates indexes from RDF data
- `VocabularyMergerMain`: Merges vocabulary files
- `PrintIndexVersionMain`: Displays index format version

### Dependencies

The project uses FetchContent for most dependencies:
- GoogleTest/GMock for testing
- ANTLR4 for SPARQL parsing
- Abseil for containers and utilities
- Boost (iostreams, program_options, url)
- ICU for internationalization
- OpenSSL for cryptography
- ZSTD for compression

## Development Workflow

### Testing Strategy
- Comprehensive unit tests for all components
- Integration tests via CTest
- CI runs tests with multiple compiler versions (GCC 11-13, Clang 16-18)
- Sanitizer builds (AddressSanitizer, UBSan, ThreadSanitizer)
- Format checking enforced in CI

### Copyright Header

Every new source file must begin with the following copyright header (adjust the year if not 2026):

```cpp
// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.
```

### Code Style
- Uses clang-format-18 with project-specific configuration
- Enforced via CI pipeline
- Format check script: `misc/format-check.sh`
- **Comment Convention**: All comments are formulated as complete sentences that end with a period (".")
  - Example: `// Helper to create a BlankNodeManager with default minIndex.`
  - NOT: `// Helper to create a BlankNodeManager with default minIndex`
- **Code references in comments**: Any code reference in a comment (filenames, class names, function names, expressions, etc.) must be wrapped in backticks.
  - Example: `// For details on the `Blubb` class see `BlubbImpl.h`, in particular the `doSomethingWithBlubb()` function.`
- **Ranges and views**: Always use `ql::ranges` and `ql::views` instead of `std::ranges` and `std::views`. They are drop-in replacements defined in `"backports/algorithm.h"`.
- **Data members**: Class data members use `camelCaseWithTrailingUnderscore_` (e.g., `numElements_`, `isValid_`).
- **Class/struct member order**: For new classes/structs, and when existing ones are heavily modified, use this order:
  1. `public` typedefs and constants, then `public` data members. (Preferred order, but a typedef that is only needed for the type of a member may stand directly before that member.)
  2. `private` typedefs, constants, and data members.
  3. `public` constructors: default constructor first, then other constructors, lastly copy/move constructors and assignment operators.
  4. `public` other member functions.
  5. `private` constructors (same ordering as for public constructors).
  6. `private` other member functions.
- **Function definition separator**: Function definitions in `.cpp` files that have their documentation at the declaration (in the `.h` file) must be preceded by an empty separator comment line of exactly 80 characters:
  ```cpp
  // ____________________________________________________________________________
  ReturnType ClassName::methodName(...) {
  ```

### Build Configurations
The project supports multiple build types optimized for different use cases:
- Release builds for production
- Debug builds for development
- Sanitizer builds for error detection
- Coverage builds for test coverage analysis

### Docker Integration
- Dockerfiles provided for Ubuntu 20.04 and 22.04
- Master Makefile (`master.Makefile`) provides Docker-based development workflow
- Supports both index building and server deployment via containers
