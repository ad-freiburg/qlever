# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

QLever is a high-performance SPARQL engine written in C++ for efficiently indexing and querying very large knowledge graphs (100+ billion triples). The project uses CMake as its build system and includes comprehensive testing with GoogleTest.

## Build System & Commands

### Building the Project
```bash
# Configure with CMake (creates build directory)
cmake -B build -DCMAKE_BUILD_TYPE=Release

# Build the project 
cmake --build build --config Release -- -j $(nproc)

# Alternative: Build specific targets
cmake --build build --target ServerMain
cmake --build build --target IndexBuilderMain
```

### Preferred Build & Test Commands (Claude Code)
**IMPORTANT**: Always use the `cmake-build-clang-18-debug` directory for builds and tests.
```bash
# Build specific test binaries (preferred method)
cmake --build cmake-build-clang-18-debug --target QueryPlannerTest -- -j $(nproc)
cmake --build cmake-build-clang-18-debug --target IndexScanTest -- -j $(nproc)
cmake --build cmake-build-clang-18-debug --target StripColumnsTest -- -j $(nproc)

# Run specific test binary
cd cmake-build-clang-18-debug/test && ./QueryPlannerTest --gtest_filter="*SubqueryColumnStripping*"

# Run all tests from specific test binary
cd cmake-build-clang-18-debug/test && ./QueryPlannerTest

# Build all tests
cmake --build cmake-build-clang-18-debug -- -j $(nproc)
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
# Check code formatting (requires clang-format-16)
./misc/format-check.sh

# Format all source files automatically  
./format-check.sh

# The format checker expects clang-format-16 specifically
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

### Code Style
- Uses clang-format-16 with project-specific configuration
- Enforced via CI pipeline
- Format check script: `misc/format-check.sh`

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