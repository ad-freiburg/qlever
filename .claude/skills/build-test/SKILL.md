---
name: build-test
description: Build QLever C++ project with CMake, run tests with ctest, and manage build configuration. Use when building, testing, or debugging the project.
---

# Build & Test Skill for QLever

Automates CMake builds, test execution, and build troubleshooting for the QLever C++ RDF/SPARQL database project.

## Quick Start

### Building the project

```bash
# Clean build with Release configuration
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=Release -GNinja ..
cmake --build .
```

### Running tests

```bash
# All tests
ctest --output-on-failure

# Specific test
ctest -R TestName --output-on-failure

# With verbose output
ctest --verbose --output-on-failure

# Parallel execution
ctest -j 4 --output-on-failure
```

## Build Configurations

Choose the appropriate build type for your task:

| Configuration | Flag | Use Case |
|--------------|------|----------|
| Release | `-DCMAKE_BUILD_TYPE=Release` | Production, benchmarking |
| Debug | `-DCMAKE_BUILD_TYPE=Debug` | Development, debugging |
| ASAN | `-DCMAKE_BUILD_TYPE=ASAN` | Memory error detection |

## Advanced Build Options

```bash
# Parallel compilation (faster builds)
cmake -DUSE_PARALLEL=true ..

# Precompiled headers (faster rebuilds)
cmake -DUSE_PRECOMPILED_HEADERS=true ..

# Set log level
cmake -DLOGLEVEL=INFO ..
```

## Common Build Issues

### Clang-format errors

```bash
# Auto-fix formatting in one file
clang-format -i src/engine/file.cpp

# Fix all files in build
pre-commit run --all-files
```

### Dependency issues

```bash
# Clean build (removes all artifacts)
rm -rf build
mkdir build && cd build

# Update conan dependencies
conan install ..

# Clear CMake cache
rm CMakeCache.txt
```

### Link errors

- Verify all libraries in CMakeLists.txt target_link_libraries
- Check library order (order matters in some cases)
- Ensure shared library dependencies are listed

## Running specific tests

```bash
# By test class
ctest -R JoinTest --output-on-failure

# By specific test case
ctest -R JoinTest.BasicJoin --output-on-failure

# Skip specific tests
ctest -E SkipThisTest --output-on-failure
```

## Address Sanitizer for memory debugging

```bash
# Build with AddressSanitizer enabled
cmake -DCMAKE_BUILD_TYPE=ASAN -GNinja ..
cmake --build .

# Run tests with ASAN
ASAN_OPTIONS=verbosity=1:log_threads=1 ctest --output-on-failure
```

## Test location conventions

Tests are organized to mirror source structure:
- `src/engine/Join.h` → `test/engine/JoinTest.cpp`
- `src/index/Index.h` → `test/index/IndexTest.cpp`
- `src/parser/QueryParser.h` → `test/parser/QueryParserTest.cpp`

## Build troubleshooting

**GCC/Clang version**: Ensure C++20 compiler
- GCC 11.0+
- Clang 16.0+

**CMake version**: Requires CMake 3.27+

**Missing headers**: Run `pre-commit install` to set up format hooks

## Key build directories

- **Libraries**: `build/lib/`
- **Executables**: `build/`
- **Main binaries**:
  - `ServerMain` - SPARQL query server
  - `IndexBuilderMain` - Build indexes from RDF data
  - `VocabularyMergerMain` - Merge vocabularies
