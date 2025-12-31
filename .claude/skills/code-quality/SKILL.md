---
name: code-quality
description: Enforce code formatting, run linting, perform static analysis, and manage code quality checks. Use when formatting code, checking style, or analyzing code quality.
---

# Code Quality Skill for QLever

Maintains code quality through formatting, linting, and static analysis using tools integrated with the QLever project.

## Code Formatting with clang-format

QLever uses **clang-format 16.0.6** following Google C++ style with 100-character line limits.

### Auto-formatting files

```bash
# Format a single file
clang-format -i src/engine/file.cpp

# Format multiple files
clang-format -i src/engine/*.cpp src/engine/*.h

# Format entire directory
find src/ -type f \( -name "*.cpp" -o -name "*.h" \) -exec clang-format -i {} \;
```

### Pre-commit hooks

QLever uses pre-commit hooks for automatic formatting:

```bash
# Install pre-commit hooks
pre-commit install

# Run all hooks on all files
pre-commit run --all-files

# Run specific hook (e.g., clang-format)
pre-commit run clang-format --all-files

# Run on staged files only
pre-commit run
```

## Code Style Standards

Follow these style conventions:

| Standard | Value |
|----------|-------|
| **Style Guide** | Google C++ Style |
| **Line Length** | 100 characters max |
| **Indentation** | Spaces (not tabs) |
| **Naming** | snake_case for functions/variables, PascalCase for classes |
| **Include Guards** | `#ifndef NAMESPACE_FILENAME_H` pattern |
| **Trailing Spaces** | Remove all |

### Style example

```cpp
// Good: Clear, concise, follows style
class QueryExecutionTree {
public:
  ResultTable execute(const QueryExecutionContext& ctx);

private:
  std::vector<std::unique_ptr<Operation>> operations_;
};

// Bad: Inconsistent style
class QueryExecutionTree{
public:
ResultTable execute( const QueryExecutionContext&ctx );
private:
std::vector<std::unique_ptr<Operation> >operations_;
};
```

## Spell Checking with codespell

QLever uses **codespell v2.6** to detect spelling errors:

```bash
# Check spelling in all files
codespell

# Check specific directory
codespell src/

# Fix spelling errors automatically
codespell --fix

# Check with custom config
codespell --config .codespellrc
```

Common false positives can be added to `.codespellrc`.

## Static Analysis

### Using clang-tidy

```bash
# Analyze a single file
clang-tidy src/engine/Join.cpp -- -std=c++20 -I src

# Analyze all files
find src -name "*.cpp" | xargs clang-tidy

# Apply fixes automatically
clang-tidy --fix src/engine/Join.cpp
```

### SonarCloud integration

QLever integrates with SonarCloud for cloud-based analysis:

```bash
# Check SonarCloud results
# Results automatically uploaded via GitHub Actions CI/CD
# View at: https://sonarcloud.io/project/overview?id=seanchatmangpt_qlever
```

## Memory Error Detection

### AddressSanitizer (ASAN)

```bash
# Build with ASAN enabled
cmake -DCMAKE_BUILD_TYPE=ASAN -GNinja ..
cmake --build .

# Run with ASAN
ASAN_OPTIONS=verbosity=1:log_threads=1 ctest --output-on-failure

# Detect memory leaks
LSAN_OPTIONS=verbosity=1 ./build/ServerMain
```

### Undefined Behavior Sanitizer (UBSAN)

```bash
# Include in ASAN build for additional checks
cmake -DCMAKE_BUILD_TYPE=ASAN -GNinja ..
```

## CI/CD Quality Gates

QLever's GitHub Actions pipeline enforces:

1. **Format verification** - All code must pass clang-format
2. **Spell checking** - codespell must pass
3. **C++17 compatibility** - Code must work with C++17 fallback
4. **Test coverage** - All new code must have tests
5. **SPARQL conformance** - Query compliance tests must pass

## Code Review Checklist

Before committing, verify:

- [ ] Code passes clang-format: `pre-commit run --all-files`
- [ ] No spelling errors: `codespell src/`
- [ ] Tests pass: `ctest --output-on-failure`
- [ ] Memory clean: `ASAN_OPTIONS=... ctest`
- [ ] No compiler warnings (treat warnings as errors)
- [ ] Comments explain non-obvious logic only
- [ ] No trailing whitespace or tab characters

## Commit message quality

Follow these patterns for clear commit messages:

```
feat: Add materialized views support
fix: Handle blank nodes in vocabulary
optimize: Improve join performance with column reordering
refactor: Extract common code into utility function
docs: Update architecture guide
test: Add tests for optional joins
chore: Update dependencies to latest versions
```

## Large-scale refactoring

For widespread formatting changes:

```bash
# Stage all formatting changes
pre-commit run --all-files

# Review changes
git diff --cached

# Commit with appropriate message
git commit -m "chore: Run clang-format across codebase"
```

## Integration with build

Code quality checks are automatically enforced:

```bash
# Build system verifies formatting
cmake --build .

# Tests verify no memory errors
ctest --output-on-failure
```

## Configuration files

- **.clang-format** - Defines formatting rules
- **.pre-commit-config.yaml** - Defines which hooks run
- **.codespellrc** - Spell checker configuration
- **CMakeLists.txt** - Build verification settings
