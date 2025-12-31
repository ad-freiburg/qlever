# QLever Agent Skills Registry

This directory contains custom Agent Skills for the QLever C++ RDF/SPARQL database project. Skills extend Claude's capabilities with domain-specific knowledge and workflows tailored to QLever development.

## Available Skills

### 1. **build-test** - Build & Test Automation
Master CMake builds, test execution, and build configuration for the QLever C++ project.

**When to use:**
- Building the project with different configurations
- Running test suites (all tests, specific tests, with verbosity)
- Troubleshooting build failures
- Setting up clean builds
- Configuring advanced build options (ASAN, precompiled headers, etc.)

**Key topics:**
- CMake build configurations (Release, Debug, ASAN)
- ctest command patterns and filtering
- Build troubleshooting and dependency resolution
- Test organization and conventions
- AddressSanitizer for memory error detection

### 2. **code-quality** - Code Quality & Formatting
Enforce code quality standards through formatting, linting, and static analysis.

**When to use:**
- Formatting code with clang-format
- Running pre-commit hooks
- Performing spell checking
- Static analysis with clang-tidy or SonarCloud
- Memory error detection with ASAN
- Code review preparation

**Key topics:**
- Google C++ style guide compliance (100-char lines)
- Clang-format automation
- Pre-commit hook setup and execution
- Codespell for spell checking
- SonarCloud integration
- AddressSanitizer and UBSAN for quality checks
- Code review checklist

### 3. **sparql-rdf** - SPARQL & RDF Knowledge
Understand SPARQL query semantics, RDF data models, and semantic web standards.

**When to use:**
- Writing or optimizing SPARQL queries
- Understanding RDF concepts and data models
- Learning SPARQL functions and expressions
- Debugging SPARQL queries
- Understanding QLever-specific features (full-text search, spatial queries)
- Working with RDF vocabularies

**Key topics:**
- RDF triple basics (IRI, Literal, Blank Node)
- SPARQL query structure (SELECT, WHERE, FILTER)
- Graph patterns and joins
- SPARQL functions (string, numeric, date/time, type checking)
- Property paths and named graphs
- QLever spatial and full-text search
- Query optimization strategies
- Common SPARQL patterns
- RDF vocabularies (FOAF, DCAT, DBpedia, etc.)

### 4. **debug-profile** - Debugging & Performance Profiling
Advanced techniques for debugging memory issues, profiling performance, and optimizing bottlenecks.

**When to use:**
- Investigating memory leaks or heap corruption
- Profiling slow queries or operations
- Detecting race conditions in multi-threaded code
- Using GDB/LLDB debuggers
- Analyzing performance bottlenecks
- Optimizing memory usage
- Detecting undefined behavior

**Key topics:**
- AddressSanitizer (ASAN) for memory error detection
- Undefined Behavior Sanitizer (UBSAN)
- Valgrind for memory profiling
- CPU profiling with perf and Perftools
- Flame graph generation and analysis
- GDB debugger commands and strategies
- LLDB debugger basics
- Query execution tracing
- ThreadSanitizer for race condition detection
- Memory leak detection and analysis
- Performance bottleneck identification

### 5. **cpp-patterns** - C++ Design Patterns & Architecture
Apply C++ design patterns, understand QLever's architecture, and write idiomatic modern C++.

**When to use:**
- Designing new features or operations
- Understanding existing code architecture
- Refactoring for better design patterns
- Implementing template metaprogramming
- Managing C++20 modern features
- Understanding concurrency patterns
- Memory management best practices

**Key topics:**
- QLever core design patterns:
  - Strategy Pattern (Operation hierarchy)
  - Cost-Based Query Optimization
  - Pimpl Pattern (implementation hiding)
  - Template Metaprogramming
  - RAII resource management
  - Lazy Evaluation & Streaming
  - Visitor Pattern
  - Concurrency patterns
  - Builder Pattern
  - Memory Management patterns
- C++20 features (concepts, ranges, coroutines, spaceship operator)
- File organization conventions
- Testing best practices
- Optimization strategies
- Dependency architecture
- When to apply specific patterns

## Skill Usage Examples

### Example 1: Building the project and running tests

```bash
# Use the build-test Skill
# "Build the project and run all tests with AddressSanitizer"
# Claude will use SKILL.md to guide the build process
```

### Example 2: Formatting code

```bash
# Use the code-quality Skill
# "Format my code and check for spelling errors"
# Claude will apply clang-format and codespell
```

### Example 3: Writing SPARQL queries

```bash
# Use the sparql-rdf Skill
# "Write a SPARQL query that finds all people over 30 with email"
# Claude will use SPARQL patterns and best practices
```

### Example 4: Debugging memory issue

```bash
# Use the debug-profile Skill
# "This test is crashing. Help me debug with AddressSanitizer"
# Claude will guide ASAN build and analysis
```

### Example 5: Implementing new operation

```bash
# Use the cpp-patterns Skill
# "I need to implement a new query operation. What pattern should I use?"
# Claude will reference Strategy Pattern and other relevant patterns
```

## Skill Discovery

Skills are automatically discovered by Claude when:
1. You ask about relevant tasks
2. Your request matches a Skill's description
3. You explicitly invoke by name in your request

For example:
- "Build the project" → Triggers **build-test** skill
- "Format this code" → Triggers **code-quality** skill
- "Write a SPARQL query" → Triggers **sparql-rdf** skill
- "Debug this crash" → Triggers **debug-profile** skill
- "How should I structure this?" → Triggers **cpp-patterns** skill

## Skill Architecture

Each Skill is a directory with:
- **SKILL.md** - Main instruction file with workflows, patterns, and guidance
- **README.md** - Overview of what the Skill provides (this file at root)

Files are loaded on-demand:
- **Metadata** (name, description) - Always loaded
- **Instructions** (SKILL.md) - Loaded when Skill is triggered
- **Examples** - Included in SKILL.md for immediate reference

## Contributing to Skills

To add or improve Skills:

1. **Add a new Skill**: Create a directory in `.claude/skills/` with `SKILL.md`
2. **Update existing Skill**: Edit the relevant `SKILL.md` file
3. **Follow conventions**:
   - Skill names: lowercase, hyphens only
   - SKILL.md must include YAML frontmatter
   - Include practical examples and command patterns
   - Link to official documentation when relevant

## Integration with Development

### Pre-commit Hooks
The **code-quality** Skill automates what `.pre-commit-config.yaml` defines:
- Format checking with clang-format
- Spell checking with codespell
- Code quality standards

### Build System
The **build-test** Skill documents CMake configuration options from `CMakeLists.txt`:
- C++ version requirements
- Available build flags
- Test organization

### Code Architecture
The **cpp-patterns** Skill explains patterns evident in the codebase:
- Operation hierarchy in `src/engine/`
- Pimpl pattern in `src/index/`
- Template specialization in `src/util/`

### Query Execution
The **sparql-rdf** Skill covers SPARQL/RDF concepts executed by the engine:
- Query patterns matching `src/parser/` output
- Expression evaluation in `src/engine/sparqlExpressions/`
- Index operations in `src/index/`

## Quick Reference: Skills by Task

| Task | Skill | Subtopic |
|------|-------|----------|
| Build project | build-test | CMake, build configurations |
| Run tests | build-test | ctest patterns, filtering |
| Debug test failure | build-test | ASAN, troubleshooting |
| Format code | code-quality | clang-format |
| Check code quality | code-quality | Pre-commit, linting |
| Spell check | code-quality | codespell |
| Write SPARQL query | sparql-rdf | Query structure, functions |
| Optimize query | sparql-rdf | Query optimization |
| Debug memory leak | debug-profile | ASAN, Valgrind |
| Profile performance | debug-profile | perf, flame graphs |
| Design new feature | cpp-patterns | Design patterns |
| Understand architecture | cpp-patterns | Code organization |
| Implement operation | cpp-patterns | Strategy pattern, templates |

## Additional Resources

- **CLAUDE.md** - Comprehensive codebase guide for AI assistants
- **Official Documentation**:
  - SPARQL Spec: https://www.w3.org/TR/sparql11-query/
  - RDF Spec: https://www.w3.org/RDF/
  - CMake Docs: https://cmake.org/cmake/help/latest/
  - Google Test: https://google.github.io/googletest/

## Support

Skills are maintained as part of the QLever project. To contribute improvements:
1. Edit relevant SKILL.md files
2. Test that Claude can find and use the skills
3. Commit with descriptive message: `docs: Update skill content`

For issues or suggestions, consult the CLAUDE.md guide for development guidelines.
