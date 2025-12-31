---
name: debug-profile
description: Debug memory issues, profile performance, analyze bottlenecks, and optimize code execution. Use when troubleshooting crashes, memory leaks, or performance problems.
---

# Debugging & Profiling Skill for QLever

Advanced techniques for debugging, memory analysis, and performance profiling of the QLever C++ database.

## Memory Debugging with AddressSanitizer (ASAN)

### Building with ASAN

```bash
# Build with AddressSanitizer enabled
cmake -DCMAKE_BUILD_TYPE=ASAN -GNinja ..
cmake --build .
```

### Running tests with ASAN

```bash
# Run with verbose ASAN output
ASAN_OPTIONS=verbosity=1:log_threads=1 ctest --output-on-failure

# Run specific test with ASAN
ASAN_OPTIONS=verbosity=1 ctest -R TestName --output-on-failure

# Full output with stack traces
ASAN_OPTIONS=verbosity=2:halt_on_error=1 ctest --output-on-failure
```

### Detecting memory leaks

```bash
# LeakSanitizer (LSAN) detects memory leaks
LSAN_OPTIONS=verbosity=1 ./build/ServerMain &
sleep 2
kill %1

# Parse output for leak detection
```

### Common ASAN errors

| Error | Cause | Fix |
|-------|-------|-----|
| `heap-buffer-overflow` | Writing beyond allocated buffer | Check array bounds and vector sizes |
| `use-after-free` | Accessing freed memory | Verify smart pointer lifetimes |
| `double-free` | Freeing same memory twice | Check deletion logic |
| `leak` | Unreleased memory | Verify RAII cleanup or smart pointers |

### ASAN output example

```
==12345==ERROR: AddressSanitizer: heap-buffer-overflow on unknown address 0x...
SUMMARY: AddressSanitizer: heap-buffer-overflow src/engine/Join.cpp:42 in ...
```

## Undefined Behavior Sanitizer (UBSAN)

### Building with UBSAN

```bash
# UBSAN is often included with ASAN build
cmake -DCMAKE_BUILD_TYPE=ASAN -GNinja ..

# Detect undefined behavior
UBSAN_OPTIONS=verbosity=1 ctest --output-on-failure
```

### Common UB detections

- Integer overflow
- Null pointer dereference
- Out-of-bounds array access
- Signed integer overflow
- Type mismatch

## Valgrind Memory Profiling

### Installation

```bash
sudo apt-get install valgrind  # Debian/Ubuntu
brew install valgrind          # macOS
```

### Memory check

```bash
# Check for memory leaks
valgrind --leak-check=full --show-leak-kinds=all \
  ./build/ServerMain < query.sparql

# With detailed output
valgrind --leak-check=full --track-origins=yes \
  ./build/ServerMain < query.sparql
```

### Memcheck options

| Option | Purpose |
|--------|---------|
| `--leak-check=full` | Detailed leak information |
| `--track-origins=yes` | Track where uninitialized values come from |
| `--show-leak-kinds=all` | Show all leak types |
| `--suppressions=file` | Suppress expected leaks |

## Performance Profiling

### CPU Profiling with perf

```bash
# Record CPU profile
perf record -g ./build/ServerMain < query.sparql

# View results
perf report

# Flamegraph visualization
perf record -g ./build/ServerMain
perf script > out.perf
# Use flamegraph.pl to visualize
```

### Google Perftools (pprof)

```bash
# Build with perftools enabled
cmake -DPERFTOOLS_PROFILER=ON ..

# Run with profiling
CPUPROFILE=profile.prof ./build/ServerMain

# Analyze profile
google-pprof ./build/ServerMain profile.prof
```

### Flame Graph Generation

```bash
# Install flamegraph tools
git clone https://github.com/brendangregg/FlameGraph

# Generate flame graph from perf
perf script | ./FlameGraph/stackcollapse-perf.pl | \
  ./FlameGraph/flamegraph.pl > graph.svg
```

## Memory Profiling with Massif

```bash
# Detailed memory profiling
valgrind --tool=massif ./build/ServerMain < query.sparql

# View results
ms_print massif.out.* > memory_profile.txt
```

## C++ Debugging with GDB

### Starting debugging

```bash
# Debug with GDB
gdb ./build/ServerMain

# Common commands
(gdb) break Join.cpp:42       # Set breakpoint at line
(gdb) break QueryPlanner      # Break in class method
(gdb) run                      # Start execution
(gdb) continue                 # Resume after break
(gdb) step                     # Step into function
(gdb) next                     # Step over function
(gdb) finish                   # Run until function return
```

### Inspecting variables

```gdb
(gdb) print variable           # Print variable value
(gdb) print *pointer           # Dereference pointer
(gdb) print array[10]          # Array element
(gdb) whatis variable          # Show variable type
(gdb) info locals              # Show all local variables
(gdb) backtrace                # Show call stack
```

### Breakpoint strategies

```gdb
# Break on condition
(gdb) break Join.cpp:42 if size > 1000

# Break on specific value
(gdb) break Operation.cpp:100 if operation_type == JOIN

# List breakpoints
(gdb) info breakpoints
```

## Lldb (Clang Debugger)

### Basic usage

```bash
# Start Lldb
lldb ./build/ServerMain

# Commands (similar to GDB)
(lldb) breakpoint set --file Join.cpp --line 42
(lldb) run
(lldb) process continue
(lldb) frame variable
```

## Query Execution Analysis

### Enable logging

```bash
# Build with debug logging
cmake -DLOGLEVEL=DEBUG ..

# Run with verbose output
./build/ServerMain --loglevel DEBUG < query.sparql
```

### Trace execution

```cpp
// In C++ code, use logging macros
LOG(INFO) << "Starting join with " << left_size << " rows";
LOG(DEBUG) << "Join intermediate result: " << result;
```

## Identifying Performance Bottlenecks

### Profiling checklist

1. **Collect profile data**
   ```bash
   perf record -g ./build/ServerMain < large_query.sparql
   ```

2. **Identify hot functions**
   ```bash
   perf report --sort comm,dso,sym --no-children
   ```

3. **Examine call chains**
   ```bash
   perf report --graph=fractal
   ```

4. **Benchmark before/after**
   - Run same query multiple times
   - Measure total time and memory
   - Compare results

### Memory usage analysis

```bash
# Monitor memory during execution
/usr/bin/time -v ./build/ServerMain < query.sparql

# Expected output includes:
# - Maximum resident set size (peak memory)
# - User time and system time
# - Page faults and I/O statistics
```

## Query Performance Optimization

### Identify slow queries

```bash
# Log query execution times
./build/ServerMain --log-queries --min-duration 100ms

# Analyze slow queries
grep "SLOW QUERY" server.log | sort | uniq -c
```

### Profile-guided optimization

1. **Baseline**: Run query, measure time/memory
2. **Profile**: Use perf/Valgrind to identify bottleneck
3. **Hypothesis**: Understand root cause
4. **Fix**: Implement optimization
5. **Verify**: Confirm improvement with benchmark

## Compiler Optimization Flags

### Build optimization levels

| Flag | Optimization | Use Case |
|------|-------------|----------|
| `-O0` | No optimization | Debugging |
| `-O1` | Basic optimization | Faster builds, still debuggable |
| `-O2` | Standard optimization | Production (default) |
| `-O3` | Aggressive optimization | Maximum performance |
| `-Ofast` | Maximum speed | Numerical computations |

### Example: Profile-guided optimization

```bash
# Build with PGO (if supported)
cmake -DCMAKE_CXX_FLAGS="-fprofile-generate" ..
cmake --build .

# Generate profile data with representative queries
./build/ServerMain < workload.sparql

# Rebuild with profile
cmake -DCMAKE_CXX_FLAGS="-fprofile-use -fprofile-correction" ..
cmake --build .
```

## Threading Analysis

### Thread sanitizer

```bash
# Build with ThreadSanitizer
cmake -DCMAKE_BUILD_TYPE=TSAN -GNinja ..
cmake --build .

# Run tests
TSAN_OPTIONS=verbosity=1 ctest --output-on-failure
```

### Detecting race conditions

```bash
# TSAN detects data races
TSAN_OPTIONS=halt_on_error=1 ./build/ServerMain
```

## Core Dump Analysis

### Enable core dumps

```bash
# Allow core dumps
ulimit -c unlimited

# Run program
./build/ServerMain

# Analyze core dump
gdb ./build/ServerMain core
```

## Common debugging scenarios

### Scenario: Memory leak in join operation

```bash
# 1. Build with ASAN
cmake -DCMAKE_BUILD_TYPE=ASAN -GNinja ..

# 2. Run specific test
ASAN_OPTIONS=verbosity=2 ctest -R JoinTest --output-on-failure

# 3. Analyze output for memory errors
# 4. Add smart pointers or RAII cleanup
# 5. Verify leak is fixed
```

### Scenario: Performance regression

```bash
# 1. Benchmark baseline
time ./build/ServerMain < queries.sparql

# 2. Profile with perf
perf record -g ./build/ServerMain < queries.sparql

# 3. Identify hotspot
perf report

# 4. Optimize and re-benchmark
```

## Integration with tests

```bash
# Run all tests with ASAN
cmake -DCMAKE_BUILD_TYPE=ASAN -GNinja ..
ctest --output-on-failure

# Run specific test with profiling
CPUPROFILE=test.prof ctest -R JoinTest --output-on-failure
```
