A short crash course on how to create benchmarks and use them.

# General top level information about the infrastructure
- All general benchmark infrastructure can be found in `Benchmark.h`.

- You can define single measurments, groups and even tables for holding the times of your benchmarks.

- A benchmark function is defined as a function, which sets up everything needed for calling the function, which execution time you wish to measure, and then passes the call wrapped into a lambda to a function of the object `BenchmarkRecords`. For more details, see 'How to write a new benchmark'.

- A benchmark file can hold as many benchmarks, as you want. This is useful for grouping similar benchmarks together.  

- Currently, all benchmark are recorded in seconds.

# How to write a new benchmark
This section is split into 2 parts:
1. Writing a new benchmark file.
2. Configuring `CMakeLists.text` for CMake.

For an example of all avaible functionality, see `BasicJoinBenchmark.cpp`.

## Writing the benchmark

First of all, your new benchmark file need to include `Benchmark.h`, which holds all of the benchmark infrastructure.

```cpp
#include "../benchmark/Benchmark.h"
```

Next, you write a benchmark function, that is to say, a function, with the return type `void` and one parameter of type `BenchmarkRecords*`.  
The structure of this function should look like this:

```cpp
void BM_function(BenchmarkRecords* records){
  // Setup needed for the function/process, you wish to measure.
  ...
  // The function call, or process, wrapped into a lambda, that takes and
  // returns nothing
  auto lambdaWrapper = [&]() {
    // Your function call, or process.
  };
  records->addSingleMeasurement("A description, or name, so that you can later recognize your benchmark", lambdaWrapper);
}
```

Now, a single measurment is exactly what it sounds like: A single benchmark measured.  
However, often enough you want to group similiar benchmarks in one place for ease of access and reading. For those, the benchmark infrastructure also provides groups and tables. Both of those have to be created, before you can add benchmarks to them.

Groups are defined as groups of single measurments and work like this:

```cpp
void BM_function(BenchmarkRecords* records){
  // Setup needed for the functions/processes, you wish to measure.
  ...
  // The function calls, or processes, wrapped into a lambda, that takes and
  // returns nothing
  auto lambdaWrapper1 = [&]() {
    // Your function call, or process.
  };
  auto lambdaWrapper2 = [&]() {
    // Your function call, or process.
  };
  // A group has to be created with an unique identifier. Multiple groups with
  // the same identifier are not allowed.
  records->addGroup("Unique descriptor");
  // Now you can add as many entries as you want.
  records->addToExistingGroup("Unique descriptor",
    "A descriptor for your entry", lambdaWrapper1);
  records->addToExistingGroup("Unique descriptor",
    "A descriptor for your entry", lambdaWrapper2);
}
```

Tables are also created with an unique descriptor, but they also need names for their rows and columns. These names don't have to be unique, because adding entries is done using `(row, column)`-coordinates, with the size of the table indirectly set by the amount of names for rows and columns.

```cpp
void BM_function(BenchmarkRecords* records){
  // Setup needed for the functions/processes, you wish to measure.
  ...
  // The function calls, or processes, wrapped into a lambda, that takes and
  // returns nothing
  auto lambdaWrapper1 = [&]() {
    // Your function call, or process.
  };
  auto lambdaWrapper2 = [&]() {
    // Your function call, or process.
  };
  // A table has to be created with an unique identifier. Multiple tables with
  // the same identifier are not allowed.
  records->addTable("Unique descriptor", {"row 1", "row 2"}, {"column 1"});
  // Now you can add entries to the table. These don't need a descriptor
  // , because they are just table entries
  records->addToExistingTable("Unique descriptor",
    0, 0, lambdaWrapper1);
  records->addToExistingTable("Unique descriptor",
    1, 0, lambdaWrapper2);
}
```

Next, you have to register this benchmark function. This is currently a bit ugly, but easily done. Simply create a global variable of type `BenchmarkRegister` and give the constructor an array of the of the benchmark functions, you wish to register.  
This can be done as many times as you want. Additionally variables of this type are more like dummies, and don't have any non static member functions, or non static member variables. So, they should be very small in terms of memory.
An example registration:

```cpp
BenchmarkRegister temp{{BM_function}};
```

## Configuring CMake

`CMakeLists.txt` contains a function for compiling a benchmark.  

```
# Compile the benchmark and link it, but do not run it.
# Usage: addAndLinkBenchmark(baseName, [additionalLibraries...])
#   - baseName: The name of the benchmark file without the .cpp ending.
function(addAndLinkBenchmark baseName)
```

Simply call it with your filename, without the `.cpp` ending, and all the librariers you need. CMake should take care of the rest.  
Example:

```
# ExampleBenchmark is the benchmark file, and engine a library.
addAndLinkBenchmark(ExampleBenchmark engine)
```

# Compiling and running a benchmark

Compile everything like normal. There are no additional steps for benchmarks required.  
If everything compiled correctly, there should be a new `benchmark` directory in your build directory, containing the compiled benchmarks.  

A compiled benchmark file runs all registerd benchmark functions inside it and measure the benchmarks in seconds. The result can be printed to the terminal, and/or written to a file in json. For more details, just run the compiled file with `--help`.
