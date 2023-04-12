# Introduction
A quick introduction and tutorial for the macro benchmark infrastructure.

As of the `12.4.2023` the benchmark infrastructure has the following features:
- Measuring the execution time of a function in seconds.
- Organizing a benchmark as a single measurement, as a group of single measurements, or as a table of measurements.
- Printing the measured benchmarks and/or exporting them as a JSON formatted file.
- Adding metadata information to benchmarks. This information will not be printed, but it will be in an exported JSON file.
- Passing configuration options at runtime, which can be set either using a JSON file, or per shorthand in the CLI.

However, support for the prevention of compiler optimization in benchmarks is not available and still in the planning stage. This can sabotage measured execution times and should be kept in mind, while writing benchmarks.  
For example: An expression without a return type and without side effects will get optimized out. Like, for example, when you are trying to measure a `getter` function without using the returned value.

# How to write a basic benchmark
This will be a rather undetailed tutorial, because all the functions and classes have their own documentation, which I do not want to repeat.

For a quick hands-on example of the general usage and all features, see `benchmark/BenchmarkExamples.cpp`.

Larger collections of benchmarks, or even a single one, are organized into classes, that inherit from `BenchmarkInterface` in `benchmark/infrastructure/Benchmark.h`.  
Those class implementations should have their own `.cpp` file in the folder `benchmark`.

## Writing the class
To write your own class, first include `benchmark/infrastructure/Benchmark.h` in you file. It includes all needed classes, interfaces and types.  
Secondly, you should write your class inside the `ad_benchmark` namespace, where all benchmark infrastructure can be found.

Now, the interface for benchmark classes has 3 functions:
- `parseConfiguration`
- `getMetadata`
- `runAllBenchmarks`
`parseConfiguration` and `getMetadata` are for advanced features, and come with default implementations, that don't actually do anything. So they can be safely ignored for the time being.  

`runAllBenchmarks` is where you actually measure your functions using the classes of `BenchmarkMeasurementContainer.h`, which should be created using `BenchmarkResults`, who will save them and later pass them on for processing by the infrastructure.

Which could look like this:
```c++
BenchmarkResults runAllBenchmarks(){
  BenchmarkResults results{};

  // Just saves the meausred execution time.
  results.addMeasurement(...);

  // Creates an enpty group. Doesn't measure anything.
  auto& group = results.addGroup(...);
  // You add the measurements as group members.
  group.addMeasurement(...);

  // Creates an enpty table with a set number of rows and columns. Doesn't
  // measure anything.
  auto& table = results.addTable(...);
  // You can add measurements to the table as entries, but you can also
  // read and set entries.
  table.addMeasurement(...);
  table.setEntry(...);
  table.getEntry(...);

  return results;
}
```

Every `addMeasurement` function takes a function as an argument, which will be called without any arguments and the execution time measured. Most of the time, those functions will probably be lambda wrappers.

After writing your class, you will have to register it. For that, simply call the macro `AD_REGISTER_BENCHMARK` with your class name and all needed arguments for construction inside the `ad_benchmark` namespace.  
For example:
```c++
AD_REGISTER_BENCHMARK(MyClass, ConstructorArgument1, ConstructorArgument2, ...);
```

## CMake
Registering your finished benchmark class is rather easy.  
Simply add the line `addAndLinkBenchmark(MyBenchmarkClassFile)`, without the ending `.cpp`, to the file `benchmark/CMakeLists.txt`.  
It will now be compiled. The compiled version can be found inside the `benchmark` folder inside your build directory.

# Using compiled benchmarks
TODO

# Using advanced benchmark features
## Metadata
TODO

## Runtime configuration
TODO
