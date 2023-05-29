# Introduction
A quick introduction and tutorial for the macro benchmark infrastructure.

As of April 2023 the benchmark infrastructure has the following features:
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

Now, the interface for benchmark classes has 5 functions:
- `name`
- `addConfigurationOptions`
- `parseConfiguration`
- `getMetadata`
- `runAllBenchmarks`  

`addConfigurationOptions`, `parseConfiguration` and `getMetadata` are for advanced features, and come with default implementations, that don't actually do anything. So they can be safely ignored for the time being.  

`name` should just return the name of your benchmark class, so that you can easily identify it later.

`runAllBenchmarks` is where you actually measure your functions using the classes of `BenchmarkMeasurementContainer.h`, which should be created using `BenchmarkResults`, who will save them and later pass them on for processing by the infrastructure.
Which could look like this:

```c++
BenchmarkResults runAllBenchmarks(){
  BenchmarkResults results{};

  /*
  The functions for measuring the execution time of functions, only take
  lambdas, which will be called without any arguments. Simply wrap the actual
  function call, you want to measure.
  */
  auto& dummyFunctionToMeasure = [](){
    // Do whatever you might want to measure.
  };

  // In order to recognise later, which time belongs to which benchmark,
  // most of the functions concerning the measurement of functions, or their
  // organization, require an identifier. A.k.a. a name.
  const std::string identifier = "Some identifier";

  // Just saves the measured execution time.
  results.addMeasurement(identifier, dummyFunctionToMeasure);

  // Creates an enpty group. Doesn't measure anything.
  auto& group = results.addGroup(identifier);
  // You add the measurements as group members.
  group.addMeasurement(identifier, dummyFunctionToMeasure);

  /*
  Create an empty table with a set number of rows and columns. Doesn't
  measure anything.
  Important: The row names aren't saved in a seperate container, but INSIDE the
  first column of the table.
  */
  auto& table = results.addTable(identifier, {"rowName1", "rowName2", "etc."},
    {"Column for row names", "columnName1", "columnName2", "etc."});

  // You can add measurements to the table as entries, but you can also
  // read and set entries.
  table.addMeasurement(0, 2, dummyFunctionToMeasure); // Row 0, column 1.
  table.setEntry(0, 1, "A custom entry can be a float, or a string.");
  table.getEntry(0, 2); // The measured time of the dummy function.

  // Replacing a row name.
  table.setEntry(0, 0, "rowName1++");

  return results;
}
```

After writing your class, you will have to register it. For that, simply call the macro `AD_REGISTER_BENCHMARK` with your class name and all needed arguments for construction inside the `ad_benchmark` namespace.  
For example:
```c++
AD_REGISTER_BENCHMARK(MyClass, ConstructorArgument1, ConstructorArgument2, ...);
```

## CMake
Registering your finished benchmark class is rather easy.  
Simply add the line `addAndLinkBenchmark(MyBenchmarkClassFile)`, without the ending `.cpp`, to the file `benchmark/CMakeLists.txt`.  
It will now be compiled. The compiled version can be found inside the `benchmark` folder inside your build directory.

# Using advanced benchmark features
## Metadata
Setting metadata is handled by the `BenchmarkMetadata` class. The set metadata information will be included in the printed output of a compiled benchmark file and in the JSON file export.

You can find instances of `BenchmarkMetadata` for your usage at 4 locations:
- At `metadata_` of created `ResultEntry` objects, in order to give metadata information about the benchmark measurement.
- At `metadata_` of created `ResultGroup` objects, in order to give metadata information about the group.
- At `metadata_` of created `ResultTable` objects, in order to give metadata information about the table.
- Writing a `getMetadata` function, like in the `BenchmarkInterface`, in order to give more general metadata information about your benchmark class. This is mostly, so that you don't have to constantly repeat metadata information, that are true for all the things you are measuring, in other places. For example, this would be a good place to give the name of an algorithm, if your whole benchmark class is about measuring the runtimes of one. Or you could give the time, at which those benchmark measurements were taken.

## Runtime configuration
Passing values to your benchmark at runtime is possible and has its own system, split up into two phases.  
Adding the configuration options and passing values to them.

### Adding options
Adding configuration options is done with the `addConfigurationOptions` function. More specifically, it's done by adding `BenchmarkConfigurationOption` objects to the passed instance of `BenchmarkConfiguration`, using the apt named `addConfigurationOption` function. These `BenchmarkConfigurationOption` objects are created using the `makeBenchmarkConfigurationOption` function.

A `BenchmarkConfigurationOption` describes four things about a configuration option:

1. What **type** of values it takes. The following types are available:
  - Boolean.
  - `std::string`.
  - Integer.
  - Floating point.
  - A `std::vector` of the previous options.

2. The name of the option.

3. A description of the option.

4. If it has a default value. If it hasn't, people will always have to provide their own value at run time.

Additionally, to those four properties, you can also add a path before your configuration option, when adding it with `addConfigurationOption`. This is mainly for organizational purpose on your end and works like in JSON.

### Passing values
Setting the values of the configuration options at runtime can be done in two ways:

1. Writing a JSON file and passing the file location via CLI.

2. Using the shorthand described in `benchmark/infrastructure/generated/BenchmarkConfigurationShorthand.g4`, by writing it directly as an argument via CLI. Note: The shorthand will overwrite the value of any configuration option, if both ways try to set it..

The shorthand is basically just normal JSON, but adjusted for easier usage. There are 3 big changes.  
First, there are no line breaks allowed. The shorthand is build for usage directly in the CLI, so that is an unneeded feature
Second, because a configuration is always represented by a JSON object, a shorthand string is always treated, as if it had `{}` braces at the beginning and end.  
Third, the keys of key-value pairs, for example `"key" : value`, don't need to be surrounded with `"`. `"` is a special symbol in the CLI, and we want to save you the extra work of always typing `\"key\"`.

Using those two ways of passing information, the configuration options held by an internally created `BenchmarkConfiguration` object, will be set.

In both of those, you have to write out the complete path to your configuration option and write the value, you wish to set it to, at the end.  
For example: Let's say, you defined a configuration option `someNumber` and added it with the path `tableSizes/3`. Then, if you wanted to set it to `20` using JSON, you would have to write:
```json
{"tableSizes": [..., {"someNumber": 20}]}
````

However, **if** the passed values can't be interpreted as the correct types for the configuration options, an exception will be thrown.

The so created and set `BenchmarkConfiguration` can be read by your class, by having a `parseConfiguration` function, as described in `BenchmarkInterface`. In this function, you can read out and interpret values using `BenchmarkConfiguration::getConfigurationOptionByNestedKeys`.
