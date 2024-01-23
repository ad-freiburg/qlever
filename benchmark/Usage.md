# Introduction

A quick introduction and tutorial for the macro benchmark infrastructure.

As of July 2023 the benchmark infrastructure has the following features:

- Measuring the execution time of a function in seconds.
- Organizing a benchmark as a single measurement, as a table of measurements, or as a group of single measurements and tables.
- Printing the measured benchmarks and/or exporting them as a JSON formatted file.
- Adding metadata information to benchmarks.
- Passing values at runtime via pre-defined configuration options, which can be set either using a JSON file, or per shorthand in the CLI.

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
- `getGeneralMetadata`
- `runAllBenchmarks`
- `getConfigManager`
- `updateDefaultGeneralMetadata`  

`name` should just return the name of your benchmark class, so that you can easily identify it later.

`getGeneralMetadata` and `getConfigManager`are getters for member variables, that are used for advanced features. So they can be safely ignored for the time being.

`updateDefaultGeneralMetadata` exists solely for the infrastructure and should be ignored.

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

  /*
  Create an empty table with a number of rows and columns. Doesn't measure anything.
  The number of columns can not be changed after creation, but the number of rows can.
  Important: The row names aren't saved in a seperate container, but INSIDE the
  first column of the table.
  */
  auto& table = results.addTable(identifier, {"rowName1", "rowName2", "etc."},
    {"Column for row names", "columnName1", "columnName2", "etc."});

  // You can add measurements to the table as entries, but you can also
  // read and set entries.
  table.addMeasurement(0, 2, dummyFunctionToMeasure); // Row 0, column 1.
  table.setEntry(0, 1, "A custom entry can be any type in 'ad_benchmark::ResultTable::EntryType', except 'std::monostate'.");
  table.getEntry(0, 2); // The measured time of the dummy function.

  // Replacing a row name.
  table.setEntry(0, 0, "rowName1++");

  /*
  Creates an empty group. Doesn't measure anything, but groups and tables can be
  added to better organize them.
  */
  auto& group = results.addGroup(identifier);
  // Normal measurement.
  group.addMeasurement(identifier, dummyFunctionToMeasure);
  // Table.
  group.addTable(identifier, {"rowName1", "rowName2", "etc."},
    {"Column for row names", "columnName1", "columnName2", "etc."});


  return results;
}
```

After writing your class, you will have to register it. For that, simply call the macro `AD_REGISTER_BENCHMARK` with your class name and all needed arguments for construction inside the `ad_benchmark` namespace.  
For example:

```c++
AD_REGISTER_BENCHMARK(MyClass, ConstructorArgument1, ConstructorArgument2, ...);
```

## CMake

Registering your finished benchmark class with CMake is rather easy.  
Simply add the line `addAndLinkBenchmark(MyBenchmarkClassFile)`, without the ending `.cpp`, to the file `benchmark/CMakeLists.txt`.  
It will now be compiled. The compiled version can be found inside the `benchmark` folder inside your build directory.

# Using advanced benchmark features

## Metadata

Setting metadata is handled by the `BenchmarkMetadata` class. The set metadata information will be included in the printed output of a compiled benchmark file and in the JSON file export.

You can find instances of `BenchmarkMetadata` for your usage at 4 locations:

- At `metadata()` of created `ResultEntry` objects, in order to give metadata information about the benchmark measurement.

- At `metadata()` of created `ResultGroup` objects, in order to give metadata information about the group.

- At `metadata()` of created `ResultTable` objects, in order to give metadata information about the table.

- In your own class, under the getter `getGeneralMetadata()`. The returned member variable exists in order to give more general metadata information about your benchmark class. This is mostly, so that you don't have to constantly repeat metadata information, that are true for all the things you are measuring, in other places. For example, this would be a good place to give the name of an algorithm, if your whole benchmark class is about measuring the runtimes of one.

## Runtime configuration

Passing values to your benchmark at runtime is possible and has its own system, split up into two phases.  
Defining the configuration options and passing values to them.

### Adding options

Adding configuration options is done by adding configuration option to the private member variable `manager_`, accessible via a getter, by using the function `ConfigManager::addOption`. That is best done in the constructor of your class.

In our system a configuration option is described by a handful of characteristics:

1. The name of the option.

2. A description of the option.

3. If it has a default value. If it hasn't, people will always have to provide their own value at run time.

4. What **type** of values it takes. The following types are available:
   
   - `bool`.
   - `std::string`.
   - `int`.
   - `size_t`
   - `float`.
   - A `std::vector` of the previous options.

However, unlike the default value, the value it takes, isn't saved internally.  

Instead, it takes a pointer to a variable of the type, that it itself takes, at construction. Whenever `ConfigOption` gets set to a value, the variable, for which the pointer was passed, is set to that value.

Note, that also happens at the time of creation, if a default value was given.

In order to organize `ConfigOption`s easier, `ConfigManager` uses JSON like paths, but made up entirely of strings, for identification. Those are defined at the time of creation of the option and can't be changed later.

### Passing values

Setting the values of the configuration options at runtime can be done in two ways:

1. Writing a JSON file and passing the file location via CLI.

2. Using the shorthand described in `src/util/ConfigManager/generated/ConfigShorthand.g4`, by writing it directly as an argument via CLI. Note: The shorthand will overwrite the value of any configuration option, if both ways try to set it.

The shorthand is basically just normal JSON, but adjusted for easier usage. There are 3 big changes.  

First, there are no line breaks allowed. The shorthand is build for usage directly in the CLI, so that is an unneeded feature

Second, because a configuration is always represented by a JSON object, a shorthand string is always treated, as if it had `{}` braces at the beginning and end.  

Third, the keys of key-value pairs, for example `"key" : value`, don't need to be surrounded with `"`. `"` is a special symbol in the CLI, and we want to save you the extra work of always typing `\"key\"`.

Using those two ways of passing information, the configuration options held by an internally created `ConfigManager` object, will be set.

In both of those, you have to write out the complete path to your configuration option and write the value, you wish to set it to, at the end.  
For example: Let's say, you defined a configuration option `someNumber` and added it with the path `tableSizes/someNumber`. Then, if you wanted to set it to `20` using JSON, you would have to write:

```json
{"tableSizes": "someNumber": 20}
```

However, **if** the passed values can't be interpreted as the correct types for the configuration options, an exception will be thrown.
