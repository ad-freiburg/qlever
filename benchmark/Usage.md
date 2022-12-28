A short crash course on how to create benchmarks and use them.

# General top level information about the infrastructure
- All general benchmark infrastructure can be found in `Benchmark.h`.  

- A benchmark is currently defined as a function, which sets up everything needed for calling the function, which execution time you wish to measure, and then passes the call wrapped into a lambda to a function of the object `BenchmarkRecords`. For more details, see 'How to write a new benchmark'.

- A benchmark file can hold as many benchmarks, as you want. This is useful for grouping similar benchmarks together.  

- Currently, all benchmark are recorded in seconds.

# How to write a new benchmark
This section is split into 2 parts:
1. Writing a new benchmark file.
2. Configuring `CMakeLists.text` for CMake.

For a more concrete example, see `BasicJoinBenchmark.cpp`.

## Writing the benchmark

First of all, your new benchmark file need to include `Benchmark.h`, which holds all of the benchmark infrastructure.

```cpp
#include "../benchmark/Benchmark.h"
```

Next, you write a benchmark, that is to say, a function, with the return type `void` and one parameter of type `BenchmarkRecords*`.  
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
  records->measuredTime("A description, or name, so that you can later recognize your benchmark", lambdaWrapper);
}
```

Next, you have to register this benchmark function. This is currently a bit ugly, but easily done. Simply create a global variable of type `BenchmarkRegister` and give the constructor an array of the addresses of the benchmark functions, you wish to register.  
This can be done as many times as you want. Additionally variables of this type are more like dummies, and don't have any non static member functions, or non static member variables. So, they should be very small in terms of memory.
An example registration:

```cpp
BenchmarkRegister temp{{BM_function}};
```

Now, this part is a bit of a hybrid with the CMake part and exist more for informationen, then for need. It can safely be skipped.  
As you probably already have noticed, there was no talk about any main function. This is because, I do not really know, what people want to do with their measured times. So I decided to keep it modular.  
Main functions are written each in their own file, and can be linked using CMake. This is to make it possible for the processing of the benchmarks to be, whatever you want, and to keep it a matter of configuration, seperated from the `C++` layer.  
Future plans include a collection of default processing behaviour, of which currently only one exists: A simple printing of the benchmark times with their descriptions.  
If you want to write your own main function, see the section further below for a basic overview.

## Configuring CMake

`CMakeLists.txt` contains a bunch of functions, to make the configuration of new benchmark files easier.  
There should be a function for each main function file, to link and compile with.  
For example:

```
# Compiles with a main function, thas just prints the benchmark times and the description of the benchmark.
addAndLinkBenchmarkWithPrint(...)
```

Simply call it with your filename, without the `.cpp` ending, and all the librariers you need. CMake should take care of the rest.  
Example:

```
# BasicJoinBenchmark is the benchmark file, and engine a library.
addAndLinkBenchmarkWithPrint(BasicJoinBenchmark engine)
```

**Important:** Do **not** change the order of the files in the `add_executable` call. `Benchmark.cpp` needs to be the first source, otherwise the static member variable of `BenchmarkRegister` in `Benchmark.h` doesn't behave as it should. (It seems to reset to empty, once reaching the main function, if `BenchmarkRegister` isn't the first source.)  
At least, that seemed to be my problem. Feel free to rewrite things, if I'm mistaken, or if I have been overlooking a simpler solution.

# Writing and linking a new main function

Writing a new main function, in other words, creating a new benchmark processing behaviour, is very simple.  
First, write a new `.cpp` file, include `Benchmark.h` and write an empty main function. A header for the file is not needed, most of the time.

```cpp
#include "../benchmark/Benchmark.h"

int main() {
};
```

Next, you have to create a `BenchmarkRecords`-object. This is used for measuring the time taken by the benchmarks and for saving those values.  
There are no setting or similar for the object, so that is simple.  

Next, go through the registered benchmark functions and call them, while passing your `BenchmarkRecords`-object. You can get the registered benchmark functions as a const vector, by calling `BenchmarkRegister::getRegisterdBenchmarks()`.  
Example:

```cpp
#include "../benchmark/Benchmark.h"

int main() {
  BenchmarkRecords benchmarksTime;
 
  for (const auto& benchmarkFunction: BenchmarkRegister::getRegisterdBenchmarks()) {
    benchmarkFunction(&benchmarksTime);
  }
};
```

And that is everything needed for the standard part.  
After this part, you have all benchmark measured and can look their information up, by looking into the array returned by calling `getRecords()` on your `BenchmarkRecords`-object.  
Now you can write you new processing behaviour with those information and do whatever you want. For an example, see `MainPrintTimes.cpp`.  

After writing your new benchmark processing behaviour, in order to use it, you will also have to write a new function in `CMakeLists.txt`.  
I recommend looking at the declarations at the end of the file, in order to find an already existing function for compiling with a main function file, going to the definition of that function and copying it.  
Then simply change the name, doc string and the file of the main function in `add_executable`, and everything should be set.  
For example:

```
# Compile the benchmark with MainPrintTimes and link the benchmark, but
# do not run it.
# Usage: addAndLinkBenchmark(basename, [additionalLibraries...]
function(addAndLinkBenchmarkWithPrint basename)
    add_executable(${basename}  Benchmark.cpp "${basename}.cpp" MainPrintTimes.cpp)
    linkBenchmark(${basename} ${ARGN})
endfunction()
```

Then you can add benchmarks with it, like all the other benchmarks entry in the file and everything *should* run smoothly.

# Compiling and running a benchmark

Compile everything like normal. There are no additional steps for benchmarks required.  
If everything compiled correctly, there should be a new `benchmark` directory in your build directory, containing the compiled benchmarks.  

Simply start the compiled benchmark (with CLI), in order to run it. Visual output and similar things are the job of the corresponding main function.
