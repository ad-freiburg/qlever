# Link binary ${basename} against the threading library,
# and all libraries that are specified as additional arguments.
function (linkBenchmark basename)
    target_link_libraries(${basename} ${ARGN} ${CMAKE_THREAD_LIBS_INIT})
endfunction()

# Add the exectutable ${basename} that is compiled from the source file
# "${basename}".cpp
function (addBenchmark basename)
    add_executable(${basename} "${basename}.cpp")
endfunction()

# Compile and link the benchmark, but do not run it.
# Usage: addAndLinkBenchmark(basename, [additionalLibraries...]
function(addAndLinkBenchmark basename)
    addBenchmark(${basename})
    linkBenchmark(${basename} ${ARGN})
endfunction()

# Add benchmarks after here.

addAndLinkBenchmark(BasicJoinBenchmark engine)
