cmake_minimum_required(VERSION 2.8.4)
project(QLever C CXX)

# C/C++ Versions
set (CMAKE_C_STANDARD 11)
set (CMAKE_C_STANDARD_REQUIRED ON)
set (CMAKE_CXX_STANDARD 17)
set (CMAKE_CXX_STANDARD_REQUIRED ON)

# Check compiler versions:
if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
  # Check version. If empty, warn. If too old, error out:
  if ("${CMAKE_CXX_COMPILER_VERSION}" STREQUAL "")
    message(WARNING "GCC Compiler version is unknown, proceed at your own risk!")
  elseif (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 4.8)
    message(FATAL_ERROR "GCC compiler version must be at least 4.8!")
  endif()
endif()

###############################################################################
##### Essential settings #####
###############################################################################

################################
# Threading
################################
find_package (Threads REQUIRED)

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
# set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}  -Wall -Wextra -Wno-missing-field-initializers -DGTEST_HAS_TR1_TUPLE=0 -DGTEST_USE_OWN_TR1_TUPLE=0")

if (${PERFTOOLS_PROFILER})
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -lprofiler")
    message(STATUS "Adding -lprofiler (make sure your have google-perftools installed.)")
endif()

if (${ALLOW_SHUTDOWN})
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DALLOW_SHUTDOWN")
    message(STATUS "Adding -DALLOW_SHUTDOWN")
endif()


set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG}")
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG}")

################################
# GTEST
################################
add_subdirectory(third_party/googletest/googletest)
include_directories(third_party/googletest/googletest/include)

################################
# NLOHNMANN-JSON
################################
# Header only, nothing to include
include_directories(third_party/json/)

################################
# STXXL
################################
# Disable GNU parallel as it prevents build on Ubuntu 14.04
set(USE_GNU_PARALLEL OFF CACHE BOOL "Don't use gnu parallel" FORCE)
set(USE_OPENMP OFF CACHE BOOL "Don't use OPENMP as default" FORCE)
add_subdirectory(third_party/stxxl)
# apply STXXL CXXFLAGS
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${STXXL_CXX_FLAGS}")
include_directories(${STXXL_INCLUDE_DIRS})

################################
# RE2
################################

# RE2 has a lot of unused-parameter warnings, we will deactivate
# these for the subproject
set(LOCAL_CXX_BACKUP_FLAGS "${CMAKE_CXX_FLAGS}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unused-parameter")

set(RE2_BUILD_TESTING OFF CACHE BOOL "enable testing for RE2" FORCE)
add_subdirectory(third_party/re2)
include_directories(third_party/re2)

# reinstate original flags including all warnings
set(CMAKE_CXX_FLAGS "${LOCAL_CXX_BACKUP_FLAGS}")

message(STATUS ---)
message(STATUS "CXX_FLAGS are : " ${CMAKE_CXX_FLAGS})
message(STATUS "CXX_FLAGS_RELEASE are : " ${CMAKE_CXX_FLAGS_RELEASE})
message(STATUS "CXX_FLAGS_DEBUG are : " ${CMAKE_CXX_FLAGS_DEBUG})
message(STATUS "IMPORTANT: Make sure you have selected the desired CMAKE_BUILD_TYPE")
message(STATUS ---)

###############################################################################
##### Actual project configuration #####
###############################################################################
set(LOG_LEVEL_FATAL 0)
set(LOG_LEVEL_ERROR 1)
set(LOG_LEVEL_WARN 2)
set(LOG_LEVEL_INFO 3)
set(LOG_LEVEL_DEBUG 4)
set(LOG_LEVEL_TRACE 5)


if(CMAKE_BUILD_TYPE MATCHES DEBUG)
  set(LOGLEVEL DEBUG CACHE STRING "The loglevel")
else()
  set(LOGLEVEL INFO CACHE STRING "The loglevel")
endif()
set_property(CACHE LOGLEVEL PROPERTY STRINGS FATAL ERROR WARN INFO DEBUG TRACE)
add_definitions(-DLOGLEVEL=${LOG_LEVEL_${LOGLEVEL}})

add_subdirectory(src/parser)
add_subdirectory(src/engine)
add_subdirectory(src/index)
enable_testing()
add_subdirectory(test)



configure_file(src/web/index.html index.html)
configure_file(src/web/style.css style.css)
configure_file(src/web/script.js script.js)


add_executable(IndexBuilderMain src/index/IndexBuilderMain.cpp)
target_link_libraries(IndexBuilderMain index ${CMAKE_THREAD_LIBS_INIT})

add_executable(CreatePatternsMain src/index/CreatePatternsMain.cpp)
target_link_libraries(CreatePatternsMain index ${CMAKE_THREAD_LIBS_INIT})

add_executable(SparqlEngineMain src/SparqlEngineMain.cpp)
target_link_libraries (SparqlEngineMain engine ${CMAKE_THREAD_LIBS_INIT})

add_executable(ServerMain src/ServerMain.cpp)
target_link_libraries (ServerMain engine ${CMAKE_THREAD_LIBS_INIT})

add_executable(WriteIndexListsMain src/WriteIndexListsMain.cpp)
target_link_libraries (WriteIndexListsMain engine ${CMAKE_THREAD_LIBS_INIT})

add_executable(MetaDataConverterMain src/MetaDataConverterMain.cpp)
target_link_libraries (MetaDataConverterMain metaConverter ${CMAKE_THREAD_LIBS_INIT})

add_executable(PrefixHeuristicEvaluatorMain src/PrefixHeuristicEvaluatorMain.cpp)
target_link_libraries (PrefixHeuristicEvaluatorMain index ${CMAKE_THREAD_LIBS_INIT})

add_executable(TurtleParserMain src/TurtleParserMain.cpp)
target_link_libraries(TurtleParserMain parser ${CMAKE_THREAD_LIBS_INIT})

add_executable(Bzip2WrapperMain src/parser/Bzip2WrapperMain.cpp)
target_link_libraries(Bzip2WrapperMain -lbz2)

#add_executable(TextFilterComparison src/experiments/TextFilterComparison.cpp)
#target_link_libraries (TextFilterComparison experiments)

