# A small cmake script that writes the current git hash and time to a
# .cpp file

# Get the current time, remove the trailing newline and add quotes.
execute_process(COMMAND date OUTPUT_VARIABLE DATETIME_OF_COMPILATION)
string(REPLACE "\n" "" DATETIME_OF_COMPILATION "${DATETIME_OF_COMPILATION}")
set(DATETIME_OF_COMPILATION "\"${DATETIME_OF_COMPILATION}\"")
message(STATUS "DATETIME_OF_COMPILATION is ${DATETIME_OF_COMPILATION}" )

# Get the current git hash.
execute_process(COMMAND git log -1 --format="%H" OUTPUT_VARIABLE GIT_HASH)
if ((NOT DEFINED GIT_HASH) OR (GIT_HASH STREQUAL ""))
    set(GIT_HASH "\"QLever compilation not taking place in a git repository\"")
endif()
message(STATUS "GIT_HASH is ${GIT_HASH}")

# Write the .cpp file.
set(CONSTANTS "#include <string>
namespace qlever::version {
const char* GitHash = ${GIT_HASH};
const char* DatetimeOfCompilation = ${DATETIME_OF_COMPILATION};
}")

# For some reason `CMAKE_CURRENT_SOURCE_DIR` inside this script is
# `CMAKE_CURRENT_BINARY_DIR` in the main `CMakeLists.txt`.
file(WRITE ${CMAKE_CURRENT_SOURCE_DIR}/CompilationInfo.cpp "${CONSTANTS}")
