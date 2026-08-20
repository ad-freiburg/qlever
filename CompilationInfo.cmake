# A small cmake script that writes the current git hash, project version,
# and time to a .cpp file

# Get the current time, remove the trailing newline and add quotes.
execute_process(COMMAND date OUTPUT_VARIABLE DATETIME_OF_COMPILATION)
string(REPLACE "\n" "" DATETIME_OF_COMPILATION "${DATETIME_OF_COMPILATION}")
set(DATETIME_OF_COMPILATION "\"${DATETIME_OF_COMPILATION}\"")
message(STATUS "DATETIME_OF_COMPILATION is ${DATETIME_OF_COMPILATION}" )

# Get the current time as a Unix timestamp (seconds since the epoch).
execute_process(COMMAND date +%s OUTPUT_VARIABLE TIME_OF_COMPILATION_UNIX)
string(REPLACE "\n" "" TIME_OF_COMPILATION_UNIX "${TIME_OF_COMPILATION_UNIX}")
set(TIME_OF_COMPILATION_UNIX "\"${TIME_OF_COMPILATION_UNIX}\"")

# The compiler and C++ standard used, forwarded from the main `CMakeLists.txt`.
set(COMPILER "\"${CXX_COMPILER_ID}\"")
set(COMPILER_VERSION "\"${CXX_COMPILER_VERSION}\"")
set(CXX_STANDARD "\"${CXX_STANDARD}\"")
message(STATUS "COMPILER is ${COMPILER} ${COMPILER_VERSION}, C++ standard is ${CXX_STANDARD}")

# Get the current git hash.
execute_process(COMMAND git log -1 --format="%H" OUTPUT_VARIABLE GIT_HASH)
if ((NOT DEFINED GIT_HASH) OR (GIT_HASH STREQUAL ""))
    set(GIT_HASH "\"QLever compilation not taking place in a git repository\"")
endif()
message(STATUS "GIT_HASH is ${GIT_HASH}")

# Get the project version from git describe.
include(${CMAKE_CURRENT_LIST_DIR}/GitVersion.cmake)
set(PROJECT_VERSION "\"${PROJECT_VERSION}\"")
message(STATUS "PROJECT_VERSION is ${PROJECT_VERSION}")

# Write the .cpp file.
set(CONSTANTS "#include \"CompilationInfo.h\"
namespace qlever::version {
constexpr std::string_view GitHash = ${GIT_HASH};
constexpr std::string_view GitShortHash = GitHash.substr(0, 7);
constexpr std::string_view DatetimeOfCompilation = ${DATETIME_OF_COMPILATION};
constexpr std::string_view TimeOfCompilationUnix = ${TIME_OF_COMPILATION_UNIX};
constexpr std::string_view ProjectVersion = ${PROJECT_VERSION};
constexpr std::string_view Compiler = ${COMPILER};
constexpr std::string_view CompilerVersion = ${COMPILER_VERSION};
constexpr std::string_view CxxStandard = ${CXX_STANDARD};

void copyVersionInfo() {
  *gitShortHashWithoutLinking.wlock() = GitShortHash;
  *datetimeOfCompilationWithoutLinking.wlock() = DatetimeOfCompilation;
  *timeOfCompilationUnixWithoutLinking.wlock() = TimeOfCompilationUnix;
  *projectVersionWithoutLinking.wlock() = ProjectVersion;
  *compilerWithoutLinking.wlock() = Compiler;
  *compilerVersionWithoutLinking.wlock() = CompilerVersion;
  *cxxStandardWithoutLinking.wlock() = CxxStandard;
}
}")

# For some reason `CMAKE_CURRENT_SOURCE_DIR` inside this script is
# `CMAKE_CURRENT_BINARY_DIR` in the main `CMakeLists.txt`.
file(WRITE ${CMAKE_CURRENT_SOURCE_DIR}/CompilationInfo.cpp "${CONSTANTS}")
