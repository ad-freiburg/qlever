// Copyright 2011, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: mostly copied from CompleteSearch code, original author unknown.
// Adapted by: Björn Buchhold <buchholb>

#ifndef GLOBALS_EXCEPTION_H_
#define GLOBALS_EXCEPTION_H_

#include <exception>
#include <sstream>
#include <string>

using std::string;

// -------------------------------------------
// Macros for throwing exceptions comfortably.
// -------------------------------------------
// Throw exception with additional assert-like info
#define AD_THROW(e, m)                                               \
  {                                                                  \
    std::ostringstream __os;                                         \
    __os << m;                                                       \
    throw ad_semsearch::Exception(e, __os.str(), __FILE__, __LINE__, \
                                  __PRETTY_FUNCTION__);              \
  }  // NOLINT

// --------------------------------------------------------------------------
// Macros for assertions that will throw Exceptions.
// --------------------------------------------------------------------------
//! NOTE: Should be used only for asserts which affect the total running only
//! very insignificantly. Counterexample: don't use this in an inner loop that
//! is executed million of times, and has otherwise little code.
// --------------------------------------------------------------------------
// Needed for Cygwin (will be an identical redefine  for *nixes)
#define __STRING(x) #x
//! Custom assert which does not abort but throws an exception
#define AD_CHECK(condition)                                                  \
  {                                                                          \
    if (!(condition)) {                                                      \
      AD_THROW(ad_semsearch::Exception::ASSERT_FAILED, __STRING(condition)); \
    }                                                                        \
  }  // NOLINT
//! Assert equality, and show values if fails
#define AD_CHECK_EQ(t1, t2)                                           \
  {                                                                   \
    if (!((t1) == (t2))) {                                            \
      AD_THROW(ad_semsearch::Exception::ASSERT_FAILED,                \
               __STRING(t1 == t2) << "; " << (t1) << " != " << (t2)); \
    }                                                                 \
  }  // NOLINT
//! Assert less than, and show values if fails
#define AD_CHECK_LT(t1, t2)                                          \
  {                                                                  \
    if (!((t1) < (t2))) {                                            \
      AD_THROW(ad_semsearch::Exception::ASSERT_FAILED,               \
               __STRING(t1 < t2) << "; " << (t1) << " >= " << (t2)); \
    }                                                                \
  }  // NOLINT
//! Assert greater than, and show values if fails
#define AD_CHECK_GT(t1, t2)                                          \
  {                                                                  \
    if (!((t1) > (t2))) {                                            \
      AD_THROW(ad_semsearch::Exception::ASSERT_FAILED,               \
               __STRING(t1 > t2) << "; " << (t1) << " <= " << (t2)); \
    }                                                                \
  }  // NOLINT
//! Assert less or equal than, and show values if fails
#define AD_CHECK_LE(t1, t2)                                          \
  {                                                                  \
    if (!((t1) <= (t2))) {                                           \
      AD_THROW(ad_semsearch::Exception::ASSERT_FAILED,               \
               __STRING(t1 <= t2) << "; " << (t1) << " > " << (t2)); \
    }                                                                \
  }  // NOLINT
//! Assert greater or equal than, and show values if fails
#define AD_CHECK_GE(t1, t2)                                          \
  {                                                                  \
    if (!((t1) >= (t2))) {                                           \
      AD_THROW(ad_semsearch::Exception::ASSERT_FAILED,               \
               __STRING(t1 >= t2) << "; " << (t1) << " < " << (t2)); \
    }                                                                \
  }  // NOLINT

// -------------------------------------------
// Exception class code
// -------------------------------------------
namespace ad_semsearch {

//! Exception class for rethrowing exceptions during a query abort
//! such exceptions are never printed but still keep the original what()
//! message just in case
class AbortException : public std::exception {
 public:
  AbortException(const std::exception& original) : _origWhat(original.what()) {}

  const char* what() const noexcept { return _origWhat.c_str(); }

 private:
  string _origWhat;
};

//! Exception class for all kinds of exceptions.
//! Compatibility with the THROW macro is ensured by using error
//! codes inside this exception class instead of implementing an
//! exception hierarchy through inheritance.
//! This approach is taken from CompleteSearch's exception code.
//! Add error codes whenever necessary.
class Exception : public std::exception {
 private:
  //! Error code
  int _errorCode;

  //! Detailed information (beyond what the code already says,
  //! optionally provided by thrower)
  string _errorDetails;

  string _errorDetailsFileAndLines;

  string _errorMessageFull;

 public:
  //! Error codes
  //! They are always of type int, whereas the least 8 bits
  //! are used to distinguish exceptions inside a category,
  //! the more significant bits are used distinguish between
  //! categories. This idea is also taken from CompleteSearch
  //! and creates an artificial hierarchy.
  enum ExceptionType {
    // range errors
    VOCABULARY_MISS = 16 * 1 + 1,
    UNKNOWN_RELATION_ID = 16 * 1 + 2,

    // formatting errors
    BAD_INPUT = 16 * 2 + 5,
    BAD_REQUEST = 16 * 2 + 6,

    // memory allocation errors
    REALLOC_FAILED = 16 * 3 + 1,
    NEW_FAILED = 16 * 3 + 2,

    // query errors
    BAD_QUERY = 16 * 4 + 1,

    // history errors

    // (de)compression errors
    UNCOMPRESS_ERROR = 16 * 6 + 1,
    // multithreading-related
    COULD_NOT_GET_MUTEX = 16 * 7 + 1,
    COULD_NOT_CREATE_THREAD = 16 * 7 + 6,
    // socket related
    COULD_NOT_CREATE_SOCKET = 17 * 8 + 1,
    // general errors
    ASSERT_FAILED = 16 * 9 + 1,
    ERROR_PASSED_ON = 16 * 9 + 3,
    NOT_YET_IMPLEMENTED = 16 * 9 + 5,
    INVALID_PARAMETER_VALUE = 16 * 9 + 6,
    CHECK_FAILED = 16 * 9 + 7,
    // unknown error
    OTHER = 0
  };

  //! Error messages (one per code)
  static const string errorCodeAsString(int errorCode) {
    switch (errorCode) {
      case VOCABULARY_MISS:
        return "VOCABULARY MISS";
      case UNKNOWN_RELATION_ID:
        return "UNKNOWN_RELATION_ID: "
               "Trying to access a relation that is not present.";
      case BAD_INPUT:
        return "BAD INPUT STRING";
      case BAD_REQUEST:
        return "BAD REQUEST STRING";
      case BAD_QUERY:
        return "BAD QUERY";
      case REALLOC_FAILED:
        return "MEMORY ALLOCATION ERROR: Realloc failed";
      case NEW_FAILED:
        return "MEMORY ALLOCATION ERROR: new failed";
      case ERROR_PASSED_ON:
        return "PASSING ON ERROR";
      case UNCOMPRESS_ERROR:
        return "UNCOMPRESSION PROBLEM";
      case COULD_NOT_GET_MUTEX:
        return "MUTEX EXCEPTION: "
               "Could not get lock on mutex";
      case COULD_NOT_CREATE_THREAD:
        return "Error creating thread";
      case COULD_NOT_CREATE_SOCKET:
        return "SOCKET ERROR: could not create socket";
      case ASSERT_FAILED:
        return "ASSERT FAILED";
      case NOT_YET_IMPLEMENTED:
        return "NOT YET IMPLEMENTED";
      case INVALID_PARAMETER_VALUE:
        return "INVALID PARAMETER VALUE";
      case CHECK_FAILED:
        return "CHECK FAILED";
      case OTHER:
        return "ERROR";
      default:
        std::ostringstream os;
        os << "UNKNOWN ERROR: Code is " << errorCode;
        return os.str().c_str();
    }
  }

  //! Constructor (code only)
  explicit Exception(int errorCode) {
    _errorCode = errorCode;
    _errorDetails = "";
    _errorDetailsFileAndLines = "";
    _errorMessageFull = errorCodeAsString(_errorCode);
  }

  //! Constructor (code + details)
  Exception(int errorCode, const string& errorDetails) {
    _errorCode = errorCode;
    _errorDetails = errorDetails;
    _errorDetailsFileAndLines = "";
    _errorMessageFull = getErrorMessage() + " (" + _errorDetails + ")";
  }

  //! Constructor
  //! (code + details + file name + line number + enclosing method)
  Exception(int errorCode, const string& errorDetails, const string& file_name,
            int line_no, const string& fct_name) {
    _errorCode = errorCode;
    _errorDetails = errorDetails;
    _errorDetailsFileAndLines = "in " + file_name + ", line " +
                                std::to_string(line_no) + ", function " +
                                fct_name;
    _errorMessageFull = getErrorMessage() + " (" + getErrorDetails() + ")";
  }

  //! Set error code
  void setErrorCode(int errorCode) { _errorCode = errorCode; }

  //! Set error details
  void setErrorDetails(const string& errorDetails) {
    _errorDetails = errorDetails;
  }

  //! Get error Code
  int getErrorCode() const noexcept { return _errorCode; }

  //! Get error message pertaining to code
  string getErrorMessage() const noexcept {
    return errorCodeAsString(_errorCode);
  }

  //! Get error details
  const string getErrorDetails() const noexcept {
    return _errorDetails + "; " + _errorDetailsFileAndLines;
  }

  const string getErrorDetailsNoFileAndLines() const noexcept {
    return _errorDetails;
  }

  const string& getFullErrorMessage() const noexcept {
    return _errorMessageFull;
  }

  const char* what() const noexcept { return _errorMessageFull.c_str(); }
};
}  // namespace ad_semsearch

#endif  // GLOBALS_EXCEPTION_H_
