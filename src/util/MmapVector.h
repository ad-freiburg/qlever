// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (joka921) <johannes.kalmbach@gmail.com>

#pragma once

#include <string>

#include <exception>
#include <fstream>
#include <sstream>
#include "../util/Exception.h"

using std::string;

namespace ad_utility {
// _________________________________________________________________________
class UninitializedArrayException : public std::exception {
  const char* what() const throw() {
    return "Tried to access a DiskBasedArray which was closed or "
           "uninitialized\n";
  }
};

// _________________________________________________________________________
class InvalidFileException : public std::exception {
  const char* what() const throw() {
    return "Error reading meta data of  Mmap file: Maybe magic number is "
           "missing or there is a version mismatch\n";
  }
};

// _________________________________________________________________________
class TruncateException : public std::exception {
 public:
  // _______________________________________________________________________
  TruncateException(const std::string& file, size_t size, int err) {
    std::stringstream stream;
    stream << "truncating of file " << file << " to size " << size
           << "set errno to" << err << " terminating\n";
    _msg = stream.str();
  }

  // ______________________________________________________________________________________________________
  const char* what() const throw() { return _msg.c_str(); }

  std::string _filename;
  size_t _bytesize;
  int _errno;
  mutable std::string _msg;
};

// ________________________________________________________________
struct VecInfo {
  size_t _capacity;
  size_t _bytesize;
};

// 2 tags to differentiate between different versions of the
// setup / construction variants of Mmap Vector which only take a filename
class CreateTag {};
class ReuseTag {};

// Enum that specifies access patterns to this array
enum class AccessPattern { None, Random, Sequential };

// STL-like class which implements a dynamic array (similar to std::vector)
// whose contents are stored persistently in a file on memory and are accessed
// using memory mapping
template <class T>
class MmapVector {
 public:
  using const_iterator = const T*;
  using iterator = T*;

  // __________________________________________________________________
  size_t size() const { return _size; }

  // __________________________________________________________________
  size_t capacity() const { return _capacity; }

  // standard iterator functions, each in a const and non-const version
  // begin
  T* begin() { return _ptr; }
  const T* begin() const { return _ptr; }
  // end
  T* end() { return _ptr + _size; }
  const T* end() const { return _ptr + _size; }
  // cbegin and cend
  const T* cbegin() const { return _ptr; }
  const T* cend() const { return _ptr + _size; }

  // Element access
  // without bounds checking
  T& operator[](size_t idx) {
    throwIfUninitialized();
    return _ptr[idx];
  }
  const T& operator[](size_t idx) const {
    throwIfUninitialized();
    return *(_ptr + idx);
  }

  // with bounds checking
  T& at(size_t idx) {
    throwIfUninitialized();
    if (idx >= _size) {
      throw std::out_of_range("call to MmapVector::at with idx >= size");
    }
    return *(_ptr + idx);
  }

  // _____________________________________________________________
  const T& at(size_t idx) const {
    throwIfUninitialized();
    if (idx >= _size) {
      throw std::out_of_range("call to MmapVector::at with idx >= size");
    }
    return *(_ptr + idx);
    AD_CHECK(idx < _size);
  }

  // _____________________________________________________________
  std::string getFilename() const { return _filename; }

  // we will never have less than this capacity
  static constexpr size_t MinCapacity = 100;

  // Default constructor. This array is invalid until initialized by one of the
  // setup methods;
  MmapVector() = default;
  virtual ~MmapVector();

  // no copy construction/assignment since we exclusively handle our mapping
  MmapVector(const MmapVector<T>&) = delete;
  MmapVector& operator=(const MmapVector<T>&) = delete;

  // move construction and assignment
  MmapVector(MmapVector<T>&& other) noexcept;
  MmapVector& operator=(MmapVector<T>&& other) noexcept;

  // default construct and open
  // all the arguments are passed to the call to open()
  template <typename... Args>
  MmapVector(Args&&... args) : MmapVector() {
    open(std::forward<Args>(args)...);
  }

  // create Array of given size  fill with default value
  // file contents will be overriden if existing!
  // allows read and write access
  void open(size_t size, const T& defaultValue, string filename,
            AccessPattern pattern = AccessPattern::None);

  // create unititialized array of given size at path specified by filename
  // file will be created or overriden!
  // allows read and write access
  void open(size_t size, string filename,
            AccessPattern pattern = AccessPattern::None);

  // create from given Iterator range
  // It must be an iterator type whose value Type must be convertible to T
  // TODO<joka921>: use enable_if or constexpr if or concepts/ranges one they're
  // out
  template <class It>
  void open(It begin, It end, const string& filename,
            AccessPattern pattern = AccessPattern::None);

  // _____________________________________________________________________
  void open(string filename, CreateTag,
            AccessPattern pattern = AccessPattern::None) {
    open(0, std::move(filename), pattern);
  }

  // open previously created array.
  // File at path filename must be a valid file created previously by this class
  // else the behavior is undefined
  void open(string filename, ReuseTag,
            AccessPattern pattern = AccessPattern::None);

  // Close the vector, saves all buffered data to the mapped file and closes it.
  // Vector
  void close();

  // change size of array.
  // if new size < old size elements and the end will be deleted
  // if new size > old size new elements will be uninitialized
  // iterators are possibly invalidated
  void resize(size_t newSize);

  // add element specified by arg el at the end of the array
  // possibly invalidates iterators
  void push_back(T&& el);
  void push_back(const T& el);

  // set a different access pattern if the use case  of  this  vector has
  // changed
  void setAccessPattern(AccessPattern p) {
    _pattern = p;
    advise(_pattern);
  }

 protected:
  // _________________________________________________________________________
  inline void throwIfUninitialized() const {
    if (_ptr == nullptr) {
      throw UninitializedArrayException();
    }
  }

  // truncate the underlying file to _bytesize and write meta data (_size,
  // _capacity, etc) for this
  // array to the end. new size of file will be _bytesize + sizeof(meta data)
  void writeMetaDataToEnd();

  // read meta data (_size, _capacity, _bytesize) etc. from the end of the
  // underlying file
  void readMetaDataFromEnd();

  // map the underlying file to memory in a read-only way.
  // Write accesses will be undefined behavior (mostly segfaults because of mmap
  // specifications, but we can enforce thread-safety etc.
  void mapForReading();

  // map the underlying file to memory in a read-write way.
  // reading and writing has to be synchronized
  void mapForWriting();

  // remap the file after a change of _bytesize
  // only supported on linux operating systems which support the mremap syscall.
  void remapLinux(size_t oldBytesize);

  // mmap can handle only multiples of the file system's page size
  // convert a size ( number of elements) to the smallest multiple of page size
  // that is >= than targetSize * sizeof(T)
  VecInfo convertArraySizeToFileSize(size_t targetSize) const;

  // make sure this vector has place for at least newCapacity elements before
  // having to "allocate" in the underlying file again.
  // Can also be used to shrink the file;
  // possibly invalidates iterators
  void adaptCapacity(size_t newCapacity);

  // wrapper to munmap, release the contents of the file.
  // invalidates iterators
  void unmap();

  // advise the kernel to use a certain access pattern
  void advise(AccessPattern pattern);

  T* _ptr = nullptr;
  size_t _size = 0;
  size_t _capacity = 0;
  size_t _bytesize = 0;
  std::string _filename = "";
  AccessPattern _pattern = AccessPattern::None;
  static constexpr float ResizeFactor = 1.5;
  static constexpr uint32_t MagicNumber = 7601577;
  static constexpr uint32_t Version = 0;
};

// MmapVector variation that only supports read-access to a previously created
// MmapVector-file.
template <class T>
class MmapVectorView : private MmapVector<T> {
 public:
  using const_iterator = const T*;
  using iterator = T*;
  // const access and iteration methods, directly map to the MmapVector-Variants
  const T* begin() const { return MmapVector<T>::begin(); }
  const T* end() const { return MmapVector<T>::end(); }
  const T* cbegin() const { return MmapVector<T>::cbegin(); }
  const T* cend() const { return MmapVector<T>::cend(); }
  const T& operator[](size_t idx) const {
    return MmapVector<T>::operator[](idx);
  }
  const T& at(size_t idx) const { return MmapVector<T>::at(idx); }

  // ____________________________________________________
  size_t size() const { return MmapVector<T>::size(); }

  // default constructor, leaves an unitialized vector that will throw until a
  // valid call to open()
  MmapVectorView() = default;

  // construct with any combination of arguments that is supported by the open()
  // member function
  template <typename... Args>
  MmapVectorView(Args&&... args) {
    open(std::forward<Args>(args)...);
  }

  // Move construction and assignment are allowed, but not copying
  MmapVectorView(MmapVectorView&& other) noexcept;
  MmapVectorView& operator=(MmapVectorView&& other) noexcept;

  // no copy construction/assignment. It would be possible to create a shared
  // mapping or to map the same file twice when copying. But the preferred
  // method should be to share one MmapVectorView by a (shared or non-owning)
  // pointer
  MmapVectorView(const MmapVectorView<T>&) = delete;
  MmapVectorView& operator=(const MmapVectorView<T>&) = delete;

  void open(string filename, AccessPattern pattern = AccessPattern::None) {
    this->unmap();
    this->_filename = std::move(filename);
    this->_pattern = pattern;
    this->readMetaDataFromEnd();
    this->mapForReading();
    this->advise(this->_pattern);
  }

  void open(const string& filename, ReuseTag,
            AccessPattern pattern = AccessPattern::None) {
    open(filename, pattern);
  }

  // explicitly close the vector to an unitialized state and free the associated
  // ressources
  void close() { MmapVector<T>::close(); }

  // destructor
  ~MmapVectorView() { close(); }

  // _____________________________________________________________
  std::string getFilename() const { return this->_filename; }
};
}  // namespace ad_utility
#include "./MmapVectorImpl.h"
