// Copyright 2011, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold <buchholb>

#pragma once
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <sys/stat.h>
#include <string>
#include <vector>
#include <iostream>

#include "./Exception.h"

using std::string;
using std::vector;
using std::cout;
using std::cerr;
using std::endl;

namespace ad_utility {
//! Wrapper class for file access. Is supposed to provide
//! methods that allow fast access to binary files.
//! Also features methods for dealing with ASCII files
//! but is less efficient then stl functions due
//! to the lack of buffering.
//! Many methods are copies from the CompleteSearch File.h
class File {
  private:

    string _name;
    FILE* _file;

  public:

    //! Default constructor
    File() {
      _file = NULL;
      _name = "";
    }

    //! Constructor that creates an instance from the
    //! file system.
    File(const char* filename, const char* mode) :
      _name(filename) {
      open(filename, mode);
    }

    //! Copy constructor.
    //! Does not copy the file in the file system!
    File(const File& orig) {
      _name = orig._name;
      open(_name.c_str(), "r");
      assert(_file);
    }

    //! Destructor closes file if still open
    ~File() {
      if (isOpen()) close();
    }

    //! OPEN FILE (exit with error if fails, returns true otherwise)
    bool open(const char* filename, const char* mode) {
      _file = fopen(filename, mode);
      if (_file == NULL) {
        cout << "! ERROR opening file \"" << filename << "\" with mode \""
            << mode << "\" (" << strerror(errno) << ")" << endl << endl;
        exit(1);
      }
      _name = filename;
      return true;
    }

    //! checks if the file is open.
    bool isOpen() const {
      return (_file != NULL);
    }

    //! Close file.
    bool close() {
      if (not isOpen()) {
        return true;
      }
      if (fclose(_file) != 0) {
        cout << "! ERROR closing file \"" << _name << "\" ("
            << strerror(errno) << ")" << endl << endl;
        exit(1);
      }
      _file = NULL;
      return true;
    }

    bool empty() {
      return sizeOfFile() == 0;
    }

    // read from current file pointer position
    // returns the number of bytes read
    size_t readFromBeginning(void* targetBuffer, size_t nofBytesToRead) {
      assert(_file);
      seek(0, SEEK_SET);
      return fread(targetBuffer, (size_t) 1, nofBytesToRead, _file);
    }

    // read from current file pointer position
    // returns the number of bytes read
    size_t read(void* targetBuffer, size_t nofBytesToRead) {
      assert(_file);
      return fread(targetBuffer, (size_t) 1, nofBytesToRead, _file);
    }

    // Reads a line from the file into param line and interprets it as ASCII.
    // Removes the \n from the end of the string.
    // Returns null on EOF when no chars have been read.
    // Throws an Exception if the buffer is not sufficiently large.
    // Warning: std::getline which works on a file stream
    // is generally more performant (it uses a general stream buffer
    // to wrap the file)
    // and appeds char by char to the string.
    bool readLine(string* line, char* buf, size_t bufferSize) {
      assert(_file);
      char* retVal = fgets(buf, bufferSize, _file);
      size_t stringLength = strlen(buf);
      if (retVal && stringLength > 0 && buf[stringLength - 1] == '\n') {
        // Remove the trailing \n
        buf[stringLength - 1] = 0;
      } else if (retVal && !isAtEof()) {
        // Stopped inside a line because the end of the buffer was reached.
        AD_THROW(ad_semsearch::Exception::INVALID_PARAMETER_VALUE,
            "Buffer too small when reading from file: " + _name + ". "
                "Or the line contains a 0 character.");
      }
      *line = buf;
      return retVal;
    }

    // write to current file pointer position
    // returns number of bytes written
    size_t write(const void* sourceBuffer, size_t nofBytesToWrite) {
      assert(_file);
      return fwrite(sourceBuffer, (size_t) 1, nofBytesToWrite, _file);
    }

    // write to current file pointer position
    // returns number of bytes written
    void writeLine(const string& line) {
      assert(_file);
      write(line.c_str(), line.size());
      write("\n", 1);
    }

    void flush() {
      fflush(_file);
    }

    bool isAtEof() {
      assert(_file);
      return feof(_file) != 0;
    }

    //! Read an ASCII file into a vector of strings.
    //! Each line represents an entry.
    void readIntoVector(vector<string>* target, char* buf, size_t bufferSize) {
      string line;
      while (readLine(&line, buf, bufferSize)) {
        target->push_back(line);
      }
    }

    //! Seeks a position in the file.
    //! Sets the file position indicator for the stream.
    //! The new position is obtained by adding seekOffset
    //! bytes to the position seekOrigin.
    //! Returns true on success
    bool seek(off_t seekOffset, int seekOrigin) {
      assert((seekOrigin == SEEK_SET) ||
          (seekOrigin == SEEK_CUR) || (seekOrigin == SEEK_END));
      assert(_file);
      return (fseeko(_file, seekOffset, seekOrigin) == 0);
    }

    //! Move file pointer to desired position
    //! (offset from file beginning) and start reading from there
    //! returns number of bytes read
    size_t read(void* targetBuffer, size_t nofBytesToRead,
        off_t seekOffsetFromStart) {
      assert(_file);
      return pread(fileno(_file), targetBuffer,
          nofBytesToRead, seekOffsetFromStart);
    }

    //! Returns the number of bytes from the beginning
    //! is 0 on opening. Later equal the number of bytes written.
    //! -1 is returned when an error occurs
    off_t tell() const {
      assert(_file);
      off_t returnValue = ftello(_file);
      if (returnValue == (off_t) -1) {
        cerr << "\n ERROR in tell() : " << strerror(errno) << endl << endl;
        exit(1);
      }
      return returnValue;
    }

    //! Returns the size of the file as off_t.
    off_t sizeOfFile() {
      seek((off_t) 0, SEEK_END);
      off_t sizeOfFile = tell();
      return sizeOfFile;
    }

    //! Returns the last part of the file as off_t.
    off_t getTrailingOffT() {
      assert(_file);
      off_t lastOffset;
      // seek to end of file
      seek((off_t) 0, SEEK_END);
      off_t sizeOfFile = tell();

      // now seek to end of file - sizeof(off_t)
#ifdef NDEBUG
      seek(sizeOfFile - sizeof(off_t), SEEK_SET);
      // Assign the result of ftello to the unset lastOffset to get rid
      // of warnings. This isn't really meaningful.
      lastOffset = ftello(_file);
#else
      bool seekReturn = seek(sizeOfFile - sizeof(off_t), SEEK_SET);
      assert(seekReturn);
      const off_t lastOffsetOffset = ftello(_file);
      assert(lastOffsetOffset == (off_t) (sizeOfFile - sizeof(off_t)));
      assert(lastOffsetOffset > (off_t) 0);
#endif


      // now read the last off_t
#ifdef NDEBUG
      read(&lastOffset, sizeof(off_t));
#else
      size_t readReturn = read(&lastOffset, sizeof(off_t));
      assert(readReturn == (size_t) sizeof(off_t));
      assert(lastOffset > 0);
#endif

      return lastOffset;
    }

    // returns the byte offset of the last off_t
    // the off_t itself is passed back by reference
    off_t getLastOffset(off_t* lastOffset) {
      assert(_file);

      // seek to end of file
      seek((off_t) 0, SEEK_END);
      off_t sizeOfFile = tell();
      assert(sizeOfFile > 0);

      // now seek to end of file - sizeof(off_t)
#ifdef NDEBUG
      seek(sizeOfFile - sizeof(off_t), SEEK_SET);
#else
      bool seekReturn = seek(sizeOfFile - sizeof(off_t), SEEK_SET);
      assert(seekReturn);
#endif
      const off_t lastOffsetOffset = ftello(_file);
      assert(lastOffsetOffset == (off_t) (sizeOfFile - sizeof(off_t)));

      // now read the last off_t
#ifdef NDEBUG
      read(lastOffset, sizeof(off_t));
#else
      size_t readReturn = read(lastOffset, sizeof(off_t));
      assert(readReturn == (size_t) sizeof(off_t));
      assert(lastOffset != nullptr);
#endif

      return lastOffsetOffset;
    }

    // Static method to check if a file exists.
    static bool exists(const string& path) {
      struct stat buffer;
      return (stat (path.c_str(), &buffer) == 0);
    }

};
}

