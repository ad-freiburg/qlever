// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <johannes.kalmbach@gmail.com>
//

#pragma once
#include <bzlib.h>
#include <exception>
#include <optional>
#include <string>
#include <vector>
#include "../util/File.h"

using std::string;
// _____________________________________________________________________
class Bzip2Exception : public std::exception {
 public:
  // TODO handle the different error codes of BZIP2
  Bzip2Exception(int errCode) {}
  Bzip2Exception(const string& msg) {}
  const char* what() const throw() {
    return "Error occured while decompressing Bzip2\n";
  }
};

class Bzip2Wrapper {
 public:
  Bzip2Wrapper() {
    // tell bzip2 to use standard malloc and free
    _bzStream.bzalloc = NULL;
    _bzStream.bzfree = NULL;
    _bzStream.opaque = NULL;
    _bzStream.avail_in = 0;
    _bzStream.next_in = nullptr;
    _bufferIn.resize(_bufferSize);

    auto errCode =
        BZ2_bzDecompressInit(&_bzStream, 0, 0);  // not verbose and not small
    if (errCode != BZ_OK) {
      throw Bzip2Exception(errCode);
    }
  }

  ~Bzip2Wrapper() { BZ2_bzDecompressEnd(&_bzStream); }

  std::optional<std::vector<char>> decompressBlock() {
    if (_bzStream.avail_in == 0) {
      readNextBlock();
    }
    _bufferOut.resize(_bufferSize);
    _bzStream.next_out = _bufferOut.data();
    _bzStream.avail_out = _bufferOut.size();
    auto errcode = BZ2_bzDecompress(&_bzStream);
    if (errcode == BZ_STREAM_END) {
      return std::nullopt;
    } else if (errcode != BZ_OK) {
      throw Bzip2Exception(errcode);
    }
    _bufferOut.resize(_bufferOut.size() - _bzStream.avail_out);
    return std::move(_bufferOut);
  }

  // _________________________________________________________________
  void open(const string& filename) {
    if (_file.isOpen()) {
      _file.close();
    }
    _file.open(filename.c_str(), "r");
  }

 private:
  bz_stream _bzStream;
  ad_utility::File _file;
  std::vector<char> _bufferIn;
  std::size_t _ReadPositionIn = 0;
  std::vector<char> _bufferOut;
  bool _endOfFile = false;
  size_t _bufferSize = 100 << 20;  // default 100 MB of Buffer

  // ________________________________________________________________
  void readNextBlock() {
    if (!_file.isOpen()) {
      throw Bzip2Exception(
          "Trying to decompress BZIP2 without specifying a source file, "
          "nothing to decompress\n");
    }
    auto bytesRead = _file.read(_bufferIn.data(), _bufferIn.size());
    if (bytesRead == 0) {
      _endOfFile = true;
    }
    _bzStream.next_in = _bufferIn.data();
    _bzStream.avail_in = bytesRead;
  }

};
