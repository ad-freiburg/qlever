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
  Bzip2Exception(int errCode) {
    string msg;
    switch (errCode) {
      case BZ_CONFIG_ERROR:
        msg = "BZ_CONFIG_ERROR";
        break;
      case BZ_PARAM_ERROR:
        msg = "BZ_PARAM_ERROR";
        break;
      case BZ_DATA_ERROR:
        msg = "BZ_DATA_ERROR";
        break;
      case BZ_DATA_ERROR_MAGIC:
        msg = "BZ_DATA_ERROR_MAGIC";
        break;
      case BZ_MEM_ERROR:
        msg = "BZ_MEM_ERROR";
        break;
      default:
        msg = "unknown error";
    }
    _msg = "Internal exception in libbz: " + msg;
  }

  Bzip2Exception(const string& msg) : _msg(msg) {}
  const char* what() const throw() { return _msg.c_str(); }
  string _msg;
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

  std::optional<size_t> decompressBlock(char* target, size_t maxRead) {
    if (_endOfBzipStream) {
      return std::nullopt;
    }
    if (_bzStream.avail_in == 0) {
      if (_endOfFile) {
        throw Bzip2Exception(
            "bzip2-compressed file ended before end of decompression stream "
            "was detected");
      }
      readNextBlock();
    }
    _bzStream.next_out = target;
    _bzStream.avail_out = maxRead;
    auto errcode = BZ2_bzDecompress(&_bzStream);
    if (errcode == BZ_STREAM_END) {
      _endOfBzipStream = true;
    } else if (errcode != BZ_OK) {
      throw Bzip2Exception(errcode);
    }
    return (maxRead - _bzStream.avail_out);
  }

  // ________________________________________________________
  std::optional<std::vector<char>> decompressBlock(size_t maxRead = 10 << 20) {
    std::vector<char> buf;
    buf.resize(maxRead);
    auto res = decompressBlock(buf.data(), buf.size());
    if (!res) {
      return std::nullopt;
    } else {
      buf.resize(res.value());
      return std::move(buf);
    }
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
  bool _endOfBzipStream = false;
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
