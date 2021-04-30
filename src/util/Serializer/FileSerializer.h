//
// Created by johannes on 30.04.21.
//

#ifndef QLEVER_FILESERIALIZER_H
#define QLEVER_FILESERIALIZER_H

#include "../File.h"

namespace ad_utility {
namespace serialization {
class FileWriteSerializer {
  constexpr bool isWriteSerializer = true;

 public:
  FileWriteSerializer(File&& file) : _file{std::move(file)} {};

  FileWriteSerializer(std::string filename) : _file{filename, 'w'} {
    AD_CHECK(_file.isOpen());
    // TODO<joka921> File should be a move-only type, should support
    // "isOpenForReading" and should automatically check for "isOpen() when
    // calling open with an appropriate error message
  }

  void serializeBytes(std::byte* bytePtr, size_t numBytes) {
    _file.write(bytePtr, numBytes);
  }

  File&& moveFileOut() && { return std::move(_file); }

 private:
  File _file;
};

class FileReadSerializer {
  constexpr bool isWriteSerializer = false;

 public:
  FileReadSerializer(File&& file) : _file{std::move(file)} {};

  FileReadSerializer(std::string filename) : _file{filename, 'r'} {
    AD_CHECK(_file.isOpen());
  }

  void serializeBytes(std::byte* bytePtr, size_t numBytes) {
    _file.read(bytePtr, numBytes);
  }

  File&& moveFileOut() && { return std::move(_file); }

 private:
  File _file;
};

}  // namespace serialization
}  // namespace ad_utility

#endif  // QLEVER_FILESERIALIZER_H
