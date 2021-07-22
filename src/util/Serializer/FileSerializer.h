//
// Created by johannes on 30.04.21.
//

#ifndef QLEVER_FILESERIALIZER_H
#define QLEVER_FILESERIALIZER_H

#include <memory>

#include "../File.h"
#include "./Serializer.h"

namespace ad_utility {
namespace serialization {

using SerializationPosition = off_t;

class FileWriteSerializer {
 public:
  static constexpr bool IsWriteSerializer = true;

  FileWriteSerializer(File&& file) : _file{std::move(file)} {};

  FileWriteSerializer(std::string filename) : _file{filename, "w"} {
    AD_CHECK(_file.isOpen());
    // TODO<joka921> File should be a move-only type, should support
    // "isOpenForReading" and should automatically check for "isOpen() when
    // calling open with an appropriate error message
  }

  void serializeBytes(const char* bytePtr, size_t numBytes) {
    _file.write(bytePtr, numBytes);
  }

  void close() { _file.close(); }

  SerializationPosition getCurrentPosition() const { return _file.tell(); }

  File&& file() && { return std::move(_file); }

 private:
  File _file;
};

class FileReadSerializer {
 public:
  static constexpr bool IsWriteSerializer = false;

  FileReadSerializer(File&& file) : _file{std::move(file)} {};

  FileReadSerializer(std::string filename) : _file{filename, "r"} {
    AD_CHECK(_file.isOpen());
  }

  void serializeBytes(char* bytePtr, size_t numBytes) {
    auto numBytesActuallyRead = _file.read(bytePtr, numBytes);
    if (numBytesActuallyRead < numBytes) {
      throw SerializationException{
          "Tried to read from a File but too few bytes were returned"};
    }
  }

  bool isExhausted() { return _file.isAtEof(); }

  void setSerializationPosition(SerializationPosition position) {
    _file.seek(position, SEEK_SET);
  }

  File&& file() && { return std::move(_file); }

 private:
  File _file;
};

/*
 * This Serializer may be copied. The copies will access the same file,
 * but will maintain different _serializationPositions
 */
class CopyableFileReadSerializer {
 public:
  static constexpr bool IsWriteSerializer = false;
  CopyableFileReadSerializer(std::shared_ptr<File> filePtr)
      : _file{std::move(filePtr)} {};

  CopyableFileReadSerializer(std::string filename)
      : _file{std::make_shared<File>(filename, "r")} {
    AD_CHECK(_file->isOpen());
  }

  void serializeBytes(char* bytePtr, size_t numBytes) {
    AD_CHECK(static_cast<ssize_t>(numBytes) ==
             _file->read(bytePtr, numBytes, _serializationPosition));
    _serializationPosition += numBytes;
  }

  void setSerializationPosition(SerializationPosition position) {
    _serializationPosition = position;
  }

 private:
  std::shared_ptr<File> _file;
  SerializationPosition _serializationPosition = 0;
};

}  // namespace serialization
}  // namespace ad_utility

#endif  // QLEVER_FILESERIALIZER_H
