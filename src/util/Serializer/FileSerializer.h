//
// Created by johannes on 30.04.21.
//

#ifndef QLEVER_FILESERIALIZER_H
#define QLEVER_FILESERIALIZER_H

#include <memory>

#include "util/File.h"
#include "util/Serializer/Serializer.h"

namespace ad_utility {
namespace serialization {

using SerializationPosition = uint64_t;

// A write serializer that writes to a file. The `AlignedSerialization`
// template parameter controls whether alignment padding is inserted for
// trivially serializable types (see `alignForType` in `Serializer.h`).
template <bool AlignedSerialization = false>
class FileWriteSerializerT {
 public:
  using SerializerType = WriteSerializerTag;
  static constexpr bool UsesAlignedSerialization = AlignedSerialization;

  FileWriteSerializerT(File&& file) : _file{std::move(file)} {};

  FileWriteSerializerT(std::string filename) : _file{filename, "w"} {
    AD_CONTRACT_CHECK(_file.isOpen());
    // TODO<joka921> File should be a move-only type, should support
    // "isOpenForReading" and should automatically check for "isOpen() when
    // calling open with an appropriate error message
  }

  void serializeBytes(const char* bytePtr, size_t numBytes) {
    _file.write(bytePtr, numBytes);
  }

  void close() { _file.close(); }

  [[nodiscard]] SerializationPosition getSerializationPosition() const {
    return _file.tell() - fileOffsetOnConstruction_;
  }

  // Alias for compatibility with alignment serialization framework.
  [[nodiscard]] size_t getCurrentPosition() const {
    return static_cast<size_t>(getSerializationPosition());
  }

  void setSerializationPosition(SerializationPosition position) {
    _file.seek(static_cast<off_t>(position + fileOffsetOnConstruction_),
               SEEK_SET);
  }

  File&& file() && { return std::move(_file); }

 private:
  File _file;
  size_t fileOffsetOnConstruction_ = _file.tell();
};

// A read serializer that reads from a file. The `AlignedSerialization`
// template parameter controls whether alignment padding is skipped for
// trivially serializable types (see `alignForType` in `Serializer.h`).
template <bool AlignedSerialization = false>
class FileReadSerializerT {
 public:
  using SerializerType = ReadSerializerTag;
  static constexpr bool UsesAlignedSerialization = AlignedSerialization;

  explicit FileReadSerializerT(File&& file) : _file{std::move(file)} {};

  explicit FileReadSerializerT(const std::string& filename)
      : _file{filename, "r"} {
    AD_CONTRACT_CHECK(_file.isOpen());
  }

  void serializeBytes(char* bytePtr, size_t numBytes) {
    auto numBytesActuallyRead = _file.read(bytePtr, numBytes);
    if (numBytesActuallyRead < numBytes) {
      throw SerializationException{
          "Tried to read from a File but too few bytes were returned"};
    }
  }

  void setSerializationPosition(SerializationPosition position) {
    _file.seek(static_cast<off_t>(position) + fileOffsetOnConstruction_,
               SEEK_SET);
  }

  void skip(size_t numBytes) {
    _file.seek(static_cast<off_t>(numBytes), SEEK_CUR);
  }

  size_t getCurrentPosition() const {
    return _file.tell() - fileOffsetOnConstruction_;
  }

  File&& file() && { return std::move(_file); }

 private:
  File _file;
  size_t fileOffsetOnConstruction_ = _file.tell();
};

// Backward-compatible aliases for the default (unaligned) serializers.
using FileWriteSerializer = FileWriteSerializerT<false>;
using FileReadSerializer = FileReadSerializerT<false>;

// Aligned variants of the file serializers.
using AlignedFileWriteSerializer = FileWriteSerializerT<true>;
using AlignedFileReadSerializer = FileReadSerializerT<true>;

/*
 * This Serializer may be copied. The copies will access the same file,
 * but will maintain different _serializationPositions
 */
class CopyableFileReadSerializer {
 public:
  using SerializerType = ReadSerializerTag;
  explicit CopyableFileReadSerializer(std::shared_ptr<File> filePtr)
      : _file{std::move(filePtr)} {};

  explicit CopyableFileReadSerializer(std::string filename)
      : _file{std::make_shared<File>(filename, "r")} {
    AD_CONTRACT_CHECK(_file->isOpen());
  }

  void serializeBytes(char* bytePtr, size_t numBytes) {
    AD_CONTRACT_CHECK(static_cast<ssize_t>(numBytes) ==
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
