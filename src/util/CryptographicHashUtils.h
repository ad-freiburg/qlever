//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures
//  Author: Hannes Baumann <baumannh@informatik.uni-freiburg.de>

#ifndef QLEVER_SRC_UTIL_CRYPTOGRAPHICHASHUTILS_H
#define QLEVER_SRC_UTIL_CRYPTOGRAPHICHASHUTILS_H

#include <absl/strings/str_join.h>
#include <openssl/evp.h>
#include <openssl/md5.h>
#include <openssl/sha.h>

#include <string_view>
#include <vector>

namespace ad_utility {
// `HashImpl` is a templated struct which returns for given input
// string the respective hash (hex) by applying the `HashEvpFunc`
// function from openssl over the provided `openssl/evp.h` high level interface.
template <auto HashEvpFunc, size_t DigestLength>
struct HashImpl {
  std::vector<unsigned char> operator()(std::string_view str) const {
    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    auto md = HashEvpFunc();
    std::vector<unsigned char> hexHash(DigestLength);
    EVP_DigestInit_ex(ctx, md, nullptr);
    EVP_DigestUpdate(ctx, str.data(), str.length());
    EVP_DigestFinal(ctx, hexHash.data(), nullptr);
    EVP_MD_CTX_free(ctx);
    return hexHash;
  }
};

// Struct which enables conversion of hash value to
// hex hash string (`std::string`) with `absl` library.
struct HexFormatter {
  void operator()(std::string* out, char c) const {
    absl::AlphaNumFormatter()(out, absl::Hex(c, absl::kZeroPad2));
  }
};
inline constexpr HexFormatter hexFormatter{};

// `hashMd5` takes an argument of type `std::string_view` and provides
// a Hex value by applying `EVP_md5()` from the openssl library as a result.
inline constexpr HashImpl<&EVP_md5, MD5_DIGEST_LENGTH> hashMd5{};

// `hashSha1` takes an argument of type `std::string_view` and provides
// a Hex by applying `EVP_sha1()` from the openssl library as a result.
inline constexpr HashImpl<&EVP_sha1, SHA_DIGEST_LENGTH> hashSha1{};

// `hashSha256` takes an argument of type `std::string_view` and provides
// a Hex by applying `EVP_sha256()` from the openssl library as a result.
inline constexpr HashImpl<&EVP_sha256, SHA256_DIGEST_LENGTH> hashSha256{};

// `hashSha384` takes an argument of type `std::string_view` and provides
// a Hex by applying `EVP_sha384()` from the openssl library as a result.
inline constexpr HashImpl<&EVP_sha384, SHA384_DIGEST_LENGTH> hashSha384{};

// `hashSha512` takes an argument of type `std::string_view` and provides
// a Hex by applying `EVP_sha512()` from the openssl library as a result.
inline constexpr HashImpl<&EVP_sha512, SHA512_DIGEST_LENGTH> hashSha512{};

}  //  namespace ad_utility

#endif  // QLEVER_SRC_UTIL_CRYPTOGRAPHICHASHUTILS_H
