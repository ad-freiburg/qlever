//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures
//  Author: Hannes Baumann <baumannh@informatik.uni-freiburg.de>

#pragma once

#include <openssl/evp.h>
#include <openssl/md5.h>
#include <openssl/sha.h>

#include <string_view>
#include <vector>

namespace ad_utility {
// `hashImpl` is a templated lambda function which returns for given input
// string the respective hash (hex) by applying the `HashEvpFunc`
// function from openssl over the provided `openssl/evp.h` high level interface.
template <auto HashEvpFunc, size_t DigestLength>
inline constexpr auto hashImpl =
    [](const std::string_view& str) -> std::vector<unsigned char> {
  EVP_MD_CTX* ctx = EVP_MD_CTX_new();
  auto md = HashEvpFunc();
  std::vector<unsigned char> hexHash(DigestLength);
  EVP_DigestInit_ex(ctx, md, NULL);
  EVP_DigestUpdate(ctx, str.data(), str.length());
  EVP_DigestFinal(ctx, hexHash.data(), NULL);
  EVP_MD_CTX_free(ctx);
  return hexHash;
};

// `HashMd5` takes an arguement of type `std::string_view` and provides
// a Hex value by applying `EVP_md5()` from the openssl library as a result.
constexpr auto hashMd5 = hashImpl<EVP_md5, MD5_DIGEST_LENGTH>;

// `HashSha1` takes an arguement of type `std::string_view` and provides
// a Hex by applying `EVP_sha1()` from the openssl library as a result.
constexpr auto hashSha1 = hashImpl<EVP_sha1, SHA_DIGEST_LENGTH>;

// `HashSha256` takes an arguement of type `std::string_view` and provides
// a Hex by applying `EVP_sha256()` from the openssl library as a result.
constexpr auto hashSha256 = hashImpl<EVP_sha256, SHA256_DIGEST_LENGTH>;

// `HashSha384` takes an arguement of type `std::string_view` and provides
// a Hex by applying `EVP_sha384()` from the openssl library as a result.
constexpr auto hashSha384 = hashImpl<EVP_sha384, SHA384_DIGEST_LENGTH>;

// `HashSha512` takes an arguement of type `std::string_view` and provides
// a Hex by applying `EVP_sha512()` from the openssl library as a result.
constexpr auto hashSha512 = hashImpl<EVP_sha512, SHA512_DIGEST_LENGTH>;

}  //  namespace ad_utility
