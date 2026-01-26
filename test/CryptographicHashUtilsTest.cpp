//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures
//  Author: Hannes Baumann <baumannh@informatik.uni-freiburg.de>

#include <gtest/gtest.h>

#include <string>

#include "util/CryptographicHashUtils.h"

using namespace ad_utility;

// Helper to convert the hex hash values to a hex digit string
inline auto toHexString(std::vector<unsigned char> hashed) {
  return absl::StrJoin(hashed, "", hexFormatter);
};

std::string testStr1 = "";
std::string testStr2 = "Friburg23o";
std::string testStr3 = "abc";

// TEST MD5
TEST(CryptographicHashUtilsTest, testMd5) {
  auto res1 = toHexString(HashMd5{}(testStr1));
  auto res2 = toHexString(HashMd5{}(testStr2));
  auto res3 = toHexString(HashMd5{}(testStr3));
  EXPECT_EQ(res1, "d41d8cd98f00b204e9800998ecf8427e");
  EXPECT_EQ(res2, "9d9a73f67e20835e516029541595c381");
  EXPECT_EQ(res3, "900150983cd24fb0d6963f7d28e17f72");
}

// TEST SHA1
TEST(CryptographicHashUtilsTest, testSha1) {
  auto res1 = toHexString(HashSha1{}(testStr1));
  auto res2 = toHexString(HashSha1{}(testStr2));
  auto res3 = toHexString(HashSha1{}(testStr3));
  EXPECT_EQ(res1, "da39a3ee5e6b4b0d3255bfef95601890afd80709");
  EXPECT_EQ(res2, "c3a77a6104fa091f590f594b3e2dba2668196d3c");
  EXPECT_EQ(res3, "a9993e364706816aba3e25717850c26c9cd0d89d");
}

// TEST SHA256
TEST(CryptographicHashUtilsTest, testSha256) {
  auto res1 = toHexString(HashSha256{}(testStr1));
  auto res2 = toHexString(HashSha256{}(testStr2));
  auto res3 = toHexString(HashSha256{}(testStr3));
  EXPECT_EQ(res1,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
  EXPECT_EQ(res2,
            "af8d98f09845a700aea36b35e8cc3a35632e38d0f7be9c0ca508e53c578da900");
  EXPECT_EQ(res3,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
}

// TEST SHA384
TEST(CryptographicHashUtilsTest, testSha384) {
  auto res1 = toHexString(HashSha384{}(testStr1));
  auto res2 = toHexString(HashSha384{}(testStr2));
  auto res3 = toHexString(HashSha384{}(testStr3));
  EXPECT_EQ(res1,
            "38b060a751ac96384cd9327eb1b1e36a21fdb71114be07434c0cc7bf63f6e1da27"
            "4edebfe76f65fbd51ad2f14898b95b");
  EXPECT_EQ(res2,
            "72810006e3b418ebd179812522cafa486cd6c2a988378fac148af1a9a098a01ce3"
            "373734c23978f7df68bf7e98955c02");
  EXPECT_EQ(res3,
            "cb00753f45a35e8bb5a03d699ac65007272c32ab0eded1631a8b605a43ff5bed80"
            "86072ba1e7cc2358baeca134c825a7");
}

// TEST SHA512
TEST(CryptographicHashUtilsTest, testSha512) {
  auto res1 = toHexString(HashSha512{}(testStr1));
  auto res2 = toHexString(HashSha512{}(testStr2));
  auto res3 = toHexString(HashSha512{}(testStr3));
  EXPECT_EQ(res1,
            "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47"
            "d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e");
  EXPECT_EQ(res2,
            "be4422bfad59ee51e98dc51c540dc9d85333cb786333b152d13b2bebde1bdaa499"
            "e9d4e1370a5bb2e831f4443b1358f2301fd5214ba80554ea0ff1d185c3b027");
  EXPECT_EQ(res3,
            "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a21"
            "92992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f");
}
