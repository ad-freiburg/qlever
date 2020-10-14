//
// Created by johannes on 04.05.20.
//
#include <gtest/gtest.h>
#include <cmath>
#include "../src/global/Id.h"
#include "../src/util/Random.h"

TEST(FancyId, SetAndAddFloat) {
  static_assert(std::is_trivially_copyable_v<FancyId>, "it is required that the FancyId type can be memcopied");
  static_assert(sizeof(FancyId) == sizeof(uint64_t), "FancyId must have 64 bits, else a lot of stuff breaks");
  auto a1 = alignof(FancyId);
  auto a2 = alignof(uint64_t);
  ASSERT_EQ(a1, a2);
  auto eval = [](auto op) {
    ad_utility::RandomFloatGenerator<float> gen;
    for (size_t i = 0; i < 1000000; ++i) {
      float f1 = gen(), f2 = gen();
      FancyId a1(f1);
      ASSERT_EQ(FancyId::FLOAT, a1.type());
      ASSERT_EQ(a1.getFloat(), f1);
      FancyId a2(f2);
      ASSERT_EQ(FancyId::FLOAT, a2.type());
      ASSERT_EQ(a2.getFloat(), f2);
      FancyId res = op(a1,  a2);
      ASSERT_EQ(FancyId::FLOAT, res.type());
      ASSERT_EQ(res.getFloat(), op(f1 , f2));
    }

    std::vector<float> vals{std::numeric_limits<float>::infinity(),
                            -std::numeric_limits<float>::infinity(), +0.0f,
                            -0.0f, std::numeric_limits<float>::quiet_NaN()};
    for (float f1 : vals) {
      for (float f2 : vals) {
        FancyId a1(f1);
        ASSERT_EQ(FancyId::FLOAT, a1.type());
        ASSERT_TRUE(a1.getFloat() == f1 ||
                    (std::isnan(a1.getFloat()) && std::isnan(f1)));
        FancyId a2(f2);
        ASSERT_EQ(FancyId::FLOAT, a2.type());
        ASSERT_TRUE(a2.getFloat() == f2 ||
                    (std::isnan(a2.getFloat()) && std::isnan(f2)));
        FancyId res = op(a1 , a2);
        ASSERT_EQ(FancyId::FLOAT, res.type());
        ASSERT_TRUE(res.getFloat() == op(f1 , f2) ||
                    (std::isnan(res.getFloat()) && std::isnan(op(f1 , f2))));
      }
    }
  };
  eval([](const auto& a, const auto& b){return a + b;});
  eval([](const auto& a, const auto& b){return a - b;});
  eval([](const auto& a, const auto& b){return a * b;});
  eval([](const auto& a, const auto& b){return a / b;});
}

TEST(FancyId, AddToNan) {
  auto eval = [](auto op) {
    ad_utility::RandomFloatGenerator<float> genF;
    ad_utility::RandomIntGenerator<uint64_t> genU(0, FancyId::MAX_VAL);
    for (size_t i = 0; i < 1000000; ++i) {
      std::vector<FancyId::Type> types = {FancyId::VOCAB, FancyId::LOCAL_VOCAB, FancyId::DATE};
      float f = 1.5f;
      FancyId idF(f);
      uint64_t u = 32u;
      FancyId idU(FancyId::VOCAB, u);
      ASSERT_EQ(idU.type(), FancyId::VOCAB);
      ASSERT_EQ(idU.getUnsigned(), u);

      {
        auto res = op(idF, idU);
        ASSERT_EQ(res.type(), FancyId::FLOAT);
        ASSERT_TRUE(std::isnan(res.getFloat()));
      }

      {
        auto res = op(idU, idF);
        ASSERT_EQ(res.type(), FancyId::FLOAT);
        ASSERT_TRUE(std::isnan(res.getFloat()));
      }
    }
  };
  eval([](const auto& a, const auto& b){return a + b;});
  eval([](const auto& a, const auto& b){return a - b;});
  eval([](const auto& a, const auto& b){return a * b;});
  eval([](const auto& a, const auto& b){return a / b;});
}

TEST(FancyId, SetUnsigned) {
  std::vector<FancyId::Type> types{FancyId::VOCAB, FancyId::LOCAL_VOCAB, FancyId::DATE};
  for (const auto& type : types) {
    {
      ad_utility::RandomIntGenerator<uint64_t> gen(
          0, std::numeric_limits<uint32_t>::max());
      for (size_t i = 0; i < 1000000; ++i) {
        auto val = gen();
        FancyId id(type, val);
        ASSERT_EQ(id.type(), type);
        ASSERT_EQ(id.getUnsigned(), val);
      }
    }
    {
      ad_utility::RandomIntGenerator<uint64_t> gen(
          std::numeric_limits<uint32_t>::max(), FancyId::MAX_VAL);
      for (size_t i = 0; i < 1000000; ++i) {
        auto val = gen();
        FancyId id(type, val);
        ASSERT_EQ(id.type(), type);
        ASSERT_EQ(id.getUnsigned(), val);
      }
    }
    {
      FancyId id(type, FancyId::MAX_VAL);
      ASSERT_EQ(id.type(), type);
      ASSERT_EQ(id.getUnsigned(), FancyId::MAX_VAL);
    }
    {
      FancyId id(type, FancyId::MAX_VAL - 1);
      ASSERT_EQ(id.type(), type);
      ASSERT_EQ(id.getUnsigned(), FancyId::MAX_VAL - 1);
    }
    {
      auto val = std::numeric_limits<uint32_t>::max();
      FancyId id(type, val);
      ASSERT_EQ(id.type(), type);
      ASSERT_EQ(id.getUnsigned(), val);
    }
    {
      auto val = std::numeric_limits<uint32_t>::max() - 1;
      FancyId id(type, val);
      ASSERT_EQ(id.type(), type);
      ASSERT_EQ(id.getUnsigned(), val);
    }
    {
      auto val =
          static_cast<uint64_t>(std::numeric_limits<uint32_t>::max()) + 1;
      FancyId id(type, val);
      ASSERT_EQ(id.type(), type);
      ASSERT_EQ(id.getUnsigned(), val);
    }
  }
}

TEST(FancyId, SetInt) {
  std::vector<FancyId::Type> types{FancyId::INTEGER};
  for (const auto& type : types) {
    {
      ad_utility::RandomIntGenerator<int64_t> gen(
          std::numeric_limits<int32_t>::min(), std::numeric_limits<uint32_t>::max());
      for (size_t i = 0; i < 1000000; ++i) {
        auto val = gen();
        FancyId id(type, val);
        ASSERT_EQ(id.type(), type);
        ASSERT_EQ(id.getInteger(), val);
      }
    }
    {
      ad_utility::RandomIntGenerator<int64_t> gen(
          std::numeric_limits<uint32_t>::max(), FancyId::INTEGER_MAX_VAL);
      for (size_t i = 0; i < 1000000; ++i) {
        auto val = gen();
        FancyId id(type, val);
        ASSERT_EQ(id.type(), type);
        ASSERT_EQ(id.getInteger(), val);
      }
    }
    {
      ad_utility::RandomIntGenerator<int64_t> gen(
          FancyId::INTEGER_MIN_VAL, std::numeric_limits<int32_t>::min());
      for (size_t i = 0; i < 1000000; ++i) {
        auto val = gen();
        FancyId id(type, val);
        ASSERT_EQ(id.type(), type);
        ASSERT_EQ(id.getInteger(), val);
      }
    }
    {
      FancyId id(type, FancyId::INTEGER_MAX_VAL);
      ASSERT_EQ(id.type(), type);
      ASSERT_EQ(id.getInteger(), FancyId::INTEGER_MAX_VAL);
    }
    {
      FancyId id(type, FancyId::INTEGER_MAX_VAL - 1);
      ASSERT_EQ(id.type(), type);
      ASSERT_EQ(id.getInteger(), FancyId::INTEGER_MAX_VAL - 1);
    }
    {
      FancyId id(type, FancyId::INTEGER_MIN_VAL);
      ASSERT_EQ(id.type(), type);
      ASSERT_EQ(id.getInteger(), FancyId::INTEGER_MIN_VAL);
    }
    {
      FancyId id(type, FancyId::INTEGER_MIN_VAL + 1);
      ASSERT_EQ(id.type(), type);
      ASSERT_EQ(id.getInteger(), FancyId::INTEGER_MIN_VAL + 1);
    }
    {
      auto val = std::numeric_limits<uint32_t>::max();
      FancyId id(type, val);
      ASSERT_EQ(id.type(), type);
      ASSERT_EQ(id.getInteger(), val);
    }
    {
      auto val = std::numeric_limits<uint32_t>::max() - 1;
      FancyId id(type, val);
      ASSERT_EQ(id.type(), type);
      ASSERT_EQ(id.getInteger(), val);
    }
    {
      auto val =
          static_cast<uint64_t>(std::numeric_limits<uint32_t>::max()) + 1;
      FancyId id(type, val);
      ASSERT_EQ(id.type(), type);
      ASSERT_EQ(id.getInteger(), val);
    }
  }
}


