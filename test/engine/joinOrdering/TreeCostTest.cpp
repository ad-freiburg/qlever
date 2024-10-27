// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
// Mahmoud Khalaf (2024-, khalaf@cs.uni-freiburg.de)

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "engine/joinOrdering/CostCout.h"
#include "engine/joinOrdering/JoinTree.h"
#include "engine/joinOrdering/RelationBasic.h"

using JoinOrdering::JoinTree, JoinOrdering::RelationBasic,
    JoinOrdering::JoinType;

class LinearTreeSanity : public testing::Test {
 protected:
  RelationBasic R1, R2, R3, R4, R5, R6, R7;
  LinearTreeSanity() {
    R1 = RelationBasic("R1", 10);
    R2 = RelationBasic("R2", 100);
    R3 = RelationBasic("R3", 100);
    R4 = RelationBasic("R4", 100);
    R5 = RelationBasic("R5", 18);
    R6 = RelationBasic("R6", 10);
    R7 = RelationBasic("R7", 20);
  }
};

class LinearTreeCost1 : public testing::Test {
 protected:
  RelationBasic R1, R2, R3;
  std::map<std::string, unsigned long long> cardinalities;
  std::map<std::string, std::map<std::string, float>> selectivities;
  LinearTreeCost1() {
    R1 = RelationBasic("R1", 10);
    R2 = RelationBasic("R2", 100);
    R3 = RelationBasic("R3", 1000);

    cardinalities["R1"] = 10;
    cardinalities["R2"] = 100;
    cardinalities["R3"] = 1000;

    selectivities["R1"]["R2"] = 0.1;
    selectivities["R2"]["R1"] = 0.1;

    selectivities["R2"]["R3"] = 0.2;
    selectivities["R3"]["R2"] = 0.2;

    selectivities["R1"]["R3"] = 1;
    selectivities["R3"]["R1"] = 1;
  }
};

class LinearTreeCost2 : public testing::Test {
 protected:
  RelationBasic R1, R2, R3;
  std::map<std::string, unsigned long long> cardinalities;
  std::map<std::string, std::map<std::string, float>> selectivities;
  LinearTreeCost2() {
    R1 = RelationBasic("R1", 1000);
    R2 = RelationBasic("R2", 2);
    R3 = RelationBasic("R3", 2);

    cardinalities["R1"] = 1000;
    cardinalities["R2"] = 2;
    cardinalities["R3"] = 2;

    selectivities["R1"]["R2"] = 0.1;
    selectivities["R2"]["R1"] = 0.1;

    selectivities["R2"]["R3"] = 1.0;
    selectivities["R3"]["R2"] = 1.0;

    selectivities["R1"]["R3"] = 0.1;
    selectivities["R3"]["R1"] = 0.1;
  }
};

class LinearTreeCost3 : public testing::Test {
 protected:
  RelationBasic R1, R2, R3, R4;
  std::map<std::string, unsigned long long> cardinalities;
  std::map<std::string, std::map<std::string, float>> selectivities;
  LinearTreeCost3() {
    R1 = RelationBasic("R1", 10);
    R2 = RelationBasic("R2", 20);
    R3 = RelationBasic("R3", 20);
    R4 = RelationBasic("R4", 10);

    cardinalities["R1"] = 10;
    cardinalities["R2"] = 20;
    cardinalities["R3"] = 20;
    cardinalities["R4"] = 10;

    selectivities["R1"]["R2"] = 0.01;
    selectivities["R2"]["R1"] = 0.01;

    selectivities["R1"]["R3"] = 1.0;
    selectivities["R3"]["R1"] = 1.0;

    selectivities["R1"]["R4"] = 1.0;
    selectivities["R4"]["R1"] = 1.0;

    selectivities["R2"]["R3"] = 0.5;
    selectivities["R3"]["R2"] = 0.5;

    selectivities["R2"]["R4"] = 1.0;
    selectivities["R4"]["R2"] = 1.0;

    selectivities["R3"]["R4"] = 0.01;
    selectivities["R4"]["R3"] = 0.01;
  }
};

TEST_F(LinearTreeSanity, JOIN_RELATION_LABELS) {
  auto t1 = JoinTree(R1, R2);
  auto t2 = JoinTree(R3, R4);
  auto tt = JoinTree(t1, t2);

  ASSERT_EQ(tt.root->left->left->relation.getLabel(), "R1");
  ASSERT_EQ(tt.root->left->right->relation.getLabel(), "R2");
  ASSERT_EQ(tt.root->right->left->relation.getLabel(), "R3");
  ASSERT_EQ(tt.root->right->right->relation.getLabel(), "R4");

  EXPECT_THAT(t1.relations_iter_str(), testing::ElementsAre("R1", "R2"));
  EXPECT_THAT(t2.relations_iter_str(), testing::ElementsAre("R3", "R4"));
  EXPECT_THAT(tt.relations_iter_str(),
              testing::ElementsAre("R1", "R2", "R3", "R4"));
}

/**
        ⋈
       / \
      /   \
     /     \
    ⋈       ⋈
   / \     / \
  R1  R2  R3  R4

 */
TEST_F(LinearTreeSanity, CONSTRUCT_2_JOIN_TREES) {
  auto tt = JoinTree(JoinTree(R1, R2), JoinTree(R3, R4), JoinType::BOWTIE);
  ASSERT_EQ(tt.expr(), "((R1⋈R2)⋈(R3⋈R4))");
}

/**
        ⋈
       / \
      ⋈   ⋈
     / \   \
    R1  R2  R5
 */
TEST_F(LinearTreeSanity, CONSTRUCT_2_1_JOIN_TREES) {
  auto tt = JoinTree(JoinTree(R1, R2, JoinType::BOWTIE), JoinTree(R5),
                     JoinType::BOWTIE);
  ASSERT_EQ(tt.expr(), "((R1⋈R2)⋈(R5))");
}

/**
          ⋈
         / \
        ⋈   R3
       / \
      /   \
     /     \
    ⋈       ⋈
   / \     / \
  R1  R2  R4  R5

 */
TEST_F(LinearTreeSanity, CONSTRUCT_3_JOIN_TREES) {
  auto t1 = JoinTree(R1, R2);
  auto t2 = JoinTree(R4, R5);
  auto t3 = JoinTree(R3);
  auto tt = JoinTree(JoinTree(t1, t2), t3);
  ASSERT_EQ(tt.expr(), "(((R1⋈R2)⋈(R4⋈R5))⋈(R3))");
}

/**
          x
         / \
        ⋈   R3
       / \
      /   \
     /     \
    x       ⋈
   / \     / \
  R1  R2  R4  R5

 */
TEST_F(LinearTreeSanity, CONSTRUCT_3_1_JOIN_TREES) {
  auto t1 = JoinTree(R1, R2, JoinType::CROSS);
  auto t2 = JoinTree(R4, R5, JoinType::BOWTIE);
  auto t3 = JoinTree(R3);
  auto tt = JoinTree(JoinTree(t1, t2), t3, JoinType::CROSS);
  ASSERT_EQ(tt.expr(), "(((R1xR2)⋈(R4⋈R5))x(R3))");
}

/**
 *
 * +------------------+---------+
 * |                  | C_{out} |
 * +------------------+---------+
 * | R1 ⋈ R2          |     100 |
 * | R2 ⋈ R3          |   20000 |
 * | R1 x R3          |   10000 |
 * | ((R1 ⋈ R2) ⋈ R3) |   20100 |
 * | ((R2 ⋈ R3) ⋈ R1) |   40000 |
 * | (R1 x R3) ⋈ R2   |   30000 |
 * +------------------+---------+
 *
 * ref: 82/637
 */
TEST_F(LinearTreeCost1, SAMPLE_COST_CALC_1) {
  auto t1 = JoinTree(R1, R2, JoinType::BOWTIE);
  auto t2 = JoinTree(R2, R3, JoinType::BOWTIE);
  auto t3 = JoinTree(R1, R3, JoinType::CROSS);

  auto t4 = JoinTree(t1, JoinTree(R3), JoinType::BOWTIE);
  auto t5 = JoinTree(t2, JoinTree(R1), JoinType::BOWTIE);
  auto t6 = JoinTree(JoinTree(R1, R3, JoinType::CROSS), JoinTree(R2),
                     JoinType::BOWTIE);

  ASSERT_EQ(JoinOrdering::Cost::Cout(t1, cardinalities, selectivities), 100);
  ASSERT_EQ(JoinOrdering::Cost::Cout(t2, cardinalities, selectivities), 20000);
  ASSERT_EQ(JoinOrdering::Cost::Cout(t3, cardinalities, selectivities), 10000);
  ASSERT_EQ(JoinOrdering::Cost::Cout(t4, cardinalities, selectivities), 20100);
  ASSERT_EQ(JoinOrdering::Cost::Cout(t5, cardinalities, selectivities), 40000);
  ASSERT_EQ(JoinOrdering::Cost::Cout(t6, cardinalities, selectivities), 30000);
}

/**
 *
 * +------------------+---------+
 * |                  | C_{out} |
 * +------------------+---------+
 * | R1 ⋈ R2          |     200 |
 * | R2 x R3          |       4 |
 * | R1 ⋈ R3          |     200 |
 * | ((R1 ⋈ R2) ⋈ R3) |     240 |
 * | ((R2 x R3) ⋈ R1) |      44 |
 * | (R1 ⋈ R3) ⋈ R2   |     240 |
 * +------------------+---------+
 *
 * ref: 83/637
 */
TEST_F(LinearTreeCost2, SAMPLE_COST_CALC_2) {
  auto t1 = JoinTree(R1, R2, JoinType::BOWTIE);
  auto t2 = JoinTree(R2, R3, JoinType::CROSS);
  auto t3 = JoinTree(R1, R3, JoinType::BOWTIE);

  auto t4 = JoinTree(t1, JoinTree(R3), JoinType::BOWTIE);
  auto t5 = JoinTree(t2, JoinTree(R1), JoinType::CROSS);
  auto t6 = JoinTree(JoinTree(R1, R3, JoinType::BOWTIE), JoinTree(R2),
                     JoinType::BOWTIE);

  ASSERT_EQ(JoinOrdering::Cost::Cout(t1, cardinalities, selectivities), 200);
  ASSERT_EQ(JoinOrdering::Cost::Cout(t2, cardinalities, selectivities), 4);
  ASSERT_EQ(JoinOrdering::Cost::Cout(t3, cardinalities, selectivities), 200);
  ASSERT_EQ(JoinOrdering::Cost::Cout(t4, cardinalities, selectivities), 240);
  ASSERT_EQ(JoinOrdering::Cost::Cout(t5, cardinalities, selectivities), 44);
  ASSERT_EQ(JoinOrdering::Cost::Cout(t6, cardinalities, selectivities), 240);
}

/**
 * +-----------------------+---------+
 * |                       | C_{out} |
 * +-----------------------+---------+
 * | R1 ⋈ R2               |       2 |
 * | R2 ⋈ R3               |     200 |
 * | R3 ⋈ R4               |       2 |
 * | ((R1 ⋈ R2) ⋈ R3) ⋈ R4 |      24 |
 * | ((R2 x R3) ⋈ R1) ⋈ R4 |     222 |
 * | (R1 ⋈ R2) ⋈ (R3 ⋈ R4) |       6 |
 * +-----------------------+---------+
 *
 * ref: 84/637
 */
TEST_F(LinearTreeCost3, SAMPLE_COST_CALC_3) {
  auto t1 = JoinTree(R1, R2, JoinType::BOWTIE);
  auto t2 = JoinTree(R2, R3, JoinType::BOWTIE);
  auto t3 = JoinTree(R3, R4, JoinType::BOWTIE);

  auto t4 =
      JoinTree(JoinTree(t1, JoinTree(R3), JoinType::BOWTIE), JoinTree(R4));

  auto t5 = JoinTree(JoinTree(JoinTree(R2, R3, JoinType::CROSS), JoinTree(R1),
                              JoinType::BOWTIE),
                     JoinTree(R4), JoinType::BOWTIE);

  auto t6 = JoinTree(JoinTree(R1, R2), JoinTree(R3, R4), JoinType::BOWTIE);

  ASSERT_EQ(JoinOrdering::Cost::Cout(t1, cardinalities, selectivities), 2);
  ASSERT_EQ(JoinOrdering::Cost::Cout(t2, cardinalities, selectivities), 200);
  ASSERT_EQ(JoinOrdering::Cost::Cout(t3, cardinalities, selectivities), 2);
  ASSERT_EQ(JoinOrdering::Cost::Cout(t4, cardinalities, selectivities), 24);
  ASSERT_EQ(JoinOrdering::Cost::Cout(t5, cardinalities, selectivities), 222);
  ASSERT_EQ(JoinOrdering::Cost::Cout(t6, cardinalities, selectivities), 6);
}
