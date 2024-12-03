// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
// Mahmoud Khalaf (2024-, khalaf@cs.uni-freiburg.de)

#include <gtest/gtest.h>

#include "engine/joinOrdering/CostCout.h"
#include "engine/joinOrdering/LinearizedDP.h"
#include "engine/joinOrdering/QueryGraph.h"

using JoinOrdering::RelationBasic, JoinOrdering::JoinTree;

class LinDPJoin1 : public testing::Test {
 protected:
  /*
        R2     1/2                         1/3      R5
       (10)   ---------+             +-----------  (18)
                       |             |

                       R1    1/5     R4
                      (10)  ------  (100)

                       |             |
        R3     1/4     |             |     1/2      R6    1/10     R7
       (100)  ---------+             +-----------  (10)  -------  (20)


                                   124/647
   */

  RelationBasic R1, R2, R3, R4, R5, R6, R7;
  JoinOrdering::QueryGraph<RelationBasic> g;

  LinDPJoin1() {
    R1 = RelationBasic("R1", 10);
    R2 = RelationBasic("R2", 100);
    R3 = RelationBasic("R3", 100);
    R4 = RelationBasic("R4", 100);
    R5 = RelationBasic("R5", 18);
    R6 = RelationBasic("R6", 10);
    R7 = RelationBasic("R7", 20);

    g = JoinOrdering::QueryGraph<RelationBasic>();
    g.add_relation(R1);
    g.add_relation(R2);
    g.add_relation(R3);
    g.add_relation(R4);
    g.add_relation(R5);
    g.add_relation(R6);
    g.add_relation(R7);

    g.add_rjoin(R1, R2, 1.0 / 2);
    g.add_rjoin(R1, R3, 1.0 / 4);
    g.add_rjoin(R1, R4, 1.0 / 5);
    g.add_rjoin(R4, R5, 1.0 / 3);
    g.add_rjoin(R4, R6, 1.0 / 2);
    g.add_rjoin(R6, R7, 1.0 / 10);
  }
};

class LinDPJoin2 : public testing::Test {
 protected:
  RelationBasic A, B, C, D, E, F;
  JoinOrdering::QueryGraph<RelationBasic> g;

  LinDPJoin2() {
    A = RelationBasic("A", 100);
    B = RelationBasic("B", 100);
    C = RelationBasic("C", 50);
    D = RelationBasic("D", 50);
    E = RelationBasic("E", 100);
    F = RelationBasic("F", 100);

    g = JoinOrdering::QueryGraph<RelationBasic>();
    g.add_relation(A);
    g.add_relation(B);
    g.add_relation(C);
    g.add_relation(D);
    g.add_relation(E);
    g.add_relation(F);

    g.add_rjoin(A, B, 0.4);
    g.add_rjoin(B, C, 0.02);
    g.add_rjoin(B, D, 0.04);
    //    g.add_rjoin({C, D}}, E, 0.01); // TODO: hyperedge?
    g.add_rjoin(E, F, 0.5);
  }
};

TEST_F(LinDPJoin1, CAN_JOIN_SAMPLE_1) {
  auto t1 = JoinTree(R1);
  auto t2 = JoinTree(R2);
  auto t3 = JoinTree(R3);
  auto t4 = JoinTree(R4);
  auto t5 = JoinTree(R5);
  auto t6 = JoinTree(R6);
  auto t7 = JoinTree(R7);

  ASSERT_TRUE(JoinOrdering::canJoin(g, t1, t2));
  ASSERT_TRUE(JoinOrdering::canJoin(g, t1, t3));
  ASSERT_TRUE(JoinOrdering::canJoin(g, t1, t4));
  ASSERT_FALSE(JoinOrdering::canJoin(g, t1, t5));
  ASSERT_FALSE(JoinOrdering::canJoin(g, t1, t6));
  ASSERT_FALSE(JoinOrdering::canJoin(g, t1, t7));

  ASSERT_TRUE(JoinOrdering::canJoin(g, t2, t1));
  ASSERT_TRUE(JoinOrdering::canJoin(g, t3, t1));
  ASSERT_TRUE(JoinOrdering::canJoin(g, t4, t1));
  ASSERT_FALSE(JoinOrdering::canJoin(g, t5, t1));
  ASSERT_FALSE(JoinOrdering::canJoin(g, t6, t1));
  ASSERT_FALSE(JoinOrdering::canJoin(g, t7, t1));

  ASSERT_FALSE(JoinOrdering::canJoin(g, t2, t3));
  ASSERT_FALSE(JoinOrdering::canJoin(g, t2, t4));
  ASSERT_FALSE(JoinOrdering::canJoin(g, t3, t2));
  ASSERT_FALSE(JoinOrdering::canJoin(g, t4, t2));

  auto t1t2 = JoinTree(t1, t2);

  ASSERT_TRUE(JoinOrdering::canJoin(g, t1t2, t3));
  ASSERT_TRUE(JoinOrdering::canJoin(g, t1t2, t4));
  ASSERT_FALSE(JoinOrdering::canJoin(g, t1t2, t5));

  auto t4t6 = JoinTree(t4, t6);
  ASSERT_TRUE(JoinOrdering::canJoin(g, t1t2, t4t6));
}

TEST_F(LinDPJoin1, ADAPTIVE_5_16) {
  auto erg = JoinOrdering::linearizedDP(g);
  //  std::cout << erg.expr() << "\n";
  // FIXME: just suppress codecov
  ASSERT_EQ(erg.expr(), "(((((((R2)⋈(R1))⋈(R4))⋈(R6))⋈(R7))⋈(R5))⋈(R3))");
}
