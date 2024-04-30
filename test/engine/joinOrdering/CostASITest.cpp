// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
// Mahmoud Khalaf (2024-, khalaf@cs.uni-freiburg.de)

#include <gtest/gtest.h>

// #include "engine/joinOrdering/CostASI.cpp"
#include "engine/joinOrdering/IKKBZ.cpp"
#include "engine/joinOrdering/QueryGraph.cpp"
#include "engine/joinOrdering/RelationBasic.cpp"

#define eps 0.001

using JoinOrdering::QueryGraph, JoinOrdering::RelationBasic,
    JoinOrdering::Direction, JoinOrdering::IKKBZ_merge,
    JoinOrdering::toPrecedenceGraph;

TEST(COSTASI_SANITY, SESSION04_EX1) {
  /**
                  R1

       1/5      |    |    1/3
  +-------------+    +--------------+
  |                                 |

  R2                                  R3
 (20)                                (30)

                          1/10      |    |   1
                     +--------------+    +----------+
                     |                              |

                     R4                            R5
                    (50)                           (2)


                        20/39

   */

  auto R1 = RelationBasic("R1", 1);
  auto R2 = RelationBasic("R2", 20);
  auto R3 = RelationBasic("R3", 30);
  auto R4 = RelationBasic("R4", 50);
  auto R5 = RelationBasic("R5", 2);

  auto g = QueryGraph<RelationBasic>();
  g.add_rjoin(R1, R2, 1.0 / 5);
  g.add_rjoin(R1, R3, 1.0 / 3);
  g.add_rjoin(R3, R4, 1.0 / 10);
  g.add_rjoin(R3, R5, 1.0);

  auto pg = toPrecedenceGraph(g, R1);
  EXPECT_NEAR(JoinOrdering::ASI::rank(pg, R2), 3.0 / 4, eps);
  EXPECT_NEAR(JoinOrdering::ASI::rank(pg, R3), 9.0 / 10, eps);
  EXPECT_NEAR(JoinOrdering::ASI::rank(pg, R4), 4.0 / 5, eps);
  EXPECT_NEAR(JoinOrdering::ASI::rank(pg, R5), 1.0 / 2, eps);

  IKKBZ_merge(pg, R3);
  auto R3R5 = pg.combine(R3, R5);
  ASSERT_EQ(R3R5.getCardinality(), 60);
  EXPECT_NEAR(JoinOrdering::ASI::rank(pg, R3R5), 19.0 / 30, 0.001);
}

TEST(IKKBZ_SANITY, SESSION04_EX2) {
  /*

 R1    1/6
(30)  ----------+
                |
                |

                R3    1/20     R4    3/4      R5      1/2     R6    1/14     R7
               (30)  -------  (20)  ------   (10)    ------  (20)  -------  (70)

                |                            |
 R2    1/10     |                            |
(100) ----------+                            | 1/5
                                             |

                                              R8
                                             (100)

                                             |
                                             | 1/25
                                             |

                                              R9
                                             (100)


                                    25/39
   */

  auto g = QueryGraph<RelationBasic>();

  auto R1 = g.add_relation(RelationBasic("R1", 30));
  auto R2 = g.add_relation(RelationBasic("R2", 100));
  auto R3 = g.add_relation(RelationBasic("R3", 30));
  auto R4 = g.add_relation(RelationBasic("R4", 20));
  auto R5 = g.add_relation(RelationBasic("R5", 10));
  auto R6 = g.add_relation(RelationBasic("R6", 20));
  auto R7 = g.add_relation(RelationBasic("R7", 70));
  auto R8 = g.add_relation(RelationBasic("R8", 100));
  auto R9 = g.add_relation(RelationBasic("R9", 100));

  g.add_rjoin(R1, R3, 1.0 / 6);
  g.add_rjoin(R2, R3, 1.0 / 10);
  g.add_rjoin(R3, R4, 1.0 / 20);
  g.add_rjoin(R4, R5, 3.0 / 4);
  g.add_rjoin(R5, R6, 1.0 / 2);
  g.add_rjoin(R6, R7, 1.0 / 14);
  g.add_rjoin(R5, R8, 1.0 / 5);
  g.add_rjoin(R8, R9, 1.0 / 25);

  auto pg = JoinOrdering::toPrecedenceGraph(g, R1);

  EXPECT_NEAR(JoinOrdering::ASI::rank(pg, R2), 9.0 / 10, eps);
  EXPECT_NEAR(JoinOrdering::ASI::rank(pg, R3), 4.0 / 5, eps);
  EXPECT_NEAR(JoinOrdering::ASI::rank(pg, R4), 0, eps);
  EXPECT_NEAR(JoinOrdering::ASI::rank(pg, R5), 13.0 / 15, eps);
  EXPECT_NEAR(JoinOrdering::ASI::rank(pg, R6), 9.0 / 10, eps);
  EXPECT_NEAR(JoinOrdering::ASI::rank(pg, R7), 4.0 / 5, eps);
  EXPECT_NEAR(JoinOrdering::ASI::rank(pg, R8), 19.0 / 20, eps);
  EXPECT_NEAR(JoinOrdering::ASI::rank(pg, R9), 3.0 / 4, eps);

  auto R6R7 = pg.combine(R6, R7);
  auto R8R9 = pg.combine(R8, R9);

  EXPECT_NEAR(JoinOrdering::ASI::rank(pg, R6R7), 49.0 / 60, eps);
  EXPECT_NEAR(JoinOrdering::ASI::rank(pg, R8R9), 79.0 / 100, eps);

  IKKBZ_merge(pg, R5);

  auto R5R8R9 = pg.combine(R5, R8R9);
  EXPECT_NEAR(JoinOrdering::ASI::rank(pg, R5R8R9), 1198.0 / 1515, eps);
}
