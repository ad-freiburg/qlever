// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
// Mahmoud Khalaf (2024-, khalaf@cs.uni-freiburg.de)

#include <gtest/gtest.h>

#include "engine/joinOrdering/CostASI.cpp"
#include "engine/joinOrdering/IKKBZ.cpp"
#include "engine/joinOrdering/QueryGraph.cpp"
#include "engine/joinOrdering/RelationBasic.cpp"

using JoinOrdering::QueryGraph, JoinOrdering::RelationBasic;

TEST(IKKBZ_SANITY, EX1_R1toR7) {
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

  auto g = QueryGraph<RelationBasic>();
  auto R1 = g.add_relation(RelationBasic("R1", 10));
  auto R2 = g.add_relation(RelationBasic("R2", 100));
  auto R3 = g.add_relation(RelationBasic("R3", 100));
  auto R4 = g.add_relation(RelationBasic("R4", 100));
  auto R5 = g.add_relation(RelationBasic("R5", 18));
  auto R6 = g.add_relation(RelationBasic("R6", 10));
  auto R7 = g.add_relation(RelationBasic("R7", 20));

  g.add_rjoin(R1, R2, 1.0 / 2);
  g.add_rjoin(R1, R3, 1.0 / 4);
  g.add_rjoin(R1, R4, 1.0 / 5);
  g.add_rjoin(R4, R5, 1.0 / 3);
  g.add_rjoin(R4, R6, 1.0 / 2);
  g.add_rjoin(R6, R7, 1.0 / 10);

  auto g_R1 = IKKBZ(g, R1);
  auto g_R2 = IKKBZ(g, R2);
  auto g_R3 = IKKBZ(g, R3);
  auto g_R4 = IKKBZ(g, R4);
  auto g_R5 = IKKBZ(g, R5);
  auto g_R6 = IKKBZ(g, R6);
  auto g_R7 = IKKBZ(g, R7);

  ASSERT_EQ(g_R1.iter(), (std::vector{R1, R4, R6, R7, R5, R3, R2}));
  ASSERT_EQ(g_R2.iter(), (std::vector{R2, R1, R4, R6, R7, R5, R3}));
  ASSERT_EQ(g_R3.iter(), (std::vector{R3, R1, R4, R6, R7, R5, R2}));
  ASSERT_EQ(g_R4.iter(), (std::vector{R4, R6, R7, R1, R5, R3, R2}));
  ASSERT_EQ(g_R5.iter(), (std::vector{R5, R4, R6, R7, R1, R3, R2}));
  ASSERT_EQ(g_R6.iter(), (std::vector{R6, R7, R4, R1, R5, R3, R2}));
  ASSERT_EQ(g_R7.iter(), (std::vector{R7, R6, R4, R1, R5, R3, R2}));
}

TEST(IKKBZ_SANITY, EX2_R1) {
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

  auto g2_R1 = JoinOrdering::IKKBZ(g, R1);
  ASSERT_EQ(g2_R1.iter(), (std::vector({R1, R3, R4, R5, R8, R9, R6, R7, R2})));
}

TEST(IKKBZ_SANITY, PrecedenceGraph1) {
  /**

   R1  -+         +-  R5
        |         |

       R3   ---  R4

        |         |
   R2  -+         +-  R6

       query graph



    R1

     |
     |
     v

    R3   -->  R2

     |
     |
     v

    R4   -->  R6

     |
     |
     v

    R5


   precedence graph rooted in R1

   ref: 107/637
   */

  auto g = QueryGraph<RelationBasic>();
  auto R1 = g.add_relation(RelationBasic("R1", 1));
  auto R2 = g.add_relation(RelationBasic("R2", 1));
  auto R3 = g.add_relation(RelationBasic("R3", 1));
  auto R4 = g.add_relation(RelationBasic("R4", 1));
  auto R5 = g.add_relation(RelationBasic("R5", 1));
  auto R6 = g.add_relation(RelationBasic("R6", 1));

  g.add_rjoin(R1, R3, 1);
  g.add_rjoin(R2, R3, 1);
  g.add_rjoin(R3, R4, 1);
  g.add_rjoin(R4, R5, 1);
  g.add_rjoin(R4, R6, 1);

  auto pg = JoinOrdering::toPrecedenceGraph(g, R1);

  ASSERT_TRUE(pg.has_rjoin(R1, R3));
  ASSERT_EQ(pg.edges_[R1][R3].direction, JoinOrdering::Direction::PARENT);

  ASSERT_TRUE(pg.has_rjoin(R2, R3));
  ASSERT_EQ(pg.edges_[R3][R2].direction, JoinOrdering::Direction::PARENT);

  ASSERT_TRUE(pg.has_rjoin(R3, R4));
  ASSERT_EQ(pg.edges_[R3][R4].direction, JoinOrdering::Direction::PARENT);

  ASSERT_TRUE(pg.has_rjoin(R4, R5));
  ASSERT_EQ(pg.edges_[R4][R5].direction, JoinOrdering::Direction::PARENT);

  ASSERT_TRUE(pg.has_rjoin(R4, R6));
  ASSERT_EQ(pg.edges_[R4][R6].direction, JoinOrdering::Direction::PARENT);
}

TEST(IKKBZ_SANITY, IKKBZ_ARGMIN_EX1) {
  auto g = QueryGraph<RelationBasic>();
  auto R1 = g.add_relation(RelationBasic("R1", 10));
  auto R2 = g.add_relation(RelationBasic("R2", 100));
  auto R3 = g.add_relation(RelationBasic("R3", 100));
  auto R4 = g.add_relation(RelationBasic("R4", 100));
  auto R5 = g.add_relation(RelationBasic("R5", 18));
  auto R6 = g.add_relation(RelationBasic("R6", 10));
  auto R7 = g.add_relation(RelationBasic("R7", 20));

  g.add_rjoin(R1, R2, 1.0 / 2);
  g.add_rjoin(R1, R3, 1.0 / 4);
  g.add_rjoin(R1, R4, 1.0 / 5);
  g.add_rjoin(R4, R5, 1.0 / 3);
  g.add_rjoin(R4, R6, 1.0 / 2);
  g.add_rjoin(R6, R7, 1.0 / 10);

  ASSERT_EQ(IKKBZ(g), (std::vector{R2, R1, R4, R6, R7, R5, R3}));
}

TEST(IKKBZ_SANITY, IKKBZ_ARGMIN_EX2) {
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

  //  ASSERT_EQ(IKKBZ(g), (std::vector({R8, R5, R4, R9, R1, R3, R6, R7, R2})));
  ASSERT_EQ(IKKBZ(g), (std::vector({R8, R5, R4, R9, R3, R1, R6, R7, R2})));
}

TEST(IKKBZ_SANITY, KRISHNAMURTHY1986_133) {
  /**

                          R1
                         (100)

            1/10      |    |        1
  +-------------------+    +------------------+
  |                                           |

    R2                                        R3
 (1000000)                                  (1000)

                                  1/30      |    |   1
                         +------------------+    +----------+
                         |                                  |

                         R4                                 R5
                      (150000)                             (50)


                               133

   */
  auto g = QueryGraph<RelationBasic>();

  auto R1 = g.add_relation(RelationBasic("R1", 100));
  auto R2 = g.add_relation(RelationBasic("R2", 1000000));
  auto R3 = g.add_relation(RelationBasic("R3", 1000));
  auto R4 = g.add_relation(RelationBasic("R4", 150000));
  auto R5 = g.add_relation(RelationBasic("R5", 50));

  g.add_rjoin(R1, R2, 1.0 / 100);
  g.add_rjoin(R1, R3, 1.0 / 1);
  g.add_rjoin(R3, R4, 1.0 / 30);
  g.add_rjoin(R3, R5, 1.0 / 1);

  ASSERT_EQ(IKKBZ(g, R1).iter(), (std::vector({R1, R3, R5, R4, R2})));
}
