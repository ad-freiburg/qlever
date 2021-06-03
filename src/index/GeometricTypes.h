//
// Created by johannes on 03.06.21.
//

#ifndef QLEVER_GEOMETRICTYPES_H
#define QLEVER_GEOMETRICTYPES_H

namespace ad_geo {
struct Point {
  float x, y;
};

struct Rectangle {
  Point lowerLeft, topRight;
};
}

#endif  // QLEVER_GEOMETRICTYPES_H
