#include <gtest/gtest.h>
#include <cstdlib>
#include "engine/SpatialJoin.h"

TEST(SpatialJoin, dummyTest) {
  ASSERT_EQ(2, 2);
}

// this function creates an input as a test set and returns it
string createTestKnowledgeGraph(bool verbose) {
  auto addPoint = [] (string* kg, double lon, double lat) {
    string name = "Point_" + std::to_string(lon) + "_"
                        + std::to_string(lat) + "";
    *kg += "";
    
    *kg += name;
    *kg += " geo:asWKT Point(";
    *kg += std::to_string(lon);
    *kg += " ";
    *kg += std::to_string(lat);
    *kg += ")^^geo:wktLiteral .\n";

    double fraction = std::abs(lon - (int)lon);
    if (fraction > 0.49 && fraction < 0.51) {
      *kg += name;
      *kg += " <lon-has-fractional-part> <one-half> .\n";
    } else if (fraction > 0.33 && fraction < 0.34) {
      *kg += name;
      *kg += " <lon-has-fractional-part> <one-third> .\n";
    } else if (fraction > 0.66 && fraction < 0.67) {
      *kg += name;
      *kg += " <lon-has-fractional-part> <two-third> .\n";
    } else if (fraction < 0.01) {
      if ((int)lon % 2 == 0) {
        *kg += name;
        *kg += " <lon-is-div-by> <two> .\n";  // divisible by two
      }
      if ((int)lon % 3 == 0) {
        *kg += name;
        *kg += " <lon-is-div-by> <three> .\n";  // divisible by three
      }
      if ((int)lon % 4 == 0) {
        *kg += name;
        *kg += " <lon-is-div-by> <four> .\n";  // divisible by four
      }
      if ((int)lon % 5 == 0) {
        *kg += name;
        *kg += " <lon-is-div-by> <five> .\n";  // divisible by five
      }
    }

    fraction = std::abs(lat - (int)lat);
    if (fraction > 0.49 && fraction < 0.51) {
      *kg += name;
      *kg += " <lat-has-fractional-part> <one-half> .\n";
    } else if (fraction > 0.33 && fraction < 0.34) {
      *kg += name;
      *kg += " <lat-has-fractional-part> <one-third> .\n";
    } else if (fraction > 0.66 && fraction < 0.67) {
      *kg += name;
      *kg += " <lat-has-fractional-part> <two-third> .\n";
    } else if (fraction < 0.01) {
      if ((int)lat % 2 == 0) {
        *kg += name;
        *kg += " <lat-is-div-by> <two> .\n";  // divisible by two
      }
      if ((int)lat % 3 == 0) {
        *kg += name;
        *kg += " <lat-is-div-by> <three> .\n";  // divisible by three
      }
      if ((int)lat % 4 == 0) {
        *kg += name;
        *kg += " <lat-is-div-by> <four> .\n";  // divisible by four
      }
      if ((int)lat % 5 == 0) {
        *kg += name;
        *kg += " <lat-is-div-by> <five> .\n";  // divisible by five
      }
    }
  };

  string kg = "";  // knowlegde graph
  // for loop to iterate over the longitudes
  for (int lon = -90; lon <= 90; lon++) {  // iterate over longitude
    for (int lat = -180; lat < 180; lat++) {  // iterate over latitude
      if (lon == -90 || lon == 90) {
        // only add one point for the poles
        addPoint(&kg, lon, 0);
        break;
      }
      
      
      addPoint(&kg, lon, lat);

      if (!verbose) {
        if (lon % 2 == 1
              || (lat > -160 && lat < -20)
              || (lat > 20 && lat < 160) ) {
          continue;
        }
      }
      
      addPoint(&kg, lon, lat + 1/3.0);
      addPoint(&kg, lon, lat + 1/2.0);
      addPoint(&kg, lon, lat + 2/3.0);

      addPoint(&kg, lon + 1/3.0, lat);
      addPoint(&kg, lon + 1/3.0, lat + 1/3.0);
      addPoint(&kg, lon + 1/3.0, lat + 1/2.0);
      addPoint(&kg, lon + 1/3.0, lat + 2/3.0);
      
      addPoint(&kg, lon + 1/2.0, lat);
      addPoint(&kg, lon + 1/2.0, lat + 1/3.0);
      addPoint(&kg, lon + 1/2.0, lat + 1/2.0);
      addPoint(&kg, lon + 1/2.0, lat + 2/3.0);
      
      addPoint(&kg, lon + 2/3.0, lat);
      addPoint(&kg, lon + 2/3.0, lat + 1/3.0);
      addPoint(&kg, lon + 2/3.0, lat + 1/2.0);
      addPoint(&kg, lon + 2/3.0, lat + 2/3.0);
    }
  }
  return kg;
}