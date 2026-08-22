// Copyright 2024, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#include <absl/strings/str_split.h>
#include <gmock/gmock.h>

#include <array>
#include <memory>

#include "backports/span.h"
#include "util/FsstCompressor.h"

TEST(FsstEncoder, firstTest) {
  std::vector<std::string> s{
      "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam "
      "nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, "
      "sed diam voluptua. At vero eos et accusam et justo duo dolores et ea "
      "rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem "
      "ipsum dolor sit amet. Lorem ipsum dolor sit amet, consetetur sadipscing "
      "elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna "
      "aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo "
      "dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus "
      "est Lorem ipsum dolor sit amet. Lorem ipsum dolor sit amet, consetetur "
      "sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et "
      "dolore magna aliquyam erat, sed diam voluptua. At vero eos et accusam "
      "et justo duo dolores et ea rebum. Stet clita kasd gubergren, no sea "
      "takimata sanctus est Lorem ipsum dolor sit amet. \n"
      "\n"
      "Duis autem vel eum iriure dolor in hendrerit in vulputate velit esse "
      "molestie consequat, vel illum dolore eu feugiat nulla facilisis at vero "
      "eros et accumsan et iusto odio dignissim qui blandit praesent luptatum "
      "zzril delenit augue duis dolore te feugait nulla facilisi. Lorem ipsum "
      "dolor sit amet, consectetuer adipiscing elit, sed diam nonummy nibh "
      "euismod tincidunt ut laoreet dolore magna aliquam erat volutpat. \n"
      "\n"
      "Ut wisi enim ad minim veniam, quis nostrud exerci tation ullamcorper "
      "suscipit lobortis nisl ut aliquip ex ea commodo consequat. Duis autem "
      "vel eum iriure dolor in hendrerit in vulputate velit esse molestie "
      "consequat, vel illum dolore eu feugiat nulla facilisis at vero eros et "
      "accumsan et iusto odio dignissim qui blandit praesent luptatum zzril "
      "delenit augue duis dolore te feugait nulla facilisi. \n"
      "\n"
      "Nam liber tempor cum soluta nobis eleifend option congue nihil "
      "imperdiet doming id quod mazim placerat facer possim assum. Lorem ipsum "
      "dolor sit amet, consectetuer adipiscing elit, sed diam nonummy nibh "
      "euismod tincidunt ut laoreet dolore magna aliquam erat volutpat. Ut "
      "wisi enim ad minim veniam, quis nostrud exerci tation ullamcorper "
      "suscipit lobortis nisl ut aliquip ex ea commodo consequat. \n"
      "\n"
      "Duis autem vel eum iriure dolor in hendrerit in vulputate velit esse "
      "molestie consequat, vel illum dolore eu feugiat nulla facilisis. \n"
      "\n"
      "At vero eos et accusam et justo duo dolores et ea rebum. Stet clita "
      "kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet. "
      "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam "
      "nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, "
      "sed diam voluptua. At vero eos et accusam et justo duo dolores et ea "
      "rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem "
      "ipsum dolor sit amet. Lorem ipsum dolor sit amet, consetetur sadipscing "
      "elitr, At accusam aliquyam diam diam dolore dolores duo eirmod eos "
      "erat, et nonumy sed tempor et et invidunt justo labore Stet clita ea et "
      "gubergren, kasd magna no rebum. sanctus sea sed takimata ut vero "
      "voluptua. est Lorem ipsum dolor sit amet. Lorem ipsum dolor sit amet, "
      "consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut "
      "labore et dolore magna aliquyam erat. \n"
      "\n"
      "Consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut "
      "labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et "
      "accusam et justo duo dolores et ea rebum. Stet clita kasd gubergren, no "
      "sea takimata sanctus est Lorem ipsum dolor sit amet. Lorem ipsum dolor "
      "sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor "
      "invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At "
      "vero eos et accusam et justo duo dolores et ea rebum. Stet clita kasd "
      "gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet. "
      "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam "
      "nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, "
      "sed diam voluptua. At vero eos et accusam et justo duo dolores et ea "
      "rebum. Stet clita kasd gubergren, no sea takimata sanctus. Lorem ipsum "
      "dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod "
      "tempor invidunt ut labore et dolore magna aliquyam erat, sed diam "
      "voluptua. At vero eos et accusam et justo duo dolores et ea rebum. Stet "
      "clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit "
      "amet. Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam "
      "nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, "
      "sed diam voluptua. At vero eos et accusam et justo duo dolores et ea "
      "rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem "
      "ipsum dolor sit amet. Lorem ipsum dolor sit amet, consetetur sadipscing "
      "elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna "
      "aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo "
      "dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus "
      "est Lorem ipsum dolor sit amet. \n"
      "\n"
      "Duis autem vel eum iriure dolor in hendrerit in vulputate velit esse "
      "molestie consequat, vel illum dolore eu feugiat nulla facilisis at vero "
      "eros et accumsan et iusto odio dignissim qui blandit praesent luptatum "
      "zzril delenit augue duis dolore te feugait nulla facilisi. Lorem ipsum "
      "dolor sit amet, consectetuer adipiscing elit, sed diam nonummy nibh "
      "euismod tincidunt ut laoreet dolore magna aliquam erat volutpat. \n"
      "\n"
      "Ut wisi enim ad minim veniam, quis nostrud exerci tation ullamcorper "
      "suscipit lobortis nisl ut aliquip ex ea commodo consequat. Duis autem "
      "vel eum iriure dolor in hendrerit in vulputate velit esse molestie "
      "consequat, vel illum dolore eu feugiat nulla facilisis at vero eros et "
      "accumsan et iusto odio dignissim qui blandit praesent luptatum zzril "
      "delenit augue duis dolore te feugait nulla facilisi. \n"
      "\n"
      "Nam liber tempor cum soluta nobis eleifend option congue nihil "
      "imperdiet doming id quod mazim placerat facer possim assum. Lorem ipsum "
      "dolor sit amet, consectetuer adipiscing elit, sed diam nonummy nibh "
      "euismod tincidunt ut laoreet dolore magna aliquam erat volutpat. Ut "
      "wisi enim ad minim veniam, quis nostrud exerci tation ullamcorper "
      "suscipit lobortis nisl ut aliquip ex ea commodo consequat. \n"
      "\n"
      "Duis autem vel eum iriure dolor in hendrerit in vulputate velit esse "
      "molestie consequat, vel illum dolore eu feugiat nulla facilisis. \n"
      "\n"
      "At vero eos et accusam et justo duo dolores et ea rebum. Stet clita "
      "kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet. "
      "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam "
      "nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, "
      "sed diam voluptua. At vero eos et accusam et justo duo dolores et ea "
      "rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem "
      "ipsum dolor sit amet. Lorem ipsum dolor sit amet, consetetur sadipscing "
      "elitr, At accusam aliquyam diam diam dolore dolores duo eirmod eos "
      "erat, et nonumy sed tempor et et invidunt justo labore Stet clita ea et "
      "gubergren, kasd magna no rebum. sanctus sea sed takimata ut vero "
      "voluptua. est Lorem ipsum dolor sit amet. Lorem ipsum dolor sit amet, "
      "consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut "
      "labore et dolore magna aliquyam erat. \n"
      "\n"
      "Consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut "
      "labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et "
      "accusam et justo duo dolores et ea rebum. Stet clita kasd gubergren, no "
      "sea takimata sanctus est Lorem ipsum dolor sit amet. Lorem ipsum dolor "
      "sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor "
      "invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At "
      "vero eos et accusam et justo duo dolores et ea rebum. Stet clita kasd "
      "gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet. "
      "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam "
      "nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, "
      "sed diam voluptua. At vero eos et accusam et justo duo dolores et ea "
      "rebum. Stet clita kasd gubergren, no sea takimata sanctus. Lorem ipsum "
      "dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod "
      "tempor invidunt ut labore et dolore magna aliquyam erat, sed diam "
      "voluptua. At vero eos et accusam et justo duo dolores et ea rebum. Stet "
      "clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit "
      "amet. Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam "
      "nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, "
      "sed diam voluptua. At vero eos et accusam et justo duo dolores et ea "
      "rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem "
      "ipsum dolor sit amet. Lorem ipsum dolor sit amet, consetetur sadipscing "
      "elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna "
      "aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo "
      "dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus "
      "est Lorem ipsum dolor sit amet. \n"
      "\n"
      "Duis autem vel eum iriure dolor in hendrerit in vulputate velit esse "
      "molestie consequat, vel illum dolore eu feugiat nulla facilisis at vero "
      "eros et accumsan et iusto odio dignissim qui blandit praesent luptatum "
      "zzril delenit augue duis dolore te feugait nulla facilisi. Lorem ipsum "
      "dolor sit amet, consectetuer adipiscing elit, sed diam nonummy nibh "
      "euismod tincidunt ut laoreet dolore magna aliquam erat volutpat. \n"
      "\n"
      "Ut wisi enim ad minim veniam, quis nostrud exerci tation ullamcorper "
      "suscipit lobortis nisl ut aliquip ex ea commodo consequat. Duis autem "
      "vel eum iriure dolor in hendrerit in vulputate velit esse molestie "
      "consequat, vel illum dolore eu feugiat nulla facilisis at vero eros et "
      "accumsan et iusto odio dignissim qui blandit praesent luptatum zzril "
      "delenit augue duis dolore te feugait nulla facilisi. \n"
      "\n"
      "Nam liber tempor cum soluta nobis eleifend option congue nihil "
      "imperdiet doming id quod mazim placerat facer possim assum. Lorem ipsum "
      "dolor sit amet, consectetuer adipiscing elit, sed diam nonummy nibh "
      "euismod tincidunt ut laoreet dolore magna aliquam erat volutpat. Ut "
      "wisi enim ad minim veniam, quis nostrud exerci tation ullamcorper "
      "suscipit lobortis nisl ut aliquip ex ea commodo consequat. \n"
      "\n"
      "Duis autem vel eum iriure dolor in hendrerit in vulputate velit esse "
      "molestie consequat, vel illum dolore eu feugiat nulla facilisis. \n"
      "\n"
      "At vero eos et accusam et justo duo dolores et ea rebum. Stet clita "
      "kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet. "
      "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam "
      "nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, "
      "sed diam voluptua. At vero eos et accusam et justo duo dolores et ea "
      "rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem "
      "ipsum dolor sit amet. Lorem ipsum dolor sit amet, consetetur sadipscing "
      "elitr, At accusam aliquyam diam diam dolore dolores duo eirmod eos "
      "erat, et nonumy sed tempor et et invidunt justo labore Stet clita ea et "
      "gubergren, kasd magna no rebum. sanctus sea sed takimata ut vero "
      "voluptua. est Lorem ipsum dolor sit amet. Lorem ipsum dolor sit amet, "
      "consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut "
      "labore et dolore magna aliquyam erat. \n"
      "\n"
      "Consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut "
      "labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et "
      "accusam et justo duo dolores et ea rebum. Stet clita kasd gubergren, no "
      "sea takimata sanctus est Lorem ipsum dolor sit amet. Lorem ipsum dolor "
      "sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor "
      "invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At "
      "vero eos et accusam et justo duo dolores et ea rebum. Stet clita kasd "
      "gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet. "
      "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam "
      "nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, "
      "sed diam voluptua. At vero eos et accusam et justo duo dolores et ea "
      "rebum. Stet clita kasd gubergren, no sea takimata sanctus. Lorem ipsum "
      "dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod "
      "tempor invidunt ut labore et dolore magna aliquyam erat, sed diam "
      "voluptua. At vero eos et accusam et justo duo dolores et ea rebum. Stet "
      "clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit "
      "amet. Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam "
      "nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, "
      "sed diam voluptua. At vero eos et accusam et justo duo dolores et ea "
      "rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem "
      "ipsum dolor sit amet. Lorem ipsum dolor sit amet, consetetur sadipscing "
      "elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna "
      "aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo "
      "dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus "
      "est Lorem ipsum dolor sit amet. \n"
      "\n"
      "Duis autem vel eum iriure dolor in hendrerit in vulputate velit esse "
      "molestie consequat, vel illum dolore eu feugiat nulla facilisis at vero "
      "eros et accumsan et iusto odio dignissim qui blandit praesent luptatum "
      "zzril delenit augue duis dolore te feugait nulla facilisi. Lorem ipsum "
      "dolor sit amet, consectetuer adipiscing elit, sed diam nonummy nibh "
      "euismod tincidunt ut laoreet dolore magna aliquam erat volutpat. \n"
      "\n"
      "Ut wisi enim ad minim veniam, quis nostrud exerci tation ullamcorper "
      "suscipit lobortis nisl ut aliquip ex ea commodo consequat. Duis autem "
      "vel eum iriure dolor in hendrerit in vulputate velit esse molestie "
      "consequat, vel illum dolore eu feugiat nulla facilisis at vero eros et "
      "accumsan et iusto odio dignissim qui blandit praesent luptatum zzril "
      "delenit augue duis dolore te feugait nulla facilisi. \n"
      "\n"
      "Nam liber tempor cum soluta nobis eleifend option congue nihil "
      "imperdiet doming id quod mazim placerat facer possim assum. Lorem ipsum "
      "dolor sit amet, consectetuer adipiscing elit, sed diam nonummy nibh "
      "euismod tincidunt ut laoreet dolore magna aliquam erat volutpat. Ut "
      "wisi enim ad minim veniam, quis nostrud exerci tation ullamcorper "
      "suscipit lobortis nisl ut aliquip ex ea commodo consequat. \n"
      "\n"
      "Duis autem vel eum iriure dolor in hendrerit in vulputate velit esse "
      "molestie consequat, vel illum dolore eu feugiat nulla facilisis. \n"
      "\n"
      "At vero eos et accusam et justo duo dolores et ea rebum. Stet clita "
      "kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet. "
      "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam "
      "nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, "
      "sed diam voluptua. At vero eos et accusam et justo duo dolores et ea "
      "rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem "
      "ipsum dolor sit amet. Lorem ipsum dolor sit amet, consetetur sadipscing "
      "elitr, At accusam aliquyam diam diam dolore dolores duo eirmod eos "
      "erat, et nonumy sed tempor et et invidunt justo labore Stet clita ea et "
      "gubergren, kasd magna no rebum. sanctus sea sed takimata ut vero "
      "voluptua. est Lorem ipsum dolor sit amet. Lorem ipsum dolor sit amet, "
      "consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut "
      "labore et dolore magna aliquyam erat. \n"
      "\n"
      "Consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut "
      "labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et "
      "accusam et justo duo dolores et ea rebum. Stet clita kasd gubergren, no "
      "sea takimata sanctus est Lorem ipsum dolor sit amet. Lorem ipsum dolor "
      "sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor "
      "invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At "
      "vero eos et accusam et justo duo dolores et ea rebum. Stet clita kasd "
      "gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet. "
      "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam "
      "nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, "
      "sed diam voluptua. At vero eos et accusam et justo duo dolores et ea "
      "rebum. Stet clita kasd gubergren, no sea takimata sanctus. Lorem ipsum "
      "dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod "
      "tempor invidunt ut labore et dolore magna aliquyam erat, sed diam "
      "voluptua. At vero eos et accusam et justo duo dolores et ea rebum. Stet "
      "clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit "
      "amet. Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam "
      "nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, "
      "sed diam voluptua. At vero eos et accusam et justo duo dolores et ea "
      "rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem "
      "ipsum dolor sit amet. Lorem ipsum dolor sit amet, consetetur sadipscing "
      "elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna "
      "aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo "
      "dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus "
      "est Lorem ipsum dolor sit amet. \n"
      "\n"
      "Duis autem vel eum iriure dolor in hendrerit in vulputate velit esse "
      "molestie consequat, vel illum dolore eu feugiat nulla facilisis at vero "
      "eros et accumsan et iusto odio dignissim qui blandit praesent luptatum "
      "zzril delenit augue duis dolore te feugait nulla facilisi. Lorem ipsum "
      "dolor sit amet, consectetuer adipiscing elit, sed diam nonummy nibh "
      "euismod tincidunt ut laoreet dolore magna aliquam erat volutpat. \n"
      "\n"
      "Ut wisi enim ad minim veniam, quis nostrud exerci tation ullamcorper "
      "suscipit lobortis nisl ut aliquip ex ea commodo consequat. Duis autem "
      "vel eum iriure dolor in hendrerit in vulputate velit esse molestie "
      "consequat, vel illum dolore eu feugiat nulla facilisis at vero eros et "
      "accumsan et iusto odio dignissim qui blandit praesent luptatum zzril "
      "delenit augue duis dolore te feugait nulla facilisi. \n"
      "\n"
      "Nam liber tempor cum soluta nobis eleifend option congue nihil "
      "imperdiet doming id quod mazim placerat facer possim assum. Lorem ipsum "
      "dolor sit amet, consectetuer adipiscing elit, sed diam nonummy nibh "
      "euismod tincidunt ut laoreet dolore magna aliquam erat volutpat. Ut "
      "wisi enim ad minim veniam, quis nostrud exerci tation ullamcorper "
      "suscipit lobortis nisl ut aliquip ex ea commodo consequat. \n"
      "\n"
      "Duis autem vel eum iriure dolor in hendrerit in vulputate velit esse "
      "molestie consequat, vel illum dolore eu feugiat nulla facilisis. \n"
      "\n"
      "At vero eos et accusam et justo duo dolores et ea rebum. Stet clita "
      "kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet. "
      "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam "
      "nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, "
      "sed diam voluptua. At vero eos et accusam et justo duo dolores et ea "
      "rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem "
      "ipsum dolor sit amet. Lorem ipsum dolor sit amet, consetetur sadipscing "
      "elitr, At accusam aliquyam diam diam dolore dolores duo eirmod eos "
      "erat, et nonumy sed tempor et et invidunt justo labore Stet clita ea et "
      "gubergren, kasd magna no rebum. sanctus sea sed takimata ut vero "
      "voluptua. est Lorem ipsum dolor sit amet. Lorem ipsum dolor sit amet, "
      "consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut "
      "labore et dolore magna aliquyam erat. \n"
      "\n"
      "Consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut "
      "labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et "
      "accusam et justo duo dolores et ea rebum. Stet clita kasd gubergren, no "
      "sea takimata sanctus est Lorem ipsum dolor sit amet. Lorem ipsum dolor "
      "sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor "
      "invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At "
      "vero eos et accusam et justo duo dolores et ea rebum. Stet clita kasd "
      "gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet. "
      "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam "
      "nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, "
      "sed diam voluptua. At vero eos et accusam et justo duo dolores et ea "
      "rebum. Stet clita kasd gubergren, no sea takimata sanctus. Lorem ipsum "
      "dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod "
      "tempor invidunt ut labore et dolore magna aliquyam erat, sed diam "
      "voluptua. At vero eos et accusam et justo duo dolores et ea rebum. Stet "
      "clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor "};
  s = absl::StrSplit(s.front(), " ");

  // First test the manual interface.
  FsstEncoder encoder{s};
  std::vector<std::string> s2;
  size_t original = 0;
  size_t compressed = 0;
  for (auto& str : s) {
    s2.push_back(encoder.compress(str));
    original += str.size();
    compressed += s2.back().size();
  }
  EXPECT_LT(compressed, original);

  auto decoder = encoder.makeDecoder();
  for (auto& str : s2) {
    str = decoder.decompress(str);
  }
  EXPECT_THAT(s2, ::testing::ElementsAreArray(s));

  // Now test the `compressAll` interface.
  {
    auto [buffer, compressedViews, decoder] = FsstEncoder::compressAll(s);
    std::vector<std::string> s3;
    for (auto compressedView : compressedViews) {
      s3.push_back(decoder.decompress(compressedView));
    }
    EXPECT_THAT(s3, ::testing::ElementsAreArray(s));
  }
}

// _____________________________________________________________________________
TEST(FsstEncoder, DecompressIntoMatchesDecompress) {
  const std::vector<std::string> words{"alpha", "beta", "gamma"};
  auto [buffer, compressedViews, decoder] = FsstEncoder::compressAll(words);
  auto word = words.begin();
  for (const auto& compressed : compressedViews) {
    const std::string viaString = decoder.decompress(compressed);
    std::string output(decoder.maxDecompressedSize(compressed), '\0');
    const size_t size = decoder.decompressInto(
        compressed, ql::span<char>{output.data(), output.size()});
    EXPECT_THAT(std::string_view{output.data(), size},
                ::testing::Eq(viaString));
    EXPECT_THAT(viaString, ::testing::Eq(*word));
    ++word;
  }
}

// _____________________________________________________________________________
template <size_t N>
void expectRepeatedDecompressIntoMatches(
    const std::vector<std::string>& words) {
  std::vector<std::string_view> views;
  views.reserve(words.size());
  for (const auto& w : words) {
    views.emplace_back(w);
  }
  std::array<FsstDecoder, N> decoders{};
  std::shared_ptr<std::string> keepAlive;
  std::vector<std::string_view> compressed = views;
  for (size_t stage = 0; stage < N; ++stage) {
    auto [buffer, nextViews, decoder] = FsstEncoder::compressAll(compressed);
    keepAlive = std::move(buffer);
    compressed.assign(nextViews.begin(), nextViews.end());
    decoders[stage] = decoder;
  }
  FsstRepeatedDecoder<N> repeated{decoders};
  std::string scratch;
  for (size_t i = 0; i < words.size(); ++i) {
    const std::string viaString = repeated.decompress(compressed[i]);
    std::string intoBuf(repeated.maxDecompressedSize(compressed[i]), '\0');
    const size_t n = repeated.decompressInto(
        compressed[i], ql::span<char>{intoBuf.data(), intoBuf.size()}, scratch);
    EXPECT_THAT(n, ::testing::Eq(viaString.size()));
    EXPECT_THAT(std::string_view(intoBuf.data(), n), ::testing::Eq(viaString));
    EXPECT_THAT(viaString, ::testing::Eq(words[i]));
  }
  if constexpr (N >= 2) {
    EXPECT_GE(scratch.size(), repeated.maxDecompressedSize(compressed.front()));
  }
}

// _____________________________________________________________________________
TEST(FsstRepeatedDecoder, decompressIntoMatchesDecompressOneStage) {
  expectRepeatedDecompressIntoMatches<1>(
      {"alpha", "beta", "gamma-gamma-gamma"});
}

// _____________________________________________________________________________
TEST(FsstRepeatedDecoder, decompressIntoMatchesDecompressTwoStages) {
  expectRepeatedDecompressIntoMatches<2>(
      {"alpha", "beta", "gamma-gamma-gamma"});
}

// _____________________________________________________________________________
TEST(FsstRepeatedDecoder, decompressIntoMatchesDecompressThreeStages) {
  expectRepeatedDecompressIntoMatches<3>(
      {"alpha", "beta", "gamma-gamma-gamma"});
}
