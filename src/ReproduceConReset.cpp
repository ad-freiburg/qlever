// Copyright 2017, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold <buchholb>

#include <stdlib.h>
#include <stdio.h>
#include <getopt.h>
#include <unistd.h>
#include <string>
#include <vector>
#include <iomanip>
#include <iostream>


#include <sstream>
#include <fstream>
#include "util/ReadableNumberFact.h"
#include "util/Log.h"
#include "util/Socket.h"

using std::string;
using std::vector;
using std::cout;
using std::endl;
using std::flush;
using std::cerr;

#define EMPH_ON  "\033[1m"
#define EMPH_OFF "\033[21m"

// Available options.
struct option options[] = {
    {"port", required_argument, NULL, 'p'},
};

// Main function.
int main(int argc, char** argv) {
  cout << endl << EMPH_ON << "ServerMain, version " << __DATE__
       << " " << __TIME__ << EMPH_OFF << endl << endl;

  char* locale = setlocale(LC_CTYPE, "en_US.utf8");
  cout << "Set locale LC_CTYPE to: " << locale << endl;

  std::locale loc;
  ad_utility::ReadableNumberFacet facet(1);
  std::locale locWithNumberGrouping(loc, &facet);
  ad_utility::Log::imbue(locWithNumberGrouping);


  // Init variables that may or may not be
  // filled / set depending on the options.
  int port = -1;
  optind = 1;
  // Process command line arguments.
  while (true) {
    int c = getopt_long(argc, argv, "p:", options, NULL);
    if (c == -1) break;
    switch (c) {
      case 'p':
        port = atoi(optarg);
        break;
      default:
        cout << endl
             << "! ERROR in processing options (getopt returned '" << c
             << "' = 0x" << std::setbase(16) << static_cast<int> (c) << ")"
             << endl << endl;
        exit(1);
    }
  }

  ad_utility::Socket server;
  bool suc = server.create() && server.bind(port) && server.listen();
  if (suc) {
    while (true) {
      ad_utility::Socket client;
      if (server.acceptClient(&client)) {
        string data;
        int rv = client.receive(&data);
        LOG(INFO) << "Receive rv: " << rv << std::endl;
        LOG(INFO) << "Received: " << data << std::endl << std::endl;

        data = data.substr(0, data.find("HTTP/1.1"));
        LOG(INFO) << "data: " << data << std::endl << std::endl;

        if (data.find("repro-con-reset") != string::npos) {
          bool js = data.find("js") != string::npos;
          string contentString;
          string contentType = "text/plain";
          string statusString = "HTTP/1.0 200 OK";
          string requestedFile = "repro-con-reset";
          requestedFile += (js ? ".js" : ".html");
          // CASE: file.
          LOG(DEBUG) << "Looking for file: \"" << requestedFile << "\" ... \n";
          std::ifstream in(requestedFile.c_str());
          if (!in) {
            statusString = "HTTP/1.0 404 NOT FOUND";
            contentString = "404 NOT FOUND";
          } else {
            // File into string
            contentString = string((std::istreambuf_iterator<char>(in)),
                                   std::istreambuf_iterator<char>());
            // Set content type
            if (ad_utility::endsWith(requestedFile, ".html")) {
              contentType = "text/html";
            } else if (ad_utility::endsWith(requestedFile, ".css")) {
              contentType = "text/css";
            } else if (ad_utility::endsWith(requestedFile, ".js")) {
              contentType = "application/javascript";
            }
          }

          size_t contentLength = contentString.size();
          std::ostringstream headerStream;
          headerStream << statusString << "\r\n"
                       << "Content-Length: " << contentLength << "\r\n"
                       << "Content-Type: " << contentType << "\r\n"
                       << "Access-Control-Allow-Origin: *\r\n"
                       << "Connection: close\r\n"
                       << "\r\n";

          string data = headerStream.str();
          data += contentString;
          auto sent = client.send(data);
          LOG(INFO) << "Sent " << sent << "bytes." << std::endl;
        }

        if (data.find("json") != string::npos) {
          LOG(DEBUG) << "sleep..." << std::endl;
          usleep(1 * 1000 * 1000);
          LOG(DEBUG) << "Serving JSON..." << std::endl;
          string json = "{\n"
              "\"query\": \"PREFIX fb: <http://rdf.freebase.com/ns/>\\nSELECT ?n3 ?n1 ?predicate1 ?object ?predicate2 ?n2 WHERE {\\n   ?person1 fb:type.object.name.en \\\"Neil Armstrong\\\" .\\n   ?person2 fb:type.object.name.en \\\"Albert Einstein\\\" .\\n   ?person1 fb:type.object.name.en ?n1 .\\n   ?person2 fb:type.object.name.en ?n2 .\\n   ?person1 ?predicate1 ?object .\\n   ?person2 ?predicate2 ?object .\\n   ?object fb:type.object.name.en ?n3 .\\n   FILTER(?n3 >= \\\"Rele\\\")\\n}\\nLIMIT 1000\\nORDER BY ASC(?person1)\",\n"
              "\"status\": \"OK\",\n"
              "\"resultsize\": \"3374\",\n"
              "\"res\": [\n"
              "[\"\\\"Release track\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"<http://rdf.freebase.com/ns/music.release_track>\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Release track\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"<http://rdf.freebase.com/ns/music.release_track>\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Release track\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"<http://rdf.freebase.com/ns/music.release_track>\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Release track\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"<http://rdf.freebase.com/ns/music.release_track>\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Release track\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"<http://rdf.freebase.com/ns/music.release_track>\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Release track\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"<http://rdf.freebase.com/ns/music.release_track>\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Release track\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"<http://rdf.freebase.com/ns/music.release_track>\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Release track\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"<http://rdf.freebase.com/ns/music.release_track>\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Release track\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"<http://rdf.freebase.com/ns/music.release_track>\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Release track\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"<http://rdf.freebase.com/ns/music.release_track>\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Release track\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"<http://rdf.freebase.com/ns/music.release_track>\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Release track\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"<http://rdf.freebase.com/ns/music.release_track>\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Release track\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"<http://rdf.freebase.com/ns/music.release_track>\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Release track\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"<http://rdf.freebase.com/ns/music.release_track>\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Release track\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"<http://rdf.freebase.com/ns/music.release_track>\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"\\\"Albert Einstein\\\"\"], \n"
              "[\"\\\"Topic\\\"\",\"\\\"Neil Armstrong\\\"\",\"<http://rdf.freebase.com/ns/type.object.type>\",\"<http://rdf.freebase.com/ns/common.topic>\",\"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\",\"\\\"Albert Einstein\\\"\"]\n"
              "],\n"
              "\"time\": {\n"
              "\"total\": \"735.991ms\",\n"
              "\"computeResult\": \"734.623ms\"\n"
              "}\n"
              "}";

          std::ostringstream os;
          os << "HTTP/1.1 200 OK\r\n" << "Content-Length: " << json.size()
             << "\r\n"
             << "Connection: close\r\n" << "Content-Type: "
             << "application/json"
             << "; charset=" << "utf-8" << "\r\n"
             << "Access-Control-Allow-Origin: *" << "\r\n" << "\r\n" << json;
          auto sent = client.send(os.str());
          LOG(INFO) << "Sent " << sent << "bytes." << std::endl;
        }
      }
      client.close();
    }
  }

  return 0;
}
