FILE(GLOB cpps *.cpp)
FILE(GLOB hs *.h)
add_library(broccoli-server
            ${cpps} ${hs}
            )