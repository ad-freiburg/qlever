SparqlEngineDraft
=================

How to use
==========

0. Requirements
---------------

Make sure you use a 64bit Linux with:

* git
* g++ 4.8 or higher
* CMake 2.8.4 or higher

Other compilers (and OS) are not supported, yet. 
So far no major problems are known. 
Support for more plattforms would be a highly appreciated contribution.

1. Build:
---------

a) Checkout this project:

    git clone https://github.com/Buchhold/SparqlEngineDraft.git --recursive

Don't forget --recursive so that submodules will be updated. 
For old versions of git, that do not support this parameter, you can do:

    git clone https://github.com/Buchhold/SparqlEngineDraft.git
    cd SparqlEngineDraft
    git submodule init
    git subodule update
    

b) Go to a folder where you want to build the binaries.
Don't do this directly in SparqlEngineDraft

    cd /path/to/YOUR_FOLDER

c) Build the project

    cmake /path/to/SparqlEngineDraft/ -DCMAKE_BUILD_TYPE=Release; make -j

d) Run ctest. All tests should pass:

    ctest


2. Creating an Index:
---------------------

IMPORTANT:
THERE HAS TO BE SUFFICIENT DISK SPACE IN UNDER THE PATH USE CHOOSE FOR YOUR INDEX!

a) from an NTriples file (currently no blank nodes allowed):

    src/index/IndexBuilderMain -n /path/to/input.nt -b /path/to/myindex

b) from a TSV File (no spaces / tabs in spo):

    src/index/IndexBuilderMain -t /path/to/input.tsv -b /path/to/myindex


3. Starting a Sever:
--------------------

    ./ServerMain -o /path/to/myindex -p <PORT>


4. Running queries:
-------------------

    curl 'http://localhost:<PORT>/&query="SELECT ?x WHERE {?x <rel> ?y}"'
