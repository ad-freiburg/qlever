SparqlEngineDraft
=================

How to use
==========

1. Build:
---------

a) Checkout this project:

    git clone https://github.com/Buchhold/SparqlEngineDraft.git

b) navigate to SparqlEngineDraft/src/index and checkout stxxl:

    git clone git clone https://github.com/stxxl/stxxl.git

c) Go to a folder where you want to build the binaries.
Don't do this directly in SparqlEngineDraft

    cd YOUR_FOLDER

d) Build the project

    cmake /path/to/SparqlEngineDraft; make -j

e) Run ctest. All tests should pass:

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
