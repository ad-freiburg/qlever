*** How to update the Sparql grammar***

All files in this folder except for `Sparql.g4` are automatically generated.
Please leave them untouched. In case the Sparql grammar has to be changed (e.g. to support
a custom language extension for QLever),
the workflow is as follows:

* Change the `SparqlAutomatic.g4` file according to your needs.
  (There is a lot of Documentation on the ANTLR4 format online).
  
* Inside this directory, Download the ANTLR parser generator
```
wget http://www.antlr.org/download/antlr-4.13.2-complete.jar
```
* Inside this directory, Run ANTLR
```
java -jar ./antlr-4.13.2-complete.jar -Dlanguage=Cpp SparqlAutomatic.g4
```

* If necessary, adapt the `SparqlQleverVisitor.h/.cpp` (one folder above)
* Format the generated files using clang-format (we suggest the pre-commit hook that is shipped with the QLever repository)
* Replace `#pragma once` in the generated files by proper header guards using the `replace_pragma_once.py` script.
* Commit all the changed files (except the `antlr.jar`)
