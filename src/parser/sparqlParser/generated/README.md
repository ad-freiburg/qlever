*** How to update the Sparql grammar***

All files in this folder except for `Sparql.g4` are automatically generated.
Please leave them untouched. In case the Sparql grammar has to be changed (e.g. to support
a custom language extension for QLever),
the workflow is as follows:

* Change the `SparqlAutomatic.g4` file according to your needs.
  (There is a lot of Documentation on the ANTLR4 format online).
  
* Inside this directory, Download the ANTLR parser generator
```
wget http://www.antlr.org/download/antlr-4.9.2-complete.jar
```
* Inside this directory, Run ANTLR
```
java -Xmx500M -cp "./antlr-4.9.2-complete.jar:$CLASSPATH" org.antlr.v4.Tool -Dlanguage=Cpp SparqlAutomatic.g4 -visitor
```

* If necessary, adapt the `SparqlQleverVisitor.h/.cpp` (one folder above)
* Commit all the changed files (except the `antlr.jar`)