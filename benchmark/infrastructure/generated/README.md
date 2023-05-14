*** How to update the benchmark configuration shorthand grammar***

All files in this folder except for `BenchmarkConfigurationShorthandAutomatic.g4` are automatically generated. Please leave them untouched. In case the benchmark configuration shorthand grammar has to be changed (e.g. to support a language extension), the workflow is as follows:

- Change the `BenchmarkConfigurationShorthandAutomatic.g4` file according to your needs.(There is a lot of Documentation on the ANTLR4 format online.)

- Inside this directory, download, or install, the ANTLR parser generator: `https://github.com/antlr/antlr4/blob/master/doc/getting-started.md`

- Generate new parser and lexer for `cpp`, following: `https://github.com/antlr/antlr4/blob/master/doc/getting-started.md#generating-parser-code`

- If necessary, adapt the `BenchmarkConfigurationShorthandVisitor.h /.cpp` (one folder above)

- Commit all the changed files, except the `antlr.jar`.
