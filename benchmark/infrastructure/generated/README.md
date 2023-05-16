*** How to update the benchmark configuration shorthand grammar***

All files in this folder except for `BenchmarkConfigurationShorthandAutomatic.g4` are automatically generated. Please leave them untouched. In case the benchmark configuration shorthand grammar has to be changed (e.g. to support a language extension), the workflow is as follows:

- Change the `BenchmarkConfigurationShorthandAutomatic.g4` file according to your needs.(There is a lot of Documentation on the ANTLR4 format online.)

- Inside this directory, download, or install, `ANTLR 4.12.0` parser generator, following the instructions found here: `https://github.com/antlr/antlr4/blob/master/doc/getting-started.md`

- Generate new parser and lexer for `cpp`, by calling: `antlr4 -no-listener -Dlanguage=Cpp BenchmarkConfigurationShorthand.g4`

- If necessary, adapt the `BenchmarkConfigurationShorthandVisitor.h /.cpp` (one folder above)

- Commit all the changed files, except the `antlr.jar`.
