*** How to update the benchmark configuration shorthand grammar***

All files in this folder except for `ANTLR4Mockup.g4` are automatically generated. Please leave them untouched. In case, you need a more complex grammar for your tests, the workflow is as follows:

- Change the `ANTLR4Mockup.g4` file according to your needs.(There is a lot of Documentation on the ANTLR4 format online.)

- Install `ANTLR4` using `pip install antlr4-tools`, which will download all prerequisites, install them and create aliases for use on your machine. A more detailed manual can be found here: `https://github.com/antlr/antlr4/blob/master/doc/getting-started.md`. Alternatively, you can also just download and use the `.jar` file for `ANTLR 4.12.0`, following these instructions: `https://github.com/antlr/antlr4/blob/master/doc/getting-started.md#unix`.

- Generate new parser and lexer for `cpp`, by calling: `antlr4 -no-listener -Dlanguage=Cpp ANTLR4Mockup.g4`

- Commit all the changed files, except the `antlr.jar`.
