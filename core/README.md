# Bistro: Calculate∙Link∙Accumulate

# Build

Command line: 
* Build the project: gradlew build
* Publish the artifact: gradlew publish

# Change Log

* v0.2.0 (2017-09-17) - Major refactoring and cleaning with new API
* v0.1.0 (2017-09-03) - Initial commit

# TODO

## General

* Maven public artifact. Currently artifact names are "core" and "formula" (same as project/folder names). We need to provide correct names like bistro-core, bistro-formula. 

* Maybe in addition to column parameters, introduce column name parameters (so that we do not need to resolve them)
  * calc(lambda, String[]) - use a list of column names (also list of NamePaths)

* Error handling in calc-link-accu. 
  * Exceptions or error state? When translation/structure errors are reported and how they are supposed to be checked by the user?
  * We need user oriented view on error handling.
  * Introduce last resort exception catch in the case of null-pointer or whatever system-level problems in user code. Surround user code with exceptions.
  * Check validity of parameters whenever possible and document what is expected, for example, empty list/array or null.

## UDE

* Introduce special Expression types:
  * constant value UDE, for example, for initializers like 0.0 
  * Column/path UDE which return the value of the specified path/column

## Import/export columns

* Define import/export column classes for reading/writing CSV
  * Import column, when evaluated, adds records to its output table. 
  * Export column when evaluated, writes the input records to the output file.
  * We need conception for schema evaluation and propagation. It answers quesiton how schema elements are created/deleted/updated, that is, how these collections are populated/updated.
  * What does it mean to be dirty for import/export columns? 

## Utilities

* Load schema from CSV string/file
* Load data from CSV string/file
* Load schema/data from JSON collection/object

## Problems
* Currently, link dependencies include also output table columns (lhs columns). If we link to them then they have to be computed (is it really so - they might be USER column). If we append then we do not care - we will append anyway.
  * For example, can we link to derived (calculated) columns in the output table? Does it make sense?
