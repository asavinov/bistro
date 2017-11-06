# Bistro-core: Calculate∙Link∙Accumulate

# What is Bistro-core

Bistro-core is a core library of Bistro for schema management, data representation and data processing.

## How to use

### Artifacts

Group: `org.conceptoriented`
Artifact: `bistro-core`
Artifact: `bistro-formula`
Version: `0.3.0`

### Maven configuration

### Gradle configuration

# How to build

Command line: 
* Build the project: gradlew build
* Publish the artifact: gradlew publish

# TODO

## General

* Configure project (gradle) to publish in maven repo
  * Signing and is it really important
  * How to specify release or snapshot and is it really important
  * Describe in "how to use/run" the usage of these artifacts in maven/gradle configurations

* Examples project. 
  * Example1 is what is in readme
  * Use simple CSV reader/writer to load more interesting data
  * Maybe define util methods for populating table schema from CSV header

## Import/export columns

* Define import/export column classes for reading/writing CSV
  * Import column, when configured and evaluated, adds records to its output table. 
  * Export column when configured and evaluated, writes the input records to the output file.
  * We need conception for schema evaluation and propagation. It answers quesiton how schema elements are created/deleted/updated, that is, how these collections are populated/updated.
  * What does it mean to be dirty for import/export columns? 
  * Once evaluated (imported), the import column is marked as clean and will never be evaluated again. To re-evaluate it, it has to be manually marked as dirty (in future we might introduce a mechanism for doing it regularly according to some strategy but this should be done externally by the corresponding mechanism).
  * How the system distinguishes between normal columns and import/export columns?
    * Special interface - then it would be good to introduce some methods important for it
    * Properties of input/output tables. For example, if input Table is not a normal table then it is an import column.
      * How to distinguish external tables? Special interface or property (similar to being primitive or super or key).

## Problems

* Currently, link dependencies include also output table columns (lhs columns). If we link to them then they have to be computed (is it really so - they might be USER column). If we append then we do not care - we will append anyway.
  * For example, can we link to derived (calculated) columns in the output table? Does it make sense?

## Error handling

* Error handling in calc-link-accu. 
  * Exceptions or error state? When translation/structure errors are reported and how they are supposed to be checked by the user?
  * We need user oriented view on error handling.
  * Introduce last resort exception catch in the case of null-pointer or whatever system-level problems in user code. Surround user code with exceptions.
  * Check validity of parameters whenever possible and document what is expected, for example, empty list/array or null.

## Utilities

* Load schema from CSV string/file
* Load data from CSV string/file
* Load schema/data from JSON collection/object

## API

* Convenience. Maybe in addition to column parameters, introduce column name parameters (so that we do not need to resolve them)
  * calc(lambda, String[]) - use a list of column names (also list of NamePaths)
  * link(String[], String[])
  * link(String[], NamePath[])
  * link(Map<Column, Column/ColumnPath>)
  * link(Map<String, String/NamePath>)

* API simplification conception. We always work in the context of some schema/graph. 
  * Maybe use 'with' conception
  * The idea is that the current schema should not be specified for each operation
  * with graph.as_default():

* Take into account that some languages allow for argument names while other parameters are optional.
  * introduce/provide method argument name conventions for typical parameters like columns, tables, name etc.

* Should we use run() instead of eval()? Check with other frameworks.

## UDE

* Introduce special Expression types:
  * constant value UDE, for example, for initializers like 0.0 
  * Column/path UDE which return the value of the specified path/column

* Conception: 
  * it would be useful to have a possibility to combine operations defined by standard expressions to build more complex expressions.
  * For example, define Expr1 and Expr2 and then combine them somehow, e.g., using Sum (either expression or lambda)

## Tables

* Introduce filter lambda/expression for each table. Is it really valid to have it for any table, that is, will it be valid/meaningful for any table or only for derived tables or only for products? Is it always necessary to have key columns (if yes, can we view id as the only key column)?
