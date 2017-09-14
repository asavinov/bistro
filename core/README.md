# Bistro: Calculate∙Link∙Accumulate

*Bistro* is a novel, general purpose, light-weight data processing engine which changes the way data is being processed. 
It going to be as simple as a spreadsheet and as powerful as SQL. 
At its core, it relies on a *column-oriented* data representation and processing engine which is unique in that its logic is described as a DAG of *column operations* as opposed to table operations in most other frameworks. 
Computations in Bistro are performed by *evaluating* columns which have custom definitions in terms of other columns. 
Bistro provides three column definition (operation) types: calculate columns, link columns and accumulate columns. 
This novel *calculate-link-accumulate* (CLA) data processing paradigm is an alternative to conventional SQL-like languages, map-reduce and other set-oriented approaches. 
Formally, Bistro is an implementation of the *concept-oriented model* (COM) where the main unit of representation and processing is that of a function. 
An advantage of this model is that it does not use such operations as join and group-by which are known to be error-prone, difficult to comprehend, require high expertise and might be inefficient when applied to analytical data processing workloads. 
As a general purpose engine, Bistro can be applied to many problems like data integration, data migration, extract-transform-load (ETL), big data processing, stream analytics, big data processing.

# How it works

## Create schema (tables and columns)

* names - Case insenstive while resolution and findning
* data type - currently only one basic data type: Object. 
The data processing function must themselves guarantee the correctness of operations with the real data type. For example, if Double or String is stored in columns.

* error handling - exceptions/returns - result for operations
* status handling - fields maybe referencing previous error/exception

## Three types of derived columns

Describe the difference between user columns and derived columns. 

Paradigm: Calculate-Accumulate-Link (map-reduce-join)

## Calculate columns

Define how column calculates its output using an expression which can be provided in these forms:
* Text formula (and expression type because there can be many syntactic conventions for writing formulas):
  * Exp4j
  * Evalex
  * JavaScript
  * Java
* User defined class implementing the UDE interface along with the specification of column which are used as inputs
* Lambda and specification of its inputs as a number of column paths

Once an expression has been specified it is necessary to check for errors which can result, for example, from the formula translation.
A typical error is that the provided column name is not found (because of a typo in its name). 

A column with a definition can be immediately evaluated:

    col.evaluate();

Alternatively, it can be evaluated automatically if its data is needed by some other column. 

## Link columns

## Accumulate columns

# Build

Command line: 
* Build the project: gradlew build
* Publish the artifact: gradlew publish

# Change Log

* v0.2.0 (2017-09-03) - Major refactoring and cleaning by focusing on new API
* v0.1.0 (2017-09-03) - Initial commit

# TODO

## General

* What to do with two types of paths in UDE: objects and names? Can we use only one?

* Error handling in calc-link-accu. Simultaniously, simplify error handling in UDE classes, maybe remove two types of errors.
  * Exceptions or error state? When translation/structure errors are reported and how they are supposed to be checked by the user?
  * We need user oriented view on error handling.
  * Introduce last resort exception catch in the case of null-pointer or whatever system-level problems in user code. Surround user code with exceptions.
  * Check validity of parameters whenever possible and document what is expected, for example, empty list/array or null.

* Arrays instead of lists in high level public methods. They are easier to define and can be declared as variable arguments.

## UDE
* So we need to distinguish these clases depending on how the main procedure is given (they can be subclasses of some basic UDE):
  * Normal UDE: evaluator is implemented by the class method. 
    * Parameterization via path setters (strings or objects).
  * Lambda UDE: evaluator is provided as a function with certain signature.
    * Lambda parameter
    * Parameterization via path setters (strings or objects).
  * Formula UDE: evaluator provided syntactically and will be translated/resolved relative to the provided table object.
    * Formula parameter for procedure (and dependencies) 
    * Table object this formulas belongs to for resolution of dependencies

* Simply UDE interface by leaving one error type etc.
* Simply UDE interface by removing translate and evaluate. Only evaluate. Translation is done etiher in construtor or when formula is set. Think about setting formula only in a setter (not in constructor).
* UDE currently has two versions of parameter paths: names and objects. Do we really need both of them?

* Translation of formulas in calc-link-accu have to use class name to instantiate UDE rather than use selector.
  * In addition, these UDE classes have to implement either constructor with formula or setters for formulas or translate method.

* Evalex UDE is not always impelmented and not tested but only a placeholder exists.

* Introduce constant UDE, for example, for initializers like 0.0 and EQUAL UDE for copying field/path values to output without any expression.
  * Use case: 1) initializer, determine manaully 2) Within UdeExp4j or other expressions which determine this internally and want simplify computations by avoiding their own expression and reusing simpler way, for example, labmda or another definitionType of constant return.

* JavaScript UDE

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
