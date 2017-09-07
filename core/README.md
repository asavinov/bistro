# Bistro: calculate-accumulate-link 

Bistro is a Java library for column-oriented data processing.
Its unique distribuishing feature is that data processing is described as operations with columns 
and can be characterized as Calculate-Accumulate-Link (CAL) because of the three column operations.   
Formally, this means that data transformations are represented as a graph of operations with functions 
rather then a graph of operations with sets in other formalisms. It is also opposed to the map-reduce 
data processing paradigm because instead of the map and reduce set operations it uses three 
column operations: calculate, accumulate and link. The formal basis for the Bistro column-oriented 
data processing engine is a novel data model called the concept-oriented model.
As a general purpose engine, Bistro can be applied to many problems like data integration, 
data migration, extract-transform-load (ETL), big data processing, stream analytics, big data processing.

# How it works

## Create schema (tables and columns)

* names - by default unique names, if not unique then exception/error creating -> see how elements are created in other frameworks
* data type/operation/definition - how to specify data type: enum, string, guid. Data type is *native* storage and native represenation/operations.

* error handling - exceptions/returns - result for operations
* status handling - fields maybe referencing previous error/exception

* column kind - by default USER (not derived, manually set values will be overwritten, no evaluation/transaltion)
* table kind - by default derived - manually added/removed records will be deleted/overwritten

## Three types of derived columns

Describe the difference between user columns and derived columns. 

Paradigm: Calculate-Accumulate-Link (map-reduce-join)

## Calculate columns

Define how column calculates its output using an expression which can be provided in these forms:
* Text formula (and expression type because there can be many syntactic conventions for writing formulas):
  * Exp4j
  * Evalex
  * JavaScript
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

* v0.1.0 (2017-09-03) - Initial commit

# TODO

## General
* Remove enum ExpressionKind. Use class name instead and simultaniously switch to using separate classes UdeExpr4j etc. 
* Rework dependency management. Probably make the protected (they are needed only for evaluation). Simplilfy graph computation: to evaluate this column, we search for dirty (isChanged) and hasErrors.
* Test evaluation in the case of complex dependencies and check two the errors: cycles, translation errors (also inherited), evaluation errors (during evaluation) etc.
* Error handling in calc-link-accu. Simultaniously, simplify error handling in UDE classes, maybe remove two types of errors.

## UDE
* Simply UDE interface by leaving one error type etc.
* Simply UDE interface by removing translate and evaluate. Only evaluate. Translation is done etiher in construtor or when formula is set. Think about setting formula only in a setter (not in constructor).
* UDE currently has two versions of parameter paths: names and objects. Do we really need both of them?
* Split UdeJava into two Udes and one base UdeExprBase
  * UdeEvalex, UdeExp4j, UdeMathparser, UdeJavaScript extend UdeExprBase - specific are type conversion (e.g., from strings) or dynamic typing like JS
* Introduce constant UDE, for example, for initializers like 0.0 and EQUAL UDE for copying field/path values to output without any expression.
* Define UdeLambda and then the corresponding calc/link/accu methods with lambda as parameters
  * UdeLambda(lambda, paramPaths)

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




