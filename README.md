# Bistro: Calculate∙Link∙Accumulate

## Waht is Bistro

*Bistro* is a novel, general purpose, light-weight data processing engine which changes the way data is being processed. It going to be as simple as a spreadsheet and as powerful as SQL. At its core, it relies on a *column-oriented* data representation and processing engine which is unique in that its logic is described as a DAG of *column operations* as opposed to table operations in most other frameworks. Computations in Bistro are performed by *evaluating* columns which have custom definitions in terms of other columns. 

## New Data Processing Paradigm: Calculate-Link-Accumulate

Bistro provides three column definition (operation) types: calculate columns, link columns and accumulate columns. This novel *calculate-link-accumulate* (CLA) data processing paradigm is an alternative to conventional SQL-like languages, map-reduce and other set-oriented approaches. 

## Formal basis: Concept-Oriented Model (COM)

Formally, Bistro is an implementation of the *concept-oriented model* (COM) where the main unit of representation and processing is that of a function. An advantage of this model is that it does not use such operations as join and group-by which are known to be error-prone, difficult to comprehend, require high expertise and might be inefficient when applied to analytical data processing workloads. 

## Where Bistro is Useful

As a general purpose engine, Bistro can be applied to many problems like data integration, data migration, extract-transform-load (ETL), big data processing, stream analytics, big data processing.

# How it works

## Define schema (tables and columns)

* names - Case insenstive while resolution and findning
* data type - currently only one basic data type: Object. 

The data processing function must themselves guarantee the correctness of operations with the real data type. For example, if Double or String is stored in columns.

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

```java
col.evaluate();
```

## Link columns

## Accumulate columns

## Dependencies

Alternatively, it can be evaluated automatically if its data is needed by some other column. 
