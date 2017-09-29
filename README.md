# Bistro: Calculate∙Link∙Accumulate

## What is Bistro

*Bistro* is a general purpose, light-weight data processing engine which changes the way data is being processed. It is going to be as simple as a spreadsheet and as powerful as SQL. At its core, it relies on a novel *column-oriented* data representation and processing engine which is unique in that its logic is described as a DAG of *column operations* as opposed to table operations in most other frameworks. Computations in Bistro are performed by *evaluating* columns which have custom definitions in terms of other columns. 

## New Data Processing Paradigm: Calc-Link-Accu

Bistro provides three column definition (operation) types: calculate columns, link columns and accumulate columns. This novel *calc-link-accu* (CLA) data processing paradigm is an alternative to conventional SQL-like languages, map-reduce and other set-oriented approaches. In set-oriented approaches, data is being processed by producing new sets (tables, collections, lists etc.) from the data stored in other sets by applying various set operations like join, group-by, map or reduce. In CLA, data is being processed by producing new columns from existing columns by applying three main operations: calculate, link and accumulate. Calculate operation roughly corresponds to the map and select operations, links roughly correspond to the join operation, and accumulate is a column-oriented analogue of group-by and reduce. 

## Formal basis: Concept-Oriented Model (COM)

Formally, Bistro relies on the *concept-oriented model* (COM) where the main unit of representation and processing is that of a *function* as opposed to using only sets in the relational and other set-oriented models. An advantage of this model is that it does not use such operations as join and group-by which are known to be error-prone, difficult to comprehend, require high expertise and might be inefficient when applied to analytical data processing workloads. Essentially, the use of functions makes CLA much closer to conventional spreadsheets which are known to be rather intuitive and easy to use for data processing. However, Bistro and CLA apply the functional approach in the context of table data rather than cells in spreadsheets.

## Where Bistro is Useful

Bistro is a *general-purpose* data processing engine and can be applied to many problems like data integration, data migration, extract-transform-load (ETL), big data processing, stream analytics, big data processing.

## Graph of column operations

In Bistro, computations nodes in the graph are columns and each column has some definition which determins what operations this node will execute to compute its values. Column definitions take some other columns as input and the output values are stored in this column as its result which can be used as input in other column definitions.

## How to use

### Artifacts

Group: `org.conceptoriented`
Artifact: `bistro-core`
Artifact: `bistro-formula`
Version: `0.3.0`

### Maven configuration

### Gradle configuration

# More info

* (Bistro-core)[https://bitbucket.org/conceptoriented/bistro/core] is a core library of Bistro for schema management, data representation and data processing.
* (Bistro-formula)[https://bitbucket.org/conceptoriented/bistro/formula] is a library for defining columns using formulas in some expression language rather than the native programming langauge.

# Change Log

* v0.3.0 (2017-09-xx) - Refactoring column definition API: arrays instead lists, factoring formulas out in a separate project etc.
* v0.2.0 (2017-09-17) - Major refactoring and cleaning with new API
* v0.1.0 (2017-09-03) - Initial commit
