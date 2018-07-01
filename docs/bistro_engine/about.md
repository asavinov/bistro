# About Bistro Engine

## What is Bistro Engine: a data processing engine

Bistro is a light-weight column-oriented *data processing engine* which radically changes the way data is processed. As a *general-purpose* data processing engine, Bistro can be applied to such problems like big data processing, data integration, data migration, extract-transform-load (ETL), stream analytics, IoT analytics. Bistro is based on a novel data model and is an alternative to map-reduce, conventional SQL-like languages and other set-oriented approaches.

## How it works: a novel data processing paradigm

At its core, Bistro relies on a novel *column-oriented* logical data model which describes data processing as a DAG of *column operations* as opposed to having only set operations in conventional approaches. Computations in Bistro are performed by *evaluating* column definitions. Each definition describes how this column output values are expressed in terms of other columns. Currently Bistro provides three column definition (operation) types:

* calculate - roughly corresponds to the Map and SQL select operations
* link [3] - roughly corresponds to the join operation
* accumulate [1] - a column-oriented analogue of Group-by and Reduce
* roll - rolling aggregation using accumulate functions

Bistro is a major alternative to most other data models and data processing frameworks which are based on table (set) operations including SQL-like languages and MapReduce. In set-oriented approaches, data is being processed by producing new sets (tables, collections etc.) from the data stored in other sets by applying various set operations like join, group-by, filter, map or reduce. In contrast, Bistro processes data by producing new columns from existing columns by applying function operations.

## Why Bistro Engine: benefits

Here are some benefits of Bistro and the underlying column-oriented data processing model:

* Bistro does not use such operations as join and group-by which are known to be error-prone, difficult to comprehend, require high expertise and might be inefficient when applied to analytical data processing workloads.
* The use of column definitions makes Bistro similar to conventional spreadsheets, which are known to be rather intuitive and easy to use for data processing. The difference from spreadsheets is that Bistro uses column definitions instead of cell formulas.
* The use of columnar physical representation is known to be faster for analytical data processing workloads.
* The use of column operations can provide additional performance improvement in comparison to the use of set operations because the latter essentially copy significant portions of data between set. Bistro avoids such unnecessary copy operations by using column operations so that no new sets are created without necessity.
* Bistro uses the conception of evaluation (of column and table definitions) instead of query execution. This can provide significant performance improvements in the case of incremental updates when only some part of the database is changed (for example, new events have been received from a stream). Bistro ensures that only the necessary changes are propagated through the database to other elements via inference.
