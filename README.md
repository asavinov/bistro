```
  ____  _     _
 | __ )(_)___| |_ _ __ ___  ___________________________
 |  _ \| / __| __| '__/ _ \ 
 | |_) | \__ \ |_| | | (_) |  C O L U M N S  F I R S T
 |____/|_|___/\__|_|  \___/ ___________________________
```
[![Gitter chat](https://badges.gitter.im/gitterHQ/gitter.png)](https://gitter.im/conceptoriented/Lobby)
[![License: MIT](https://img.shields.io/badge/License-MIT-brightgreen.svg)](https://github.com/asavinov/bistro/blob/master/LICENSE)

# About Bistro project

The goal of this project is to implement a novel general-purpose data modeling and data processing technology which radically differs from most of the existing approaches. Shortly, it can be viewed as a major alternative to set-orientation like SQL or MapReduce. The project consists of the following sub-projects:

* [**Bistro Engine**](./core) - a general-purpose data processing library - it is an alternative to MapReduce
* [**Bistro Streams**](./server) - a library for stream analytics (for IoT and edge computing) - it is an alternative to Kafka Streams
* [**Examples**](./examples) - examples of how to use Bistro Engine and Bistro Streams

There also another project based on the same column-oriented principles but aimed at feature engineering and data mining: Lambdo https://github.com/asavinov/lambdo

# Contents

* [Why column-orientation?](#Why-column-orientation)
  * [Calculating data](#Calculating-data)
  * [Linking data](#Linking-data)
  * [Aggregating data](#Aggregating-data)

* [Frequently asked questions](#Frequently-asked-questions)
  * [What is Bistro intended for?](#What-is-Bistro-intended-for)
  * [What kind of data can Bistro process?](#What-kind-of-data-can-Bistro-process)
  * [How does Bistro process data?](#How-does-Bistro-process-data)
  * [How is Bistro positioned among other data processing technologies?](#How-is-Bistro-positioned-among-other-data-processing-technologies)
  * [Does Bistro have a query language?](#Does-Bistro-have-a-query-language)
  * [What are unique features of Bistro?](#What-are-unique-features-of-Bistro)
  * [What are benefits of column-orientation?](#What-are-benefits-of-column-orientation)
  * [What is the formal basis of Bistro?](#What-is-the-formal-basis-of-Bistro)
  * [What are other implementation of this approach to data processing?](#What-are-other-implementation-of-this-approach-to-data-processing)
  * [Who has been developing Bistro?](#Who-has-been-developing-Bistro)

* [References](#references)

# Why column-orientation?

## Calculating data

One of the simplest data processing operations is computing a new attribute using already existing attributes. For example, if we have a table with order `Items` each characterized by `Quantity` and `Price` then we could compute a new attribute `Amount` as their arithmetic product:

```sql
SELECT *, Quantity * Price AS Amount FROM Items
```

Although this wide spread data processing pattern may seem very natural and almost trivial it does have one significant flaw: the task was to compute a new attribute while the query produces a new table. Although the result table does contain the required attribute, the question is why not to do exactly what has been requested? Why is it necessary to produce a new table if we actually want to compute only an attribute?

The same problem exists in MapReduce. If our goal is to compute a new field then we apply the map operation which will emit completely new objects each having this new field. Here again the same problem: our intention was not to create a new collection with new objects – we wanted to add a new computed property to already existing objects. However, the data processing framework forces us to describe this task in terms of operations with collections. The reason is that we do not have any choice because these data models provides only sets and set operations and the only way to add a new attribute is to produce a set with this attribute.

An alternative approach consists in using column operations for data transformations and then we could do exactly what is requested: adding (calculated) attributes to existing tables.

## Linking data

Another wide spread task consists in computing links or references between different tables: given an element of one table, how can I access attributes in a related table? For example, assume that `Price` is not an attribute of the `Items` table as in the above example but rather it is an attribute of a `Products` table. Here we have two tables, `Items` and `Products`, with attributes `ProductId` and `Id`, respectively, which relate their records. If now we want to compute the amount for each item then the price needs to be retrieved from the related `Products` table. The standard solution is to copy the necessary attributes into a *new* table by using the relational (left) join operation for matching the records:

```sql
SELECT item.*, product.Price FROM Items item
JOIN Products product ON item.ProductId = product.Id
```

This new result table can be now used for computing amount precisely as we described above because it has the necessary attributes copied from the two source tables. Let us again compare this solution with the problem formulation. Do we really need a new table? No. Our goal is to have a possibility to access attributes from the second `Products` table (while computing a new attribute in the first table). Hence it again can be viewed as a workaround where a new set is produced just because there is no possibility not to produce it.

A principled solution to this problem is a data model which uses column operations for data processing so that a link can be defined as a new column in an existing table [3].

## Aggregating data

Assume that for each product, we want to compute the total number of items ordered. This task can be solved using group-by operation:

```sql
SELECT ProductId, COUNT(ProductID) AS TotalQuantity
FROM Items GROUP BY ProductId
```

Here we again get a *new* table although the goal is to produce a new (aggregated) attribute in an existing table (`Products`). Indeed, what we really want is to add a new attribute to the `Products` table which would be equivalent to all other attributes (like product `Price` used in the previous example). This `TotalQuantity` could be then used to compute some other properties of products. Of course, this also can be done using set operations in SQL but then we will have to again use join to combine the group-by result with the original `Products` table followed by producing yet another table with new calculated attributes. It is apparently not how it should work in an ideal data model because the task formulation does not mention and does not actually require any new tables - only attributes. Thus we see that the use of set operations in this and above cases is a problem-solution mismatch.

A solution to this problem again is provided by a column oriented data model where aggregated columns can be defined without adding new tables [1].

# Frequently asked questions

## What is Bistro intended for?

The main general goal of Bistro is *data processing*. By data processing we mean deriving new data from existing data.

## What kind of data can Bistro process?

Bistro assumes that data is represented as a number of *sets* of elements. Each *element* is a tuple which is a combination of column values. A *value* can be any (Java) object.

## How does Bistro process data?

Tables and columns in Bistro may have *definitions*. A table definition specifies how elements of this set are produced (inferred or derived) from elements of other sets. A column definition specifies how the values of this column are computed from the values of other columns (in this or other tables). Table and column definitions in Bistro are analogous to queries in conventional DBMS.

## How is Bistro positioned among other data processing technologies?

We can distinguish between two big approaches:
* set-oriented or set theoretic approaches rely on sets for representing data and set operations for manipulating data (inference)
* column-oriented or functional approaches rely on functions for representing data and operations with functions for manipulating data (inference)

The following table shows some typical technologies and how they are positioned along the two dimensions:

|  | Column-oriented | Set-oriented
--- | --- | ---
Data models (logical) | Functional, ODM, Concept-oriented model | Relational model
Data stores (physical) | Vertica, SAP HANA etc. | Classical DBMSs
Data processing (batch) | **Bistro Engine** | MapReduce, SQL
Stream processing | **Bistro Streams** | Kafka Streams, Spark Streaming, Flink etc.

Notes:
* This table is a very rough representation because many technologies have significant overlaps 
* Bistro and the concept-oriented model do support set operations. They simply shift priority from set operations to column operations

## Does Bistro have a query language?

No, Bistro does not provide any query language. Instead, Bistro uses definitions which are *evaluated* against the data as opposed to executing a query. These definitions (in contrast to queries) are integral part of the database. A table or column with a definition is treated equally to all other tables and columns. It is similar to defining views in a database which can be updated if some data changes.

Bistro has a sub-project called bistro-formula which is intended for supporting expression languages instead of native Java code. Yet, such expressions (similar to Excel formulas) will be translated into native code in column definitions.

## What are unique features of Bistro?

Bistro relies on column definitions and much less uses table definitions. In contrast, most traditional approaches (including SQL and, map-reduce) use set operations for data transformations.

## What are benefits of column-orientation?

Describing data processing logic using column operations can be much more natural and simpler in many scenarios (for the same reason why spreadsheets are). In particular, Bistro does not use joins and group-by which are known to be difficult to understand and use but which are very hard to get rid of. Logical column operations are also naturally mapped to physical column operations.

## What is the formal basis of Bistro?

Bistro relies on the *concept-oriented model* (COM) [2] where the main unit of representation and processing is a *function* as opposed to using only sets in the relational and other set-oriented models. Data in this model is stored in functions (mappings between sets) and it provides operations for computing new functions from existing functions. COM supports set operations but they have weaker role in comparison to set-oriented models.

## What are other implementation of this approach to data processing?

The same formal basis has been also used in these projects:
* Lambdo: https://github.com/asavinov/lambdo - A column-oriented approach to feature engineering. Feature engineering and machine learning: together at last!

* Stream Commandr (2016-2017):
  * Stream Commandr Engine: https://github.com/asavinov/sc-core
  * Stream Commandr HTTP REST Server: https://github.com/asavinov/sc-rest
  * Stream Commandr Web Application (Angular): https://github.com/asavinov/sc-web

* Data Commandr (2013-2016):
  * DataCommandr Engine Java: https://github.com/asavinov/dc-core.git
  * DataCommandr Engine C#: https://github.com/asavinov/dce-csharp
  * DataCommandr Application (WPF): https://github.com/asavinov/dc-wpf.git

## Who has been developing Bistro?

Bistro and the underlying concept-oriented model of data has been developed by Alexandr Savinov: http://conceptoriented.org

# References

* [1] A. Savinov, From Group-By to Accumulation: Data Aggregation Revisited. Proc. IoTBDS 2017, 370-379. https://www.researchgate.net/publication/316551218_From_Group-by_to_Accumulation_Data_Aggregation_Revisited
* [2] A. Savinov, Concept-oriented model: the Functional View, Eprint: [arXiv:1606.02237](https://arxiv.org/abs/1606.02237) [cs.DB], 2016. https://www.researchgate.net/publication/303840097_Concept-Oriented_Model_the_Functional_View
* [3] A. Savinov, Joins vs. Links or Relational Join Considered Harmful. Proc. IoTBD 2016, 362-368. https://www.researchgate.net/publication/301764816_Joins_vs_Links_or_Relational_Join_Considered_Harmful
* [4] A. Savinov, DataCommandr: Column-Oriented Data Integration, Transformation and Analysis. Proc. IoTBD 2016, 339-347. https://www.researchgate.net/publication/301764506_DataCommandr_Column-Oriented_Data_Integration_Transformation_and_Analysis

# License

See LICENSE file in the project directory: https://github.com/asavinov/bistro/blob/master/LICENSE
