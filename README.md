```
  ____  _     _
 | __ )(_)___| |_ _ __ ___  ___________________________
 |  _ \| / __| __| '__/ _ \ 
 | |_) | \__ \ |_| | | (_) |  C O L U M N S  F I R S T
 |____/|_|___/\__|_|  \___/ ___________________________
```

## About Bistro project?

The goal of this project is to implement a novel general-purpose data modeling and data processing technology which radically differs from most of the existing approaches. Shortly, it can be viewed as a major alternative to SQL, MapReduce and other set-oriented approaches. The project consists of the following sub-projects:

* [**Bistro Engine**](./core) - a general-purpose data processing library - very roughly, it is an alternative to MapReduce
* [**Bistro Streams**](./server) - a library for stream analytics (for IoT and edge computing) - very informally, it is an alternative to Kafka Streams
* [Examples](./examples) - examples of how to use Bistro Engine and Bistro Streams

## What is Bistro intended for?

The main general goal of Bistro is *data processing*. By data processing we mean deriving new data from existing data.

## What kind of data Bistro can process?

Bistro assumes that data is represented as a number of *sets* of elements. Each *element* is a tuple which is a combination of column values. A *value* can be any (Java) object.

## How Bistro processes data?

Tables and columns in Bistro may have *definitions*. A table definition specifies how elements of this set are produced (inferred or derived) from elements of other sets. A column definition specifies how the values of this column are computed from the values of other columns (in this or other tables). Table and column definitions in Bistro are analogous to queries in conventional DBMS.

## How Bistro is positioned among other data processing technologies?

We can distinguish between two big approaches:
* set-oriented or set theoretic approaches rely on sets for representing data and set operations for manipulating data
* column-oriented or functional approaches rely on functions for representing data and operations with functions for manipulating data

The following table shows some typical technologies and how they are positioned along two dimensions:

|  | Column-oriented | Set-oriented
--- | --- | ---
Data models (logical) | Functional, ODM | Relational model
Data stores (physical) | Vertica, SAP HANA etc. | Classical DBMSs
Data processing (batch) | **Bistro Engine** | MapReduce, SQL
Stream processing | **Bistro Streams** | Kafka Streams, Spark Streaming, Flink etc.

(Of course, this table is a very rough representation because many technologies have significant overlaps.)

## Does Bistro have queries?

No, Bistro does not provide any query language. Instead, Bistro uses definitions which are *evaluated* against the data as opposed to executing a query. These definitions (in contrast to queries) are integral part of the database. A table or column with a definition is treated equally to all other tables and columns. It is similar to defining views in a database which can be updated if some data changes.

## What are unique features of Bistro?

Bistro relies on column definitions and much less uses table definitions. In contrast, most traditional approaches (including SQL and, map-reduce) use set operations for data transformations.

## What are the benefits of column-orientation (at logical level)?

Describing data processing logic using column operations can be much more natural and simpler in many scenarios (for the same reason why spreadsheets are). In particular, Bistro does not use joins and group-by which are known to be difficult to understand and use but which are very hard to get rid of. Logical column operations are also naturally mapped to physical column operations.

## What is the formal basis of Bistro?

Formally, Bistro relies on the *concept-oriented model* (COM) [2] where the main unit of representation and processing is a *function* as opposed to using only sets in the relational and other set-oriented models. Data in this model is stored in functions (mappings between sets) and it provides operations for computing new functions from existing functions. COM supports set operations but they have weaker role in comparison to set-oriented models.

## What are other implementation of this approach to data processing?

The same formal basis has been also used in these projects:
* Stream Commandr (2016-2017):
  * Stream Commandr Engine: https://github.com/asavinov/sc-core
  * Stream Commandr HTTP REST Server: https://github.com/asavinov/sc-rest
  * Stream Commandr Web Application (Angular): https://github.com/asavinov/sc-web --> You can test it here: http://dc.conceptoriented.com

* Data Commandr (2013-2016):
  * DataCommandr Engine Java: https://github.com/asavinov/dc-core.git
  * DataCommandr Engine C#: https://github.com/asavinov/dce-csharp
  * DataCommandr Application (WPF): https://github.com/asavinov/dc-wpf.git

# Who has been developing Bistro?

Bistro and the underlying concept-oriented model of data has been developed by Alexandr Savinov: http://conceptoriented.org

# License

See LICENSE file in the project directory: https://github.com/asavinov/bistro/blob/master/LICENSE
[![License: MIT](https://img.shields.io/badge/License-MIT-brightgreen.svg)](https://github.com/asavinov/bistro/blob/master/LICENSE)
