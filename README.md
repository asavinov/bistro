```
  ____  _     _
 | __ )(_)___| |_ _ __ ___  ___________________________
 |  _ \| / __| __| '__/ _ \ 
 | |_) | \__ \ |_| | | (_) |  C O L U M N S  F I R S T
 |____/|_|___/\__|_|  \___/ ___________________________
```

* [About](#about)
* [FAQ](#faq)
* [Links and references](#links-and-references)

# About

The goal of this project is to implement a novel general-purpose data modeling and data processing technology which radically differs from most of the existing approaches. Shortly, it can be viewed as a major alternative to SQL, MapReduce and other set-oriented approaches. 

|  | Column-oriented | Set-oriented
--- | --- | ---
Data models (logical) | Functional, ODM | Relational model
Data stores (physical) | Vertica, SAP HANA | Classical DBMSs
Data processing | **Bistro Engine** | MapReduce, SQL
Stream processing | **Bistro Streams** | Kafka Streams, Spark Streaming, Flink etc.

This project currently includes the following sub-projects where you can find more details about each of them:

* [Bistro Engine](./core) - a general-purpose data processing library
* [Bistro Streams](./server) - a library for stream analytics (for IoT and edge computing)
* [Examples](./examples) - examples of how to use Bistro Engine and Bistro Streams

# FAQ

### What is Bistro intended for?

The main general goal of Bistro is *data processing*. By data processing we mean deriving new data from existing data.

### What kind of data Bistro can process?

Bistro assumes that data is represented as a number of *sets* of elements. Each *element* is a tuple which is a combination of column values. A *value* can be any (Java) object.

### How Bistro processes data?

Tables and columns in Bistro may have *definitions*. A table definition specifies how elements of this set are produced (inferred or derived) from elements of other sets. A column definition specifies how the values of this column are computed from the values of other columns (in this or other tables). Table and column definitions in Bistro are analogous to queries in conventional DBMS.

## Does Bistro have queries?

No, Bistro does not provide any query language. Instead, Bistro uses definitions which are *evaluated* against the data as opposed to executing a query. These definitions (in contrast to queries) are integral part of the database. A table or column with a definition is treated equally to all other tables and columns. It is similar to defining views in a database which can be updated if some data changes.

## What are unique features of Bistro?

Bistro heavily relies on column definitions and much less uses table definitions. In contrast, most traditional approaches (including SQL and, map-reduce) use set operations for data transformations. Describing data processing logic using column operations can be much more natural and simpler in many scenarios (for the same reason why spreadsheets are). In particular, Bistro does not use joins and group-by which are known to be difficult to understand and use but which are very hard to get rid of.

## What is the formal basis of Bistro?

Formally, Bistro relies on the *concept-oriented model* (COM) [2] where the main unit of representation and processing is a *function* as opposed to using only sets in the relational and other set-oriented models. Data in this model is stored in functions (mappings between sets) and it provides operations for computing new functions from existing functions. COM supports set operations but they have weaker role in comparision to set-oriented models.

# Links and references

* Sub-projects:
  * [Bistro Engine](./core) - a general-purpose data processing engine
  * [Bistro Streams](./server) - a stream analytics server
  * [Examples](./examples) - examples of how to use Bistro Engine and Bistro Streams

* A web application based on the same principles as Bistro can be evaluated here:
  * http://dc.conceptoriented.com - DataCommandr

* Alexandr Savinov is an author of Bistro as well as the underlying concept-oriented model (COM):
  * http://conceptoriented.org - Home page

* Papers related to this approach:
  * [1] A. Savinov, From Group-By to Accumulation: Data Aggregation Revisited. Proc. IoTBDS 2017, 370-379. https://www.researchgate.net/publication/316551218_From_Group-by_to_Accumulation_Data_Aggregation_Revisited
  * [2] A. Savinov, Concept-oriented model: the Functional View, Eprint: [arXiv:1606.02237](https://arxiv.org/abs/1606.02237) [cs.DB], 2016. https://www.researchgate.net/publication/303840097_Concept-Oriented_Model_the_Functional_View
  * [3] A. Savinov, Joins vs. Links or Relational Join Considered Harmful. Proc. IoTBD 2016, 362-368. https://www.researchgate.net/publication/301764816_Joins_vs_Links_or_Relational_Join_Considered_Harmful
  * [4] A. Savinov, DataCommandr: Column-Oriented Data Integration, Transformation and Analysis. Proc. IoTBD 2016, 339-347. https://www.researchgate.net/publication/301764506_DataCommandr_Column-Oriented_Data_Integration_Transformation_and_Analysis

# License

See LICENSE file in the project directory
