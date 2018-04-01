```
  ____  _     _
 | __ )(_)___| |_ _ __ ___  ___________________________
 |  _ \| / __| __| '__/ _ \ 
 | |_) | \__ \ |_| | | (_) |  C O L U M N S  F I R S T
 |____/|_|___/\__|_|  \___/ ___________________________
```

* [Introduction](#introduction)
* [Bistro Engine](#bistro-engine)
  * [What is Bistro Engine: a data processing engine](#what-is-bistro-engine-a-data-processing-engine)
  * [How it works: a novel data processing paradigm](#how-it-works-a-novel-data-processing-paradigm)
  * [Why Bistro Engine: benefits](#why-bistro-engine-benefits)
* [Bistro Streams](#bistro-streams)
  * [What is Bistro Streams: a stream analytics server](#what-is-bistro-streams-a-stream-analytics-server)
  * [How it works: a novel approach to stream processing](#how-it-works-a-novel-approach-to-stream-processing)
  * [Why Bistro Streams: benefits](#why-bistro-streams-benefits)
* [Formal basis: Concept-Oriented Model](#formal-basis-concept-oriented-model)
* [More information](#more-information)

# Introduction

The goal of this project is to implement a novel general-purpose data modeling and data processing technology which radically differs from most of the existing approaches. Shortly, it can be viewed as a major alternative to SQL, MapReduce and other set-oriented approaches. This project currently includes the following sub-projects:

* [Bistro Engine](./core) - a general-purpose data processing engine
* [Bistro Streams](./server) - a stream analytics server
* [Examples](./examples) - examples of how to use Bistro Engine and Bistro Streams

Below we provide a general description of these projects and more detailed information can be found in the corresponding project sub-folders.

# Bistro Engine

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
* The use of column operations can provide additional performance improvment in comparision to the use of set operations because the latter essentially copy significatn poritions of data between set while processing them. Bistro avoids such unnecessary copy operations. 

# Bistro Streams

## What is Bistro Streams: a stream analytics server

Bistro Streams is a light-weight column-oriented *stream analytics server* which radically changes the way stream data is processed. In contrast to other stream processing systems, Bistro Streams defines its data processing logic using *column definitions*. In particular, it does not use such difficult to comprehend and execute operations like join and group-by. In fact, it is a *general-purpose* server which can be used not only for in-stream analytics but also for other tasks data integration, data migration, extract-transform-load (ETL) or big data processing. Yet, currently its features are more focused on stream analytics with applications in IoT and edge computing.

## How it works: a novel approach to stream processing

Internally, Bistro Streams is simply a database consisting of a number of tables and columns. These tables and columns may have their own definitions and it is precisely how data is being processed in the system. In other words, instead of continuously evaluating queries, Bistro Streams evaluates column and table definitions by deriving new data from the existing data. The unique distinguishing feature of Bistro Streams is *how* it processes data:

> Bistro Streams does for stream analytics what column stores did for databases.

The role of Bistro Streams is to organize interaction of this interval database with the outside world. In particular, the server provides functions for (synchronously or asynchronously) feeding data into the database and reading data from the database. These functions are implemented by *connectors* which know how to interact with the outside data sources and how to interact with the internal data engine. Thus the internal database (Bistro Engine) is unaware of who and why changes its state - this role is implemented by connectors in the server. On the other hand, Bistro Streams is unaware of how this data state is managed - it is the task of Bistro Engine.

## Why Bistro Streams: benefits

Here are some benefits and unique features of Bistro Streams:

* Bistro Streams is a light-weight server and therefore very suitable for running on edge devices. It is estimated that dozens billions connected things will come online by 2020 and analytics at the edge is becoming the cornerstone of any successful IoT solution. Bistro Streams is able to produce results and make intelligent decisions immediately at the edge of the network or directly on device (as opposed to cloud computing where data is transmitted to a centralized server for analysis). In other words, Bistro Streams is intended to analyze data as it is being created by producing results immediately as close to the data source as possible
* Bistro Streams is based on a general-purpose data processing engine which supports arbitrary operations with data starting from simple filters and ending with artificial intelligence and data mining.
* Bistro Streams is easily configurable and adaptable to various environment and tasks. It separates the logic of interaction with the outside world and the logic of data processing. It is easy to implement a custom connector for interacting with specific devices or data sources, and it is also easy to implement custom actions to interact with the internal data processing engine.
* Bistro Streams is based on a novel data processing paradigm which is conceptually simpler and easier to use than conventional approaches to data processing based on SQL-like languages or MapReduce. In order to define how data has to be processed it is enough to define new columns instead of writing complex queries which are difficult to understand and maintain.

## Formal basis: Concept-Oriented Model

Formally, Bistro relies on the *concept-oriented model* (COM) [2] where the main unit of representation and processing is a *function* as opposed to using only sets in the relational and other set-oriented models.

# More information

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
