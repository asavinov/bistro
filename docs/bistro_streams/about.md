# About Bistro Streams

## What is Bistro Streams: a stream analytics server

Bistro Streams is a light-weight column-oriented *stream analytics server* which radically changes the way stream data is processed. It is a *general-purpose* server which is not limited by in-stream analytics but can also be applied to batch processing including such tasks as (real-time) data integration, data migration, extract-transform-load (ETL), data monitoring, anomaly detection or big data processing. Yet, currently its implemented features are more focused on stream analytics with applications in IoT and edge computing.

Bistro Streams defines its data processing logic using *column definitions* as opposed to the existing approaches which process data using set operations. In particular, it does not use such difficult to comprehend and execute operations like join and group-by. More about the column-oriented approach to data processing can be found here:
* Root project with FAQ about Bistro: https://github.com/asavinov/bistro
* Bistro Engine sub-project with references to publications: [Bistro Engine](../core)
* Examples of using Bistro Engine and Bistro Streams: [Examples](../examples)

## How it works: a novel approach to stream processing

Internally, Bistro Streams is simply a database consisting of a number of *tables* and *columns*. These tables and columns may have their own *definitions* and it is precisely how data is being processed in the system. In other words, instead of continuously evaluating queries, Bistro Streams evaluates column and table definitions by deriving new data from the existing data. The unique distinguishing feature of Bistro Streams is *how* it processes data: it relies on the mechanism of *evaluating* column and table definitions by deriving their outputs and population, respectively, as opposed to executing set-oriented queries in traditional systems.

The main purpose of of Bistro Streams server is to organize interactions of this internal database (Bistro Engine) with the outside world. In particular, the server provides functions for feeding data into the database and reading data from the database. These functions are implemented by *connectors* which know how to interact with the outside data sources and how to interact with the internal data engine. Thus the internal database is unaware of who and why changes its state - this role is implemented by connectors in the server. On the other hand, Bistro Streams server is unaware of how this data state is managed and how new data is derived - it is the task of Bistro Engine.

Bistro Streams consists of the following components:

* A *schema* instance is a database (Bistro Engine) storing the current data state. Yet, it does not have any threads and is not able to do any operations itself - it exposes only native Java interface.
* A *server* instance (Bistro Streams) provides one or more threads and ability to change the data state. The server knows how to work with the data and its threads are intended for working exclusively with the data state. Yet, the server does not know what to do, that is, it does not have any sources of data and any sources of commands to be executed.
* *Actions* describe commands or operations with data which are submitted to the server by external threads (in the same virtual machine) and are then executed by the server. Internally, actions are implemented in terms of the Bistro Engine API.
* A *task* is simply a sequence of actions. Tasks are used if we want to execute several actions sequentially.
* A *connector* is essentially an arbitrary process which runs in the same virtual machine. Its task is to connect the server with the outside world. On one hand, it knows something about the outside world, for example, how to receive notifications from an event hub using a certain protocol. On the other hand, it knows how to work with the server by submitting actions. Thus connectors are intended for streaming data between Bistro Streams and other systems or for moving data into and out of Bistro Streams. A connector can also simulate events, for example, a timer is an example of a typical and very useful connector which regularly produces some actions like evaluating the database or printing its state.

## Why Bistro Streams: benefits

Here are some benefits and unique features of Bistro Streams:

* Bistro Streams is a light-weight server and therefore very suitable for running on edge devices. It is estimated that dozens billions connected things will come online by 2020 and analytics at the edge is becoming the cornerstone of any successful IoT solution. Bistro Streams is able to produce results and make intelligent decisions immediately at the edge of the network or directly on device as opposed to cloud computing where data is transmitted to a centralized server for analysis. In other words, Bistro Streams is intended to analyze data as it is being created by producing results immediately as close to the data source as possible.
* Bistro Streams is based on a general-purpose data processing engine which supports arbitrary operations starting from simple filters and ending with artificial intelligence and data mining.
* Bistro Streams is easily configurable and adaptable to various environments and tasks. It separates the logic of interaction with the outside world and the logic of data processing. It is easy to implement a custom *connector* to interacte with specific devices or data sources, and it is also easy to implement custom *actions* to interact with the internal data processing engine.
* Bistro Streams is based on a novel data processing paradigm which is conceptually simpler and easier to use than conventional approaches to data processing based on SQL-like languages or MapReduce. It is easier to define new columns rather than write complex queries using joins and group-by which can be difficult to understand, execute and maintain.
* Bistro Streams is very efficient in deriving new data (query execution in classical systems) from small incremental updates (typically when new events are received). It maintains dirty state for each column and table, and knows how to propagate these changes to other elements of the database by updating their state incrementally via inference. In other words, if 10 events have been appended to and 5 events deleted from an event table with 1 million records, then the system will process only these 15 records and not the whole table.
