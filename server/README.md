```
  ____  _     _
 | __ )(_)___| |_ _ __ ___  ___________________________
 |  _ \| / __| __| '__/ _ \ 
 | |_) | \__ \ |_| | | (_) |  C O L U M N S  F I R S T
 |____/|_|___/\__|_|  \___/ ___________________________
```

---
> **Bistro Streams does for stream analytics what column stores did for databases**
---


* [About Bistro Streams](#about-bistro-streams)
  * [What is Bistro Streams: a stream analytics server](#what-is-bistro-streams-a-stream-analytics-server)
  * [How it works: a novel approach to stream processing](#how-it-works-a-novel-approach-to-stream-processing)
  * [Why Bistro Streams: benefits](#why-bistro-streams-benefits)
* [Getting started with Bistro Streams](#getting-started-with-bistro-streams)
  * [Creating a server](#creating-a-server)
  * [Actions](#actions)
  * [Connectors](#connectors)
* [How to build](#how-to-build)

# About Bistro Streams

## What is Bistro Streams: a stream analytics server

Bistro Streams is a light-weight column-oriented *stream analytics server* which radically changes the way stream data is processed. In contrast to other stream processing systems, Bistro Streams defines its data processing logic using *column definitions*. In particular, it does not use such difficult to comprehend and execute operations like join and group-by. In fact, it is a *general-purpose* server which can be used not only for in-stream analytics but also for other tasks data like integration, data migration, extract-transform-load (ETL) or big data processing. Yet, currently its features are more focused on stream analytics with applications in IoT and edge computing.

## How it works: a novel approach to stream processing

Internally, Bistro Streams is simply a database consisting of a number of tables and columns. These tables and columns may have their own *definitions* and it is precisely how data is being processed in the system. In other words, instead of continuously evaluating queries, Bistro Streams evaluates column and table definitions by deriving new data from the existing data. The unique distinguishing feature of Bistro Streams is *how* it processes data: it relies on the mechanism of evaluating column and table definitions by deriving their outputs and population, respectively, as opposed to executing set-oriented queries in traditional systems.

The role of Bistro Streams is to organize interaction of this internal database with the outside world. In particular, the server provides functions for (synchronously or asynchronously) feeding data into the database and reading data from the database. These functions are implemented by *connectors* which know how to interact with the outside data sources and how to interact with the internal data engine. Thus the internal database (Bistro Engine) is unaware of who and why changes its state - this role is implemented by connectors in the server. On the other hand, Bistro Streams is unaware of how this data state is managed - it is the task of Bistro Engine.

Bistro Streams consists of the following components:

* A *schema* instance is a database (Bistro Engine) storing the current data state. Yet, it does not have any threads and is not able to do any operations itself.
* A *server* instance provides one or more threads and ability to change the data state. The server knows how to work with the data and its threads are intended for working exclusively with the data state. Yet, the server does not know what to do, that is, it does not have any sources of data and any source of commands to be executed.
* *Actions* describe operations with data which are submitted to the server by external threads (in the same virtual machine) and are then executed by the server. Actions are written in terms of the Bistro Engine API.
* A *task* is simply a sequence of actions. Tasks are used if we want to execute several actions sequentially.
* A *connector* is essentially an arbitrary process which runs in the same virtual machine. Its task is to connect the server with the outside world. On one hand, it knows something about the outside world (e.g., how to receive notifications from an event hub using a certain protocol). On the other hand, it knows how to work with the server by submitting actions. Thus connectors are intended for streaming data between Bistro Streams and other systems or for moving data into and out of Bistro Streams.

## Why Bistro Streams: benefits

Here are some benefits and unique features of Bistro Streams:

* Bistro Streams is a light-weight server and therefore very suitable for running on edge devices. It is estimated that dozens billions connected things will come online by 2020 and analytics at the edge is becoming the cornerstone of any successful IoT solution. Bistro Streams is able to produce results and make intelligent decisions immediately at the edge of the network or directly on device (as opposed to cloud computing where data is transmitted to a centralized server for analysis). In other words, Bistro Streams is intended to analyze data as it is being created by producing results immediately as close to the data source as possible
* Bistro Streams is based on a general-purpose data processing engine which supports arbitrary operations with data starting from simple filters and ending with artificial intelligence and data mining.
* Bistro Streams is easily configurable and adaptable to various environments and tasks. It separates the logic of interaction with the outside world and the logic of data processing. It is easy to implement a custom *connector* for interacting with specific devices or data sources, and it is also easy to implement custom *actions* to interact with the internal data processing engine.
* Bistro Streams is based on a novel data processing paradigm which is conceptually simpler and easier to use than conventional approaches to data processing based on SQL-like languages or MapReduce. In order to define how data has to be processed it is enough to define new columns as opposed to writing complex queries which can be difficult to understand, execute and maintain.
* Bistro Streams is very efficient in deriving new data (query execution in classical systems) from small incremental updates (typically when new events are received). It maintains dirty state for each column and data, and knows how to propagate it to other elements of the database by updating their state incrementally via inference.

# Getting started with Bistro Streams

## Creating a server

A *server* is responsible for *executing* operations on the data state managed by the Bistro Engine. In particular, it is resonsible for making all operations safe including thread-safety. Thus it is necessary first to create a schema which will be managed by the server. For example, we could create one table which has one column:

```java
Schema schema = new Schema("My Schema");
Table table = schema.createTable("EVENTS");
Column column = schema.createColumn("Temperature", table);
```

Now we can create a server instance by passing the schema object to its constructor as a parameter:

```java
Server server = new Server(schema);
```

After starting the server, we should not access the schema directly using its API because it is responsibility of the server and the server provides its own API for working with data:

```java
server.start();
```

## Actions

An *action* represents one user-defined operation with data. Actions are submitted to the server and one action will be executed within one thread. There are a number of standard actions which implement conventional operations like adding or removing data elements. 

For example, we could create a record, and then add it to the table via such an action object:

```java
Map<Column, Object> record = new HashMap<>();
record.put(column, 36.6);
Action action = new ActionAdd(table, record);
server.submit(action);
```

The server then decides when and how to execute this operation.

If we want to execute a sequence of operations then they have to be defined as a *task*. For example, we might want to delete unnecessary records after adding each new record:

```java
Action action1 = new ActionAdd(table, record);
Action action2 = new ActionRemove(table, 10);

Task task = new Task(Arrays.asList(action1, action2), null);

server.submit(task);
```

This will guarantee that the table will have maximum 10 records and older records will be deleted by the second action.

More complex actions can be defined via user-defined functions:

```java
server.submit(
        x -> System.out.println("Number of records: " + table.getLength())
);
```

The server will execute this action and print the current number of records in the table.

## Connectors

Submitting actions can be used for batch processing scenarios. In the case of stream analytics, the server receives and processes data continuously. However, the server itself knows only how to execute the actions, and receiving or sending data is performed by *connectors*.

For example, a standard timer connector can be used to perform some actions regularly. In our example, we could regularly add random data to the table by using the standard time connector:

```java
ConnectorTimer timer = new ConnectorTimer(server,500); // Do something every 500 milliseconds
timer.addAction(
        x -> {
            long id = table.add();
            double value = ThreadLocalRandom.current().nextDouble(30.0, 40.0);
            column.setValue(id, value);
        }
);
```

After adding each new record, we can evaluate the schema by deriving new data:

```java
timer.addAction(new ActionEval(schema));
```

(In our example it will do nothing because we do not have derived columns with their own definitions.)

Once a connector has been configured it has to be started.

```java
timer.start();
```

There are different types of connectors but they can be divided into two big classes:

* asynchronous connectors which are triggered by events coming from outside like event hubs or incoming http requests
* synchronous connectors which regularly request new data from some data source like database or a remote service

In any case, a connector will receive events and then add them to the server. Connectors also are supposed to sink data to external event hubs or data stores. For example, in the case some unusual behavior has been detected by the system (during evaluation) such a connector can append a record to a database or sent an alert to an event hub. A connector can also implement a protocol for accessing the data in the server like JDBC. External clients can then connect to the server via JDBC and visually explore its current data state.

# How to build

From the project folder (`git/bistro/server`) execute the following to clean, build and publish the artifact:

```console
$ gradlew clean
$ gradlew build
$ gradlew publish
```

The artifact will be placed in your local repository from where it will be available in other projects.

In order to include this artifact into your project add the following lines to dependencies of your `build.gradle`:

```groovy
dependencies {
    compile("org.conceptoriented:bistro-core:0.7.0")
    compile("org.conceptoriented:bistro-formula:0.7.0")
    compile("org.conceptoriented:bistro-server:0.7.0")

    // Other dependencies
}
```

# How to build

# How to build

From the project folder (`git/bistro/server`) execute the following commands to clean, build and publish the artifact:

```console
$ gradlew clean
$ gradlew build
$ gradlew publish
```

The artifact will be stored in your local repository from where it will be available to other projects.

In order to include this artifact into your project add the following lines to dependencies of your `build.gradle`:

```groovy
dependencies {
    compile("org.conceptoriented:bistro-core:0.7.0")

    // Other dependencies
}
```

# Links and references

Links and references can be found in the [root project](https://github.com/asavinov/bistro)

Information about how to write programs for Bistro Streams can be found in the [Bistro Engine sub-project](../core)
