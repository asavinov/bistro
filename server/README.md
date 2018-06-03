```
  ____  _     _
 | __ )(_)___| |_ _ __ ___  ___________________________
 |  _ \| / __| __| '__/ _ \ 
 | |_) | \__ \ |_| | | (_) |  C O L U M N S  F I R S T
 |____/|_|___/\__|_|  \___/ ___________________________
```
[![Gitter chat](https://badges.gitter.im/gitterHQ/gitter.png)](https://gitter.im/conceptoriented/Lobby)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.conceptoriented/bistro-server/badge.svg)](https://maven-badges.herokuapp.com/maven-central/org.conceptoriented/bistro-server)

---
> **Bistro Streams does for stream analytics what column stores did for databases**
---

* [About Bistro Streams](#about-bistro-streams)
  * [What is Bistro Streams: a stream analytics server](#what-is-bistro-streams-a-stream-analytics-server)
  * [How it works: a novel approach to stream processing](#how-it-works-a-novel-approach-to-stream-processing)
  * [Why Bistro Streams: benefits](#why-bistro-streams-benefits)
* [Getting started with Bistro Streams](#getting-started-with-bistro-streams)
  * [Creating a schema](#creating-a-schema)
  * [Creating a server](#creating-a-server)
  * [Actions](#actions)
  * [Connectors](#connectors)
* [How to build](#how-to-build)
* [How to use](#how-to-use)
* [More information](#more-information)

# About Bistro Streams

## What is Bistro Streams: a stream analytics server

Bistro Streams is a light-weight column-oriented *stream analytics server* which radically changes the way stream data is processed. It is a *general-purpose* server which is not limited by in-stream analytics but can also be applied to batch processing including such tasks as data integration, data migration, extract-transform-load (ETL) or big data processing. Yet, currently its implemented features are more focused on stream analytics with applications in IoT and edge computing.

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

# Getting started with Bistro Streams

## Creating a schema

First, we need to create a database where all data will be stored and which will execute all operations. For example, we could create one table which has one column:

```java
Schema schema = new Schema("My Schema");
Table table = schema.createTable("EVENTS");
Column column = schema.createColumn("Temperature", table);
```

## Creating a server

A *server* instance is created by passing the schema object to its constructor as a parameter:

```java
Server server = new Server(schema);
```

The server will be responsible for executing all operations on the data state managed by Bistro Engine. In particular, it is resonsible for making all operations safe including thread-safety. After starting the server, we should not access the schema directly using its API because it is responsibility of the server and the server provides its own API for working with data:

```java
server.start();
```

Now the server is waiting for incoming actions to be executed.

## Actions

An *action* represents one user-defined operation with data. Actions are submitted to the server and one action will be executed within one thread. There are a number of standard actions which implement conventional operations like adding or removing data elements.

For example, we could create a record, and then add it to the table via such an action object:

```java
Map<Column, Object> record = new HashMap<>();
record.put(column, 36.6);
Action action = new ActionAdd(table, record);
server.submit(action);
```

The server stores the submitted actions in the queue and then decides when and how to execute them.

If we want to execute a sequence of operations then they have to be defined as a *task*. For example, we might want to delete unnecessary records after adding each new record:

```java
Action action1 = new ActionAdd(table, record);
Action action2 = new ActionRemove(table, 10);

Task task = new Task(Arrays.asList(action1, action2), null);

server.submit(task);
```

This will guarantee that the table will have maximum 10 records and older records will be deleted by the second action. In fact, it is an example how retention policy can be implemented.

More complex actions can be defined via user-defined functions:

```java
server.submit(
        x -> System.out.println("Number of records: " + table.getLength())
);
```

The server will execute this action and print the current number of records in the table.

## Connectors

The server instance is not visible from outside - it exposes only its action submission API within JVM. The server itself knows only how to execute actions while and receiving or sending data is performed by *connectors*. A connector is supposed to have its own thread in order to be able to asynchronously interact with the outside world. 

In fact, Bistro does not restrict what connectors can do - it is important only that the decouple the logic of interaction with external processes from the internal logic of action processing. In particular, connectors are used for the following scenarios:
* Batch processing. Such a connector will simply load the whole or some part of the input data set into the server. It can do it after receiving certain event or automatically after start.
* Stream processing. Such a connector will create a listener for certain event types (for example, by subscribing to a Kafka topic) and then submit an action after receiving new events. Its logic can be more complicated. For example, it can maintain an internal buffer in order to wait for late events and then submit them in batches. Or, it can do some pre-processing like filtering or enrichment (of course, this could be also done by Bistro Engine).
* Timers. Timers are used for performing regular actions like sending output, checking the state of some external processes, evaluating the data state by deriving new data, or implementing some custom retention policy (deleting unnecessary data).
* Connectors are also supposed to sink data to external event hubs or data stores. For example, in the case some unusual behavior has been detected by the system (during evaluation) such a connector can append a record to a database or sent an alert to an event hub. 
* A connector can also implement a protocol for accessing data stored in the server like JDBC. External clients can then connect to the server via JDBC and visually explore its current data state.

In our example, we can use a standard timer to simulate a data source by regularly adding random data to the table:

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

After adding each new record, we want to evaluate the schema by deriving new data:

```java
timer.addAction(new ActionEval(schema));
```

(In our example it will do nothing because we do not have derived columns with their custom definitions.)

Once a connector has been configured it has to be started.

```java
timer.start();
```

# How to build

From the project folder (`git/bistro/server`) execute the following to clean, build and publish the artifact:

```console
$ gradlew clean
$ gradlew build
$ gradlew publish
```

The artifact will be placed in your local repository from where it will be available in other projects.

# How to use

In order to include this artifact into your project add the following lines to dependencies of your `build.gradle`:

```groovy
dependencies {
    compile("org.conceptoriented:bistro-core:0.8.0")
    compile("org.conceptoriented:bistro-server:0.8.0")

    // Other dependencies
}
```

# More information

* How to write programs for Bistro Streams: [Bistro Engine sub-project](../core)
* FAQ: [Root project](https://github.com/asavinov/bistro)
* Examples of using Bistro Engine and Bistro Streams: [Examples](../examples)
