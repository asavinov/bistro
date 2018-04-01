```
  ____  _     _
 | __ )(_)___| |_ _ __ ___  ___________________________
 |  _ \| / __| __| '__/ _ \ 
 | |_) | \__ \ |_| | | (_) |  C O L U M N S  F I R S T
 |____/|_|___/\__|_|  \___/ ___________________________
```

> General information about Bistro including what is Bistro, how it works, its formal basis and why Bistro should be used can be found in the description of the root project: [../README.md](https://github.com/asavinov/bistro)

* [Getting started with Bistro Streams](#getting-started-with-bistro-streams)

# Getting started with Bistro Streams

## Creating a server

A *server* is responsible for *executing* operations on the data state managed by the Bistro Engine. In particular, it is resonsible for making all operations safe including thread-safity. Thus it is necessary first to create a schema which will be managed by the server. For example, we could create one table which has one column:

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

An *action* reresents one user-defined operation with data. Actions are submitted to the server and one action will be executed within one thread. There are a number of standard actions which implement conventional operations like adding or removing data elements. 

For example, we could create a record, and then add it to the table via such an action object:

```java
Map<Column, Object> record = new HashMap<>();
record.put(columns, 36.6);
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

This will guarantee that the table will have maximum 10 records and older records will be deleted.

More complex actions can be defined via user-defined functions:

```java
server.submit(
        x -> System.out.println("Number of records: " + table.getLength())
);
```

The server will execute this action and printe the current number of records in the table.

## Connectors

Submitting actions can be used for batch processing scenarios. In the case of stream analytics, the server receives and processes data continuously. However, the server itself knows only how to execute the actions and receiving and sending data is performed by *connectors*.

For example, a standard timer connector can be used to perform some actions regularly. In our example, we could regularly add random data to the table by customizing the time connector:

```java
ConnectorTimer timer = new ConnectorTimer(server,500);
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

Once a connector has been configured it has to be started.

```java
connector.start();
```

There are different types of connectors but they can be divided into two big classes:

* asynchronous connectors which are triggered by events coming from outside like event hubs or incoming http requests
* synchronous connectors which regularly request new data from some data source like database or a remote service

In any case, a connector will receive events and then add them to the server.

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
    compile("org.conceptoriented:bistro-core:0.7.0-SNAPSHOT")
    compile("org.conceptoriented:bistro-formula:0.7.0-SNAPSHOT")
    compile("org.conceptoriented:bistro-server:0.7.0-SNAPSHOT")

    // Other dependencies
}
```
