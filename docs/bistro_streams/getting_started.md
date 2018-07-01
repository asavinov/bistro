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
