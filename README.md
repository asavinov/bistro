```
  ____  _     _              
 | __ )(_)___| |_ _ __ ___   
 |  _ \| / __| __| '__/ _ \  ▪ Calculate
 | |_) | \__ \ |_| | | (_) | ▪ Link
 |____/|_|___/\__|_|  \___/  ▪ Accumulate
```

# Table of Contents

* [Bistro: Calculate-Link-Accumulate](#bistro-calculate-link-accumulate)
    * [What is Bistro: a data processing engine](#what-is-bistro-a-data-processing-engine)
    * [How it works: a novel data processing paradigm](#how-it-works-a-novel-data-processing-paradigm)
    * [Formal basis: Concept-Oriented Model](#formal-basis-concept-oriented-model)
* [Getting started with Bistro](#getting-started-with-bistro)
    * [Schema elements](#schema-elements)
        * [Creating schema](#creating-schema)
        * [Creating tables](#creating-tables)
        * [Creating columns](#creating-columns)
    * [Defining columns](#defining-columns)
        * [Calculate columns](#calculate-columns)
        * [Link columns](#link-columns)
        * [Accumulate columns](#accumulate-columns)
        * [Numeric accumulation](#numeric-accumulation)
        * [Column paths](#column-paths)
    * [Defining tables](#defining-tables)
        * [Product tables](#product-tables)
        * [Where functions](#where-functions)
        * [Project columns](#project-columns)
    * [Schema evaluation](#schema-evaluation)
* [More information](#more-information)

# Bistro: Calculate-Link-Accumulate

## What is Bistro: a data processing engine

*Bistro* is a light-weight data processing engine which changes the way data is being processed. As a *general-purpose* data processing engine, Bistro can be applied to many problems like data integration, data migration, extract-transform-load (ETL), big data processing, stream analytics, big data processing. 

## How it works: a novel data processing paradigm

At its core, Bistro relies on a novel *column-oriented* logical data model which describes data processing as a DAG of *column operations*. Computations in Bistro are performed by *evaluating* all column definitions each of which describing how this column output values are expressed in terms of other columns. In this sense, it is opposed to most other models and frameworks which are based on table (set) operations for data processing.

Bistro provides three column definition (operation) types: calculate columns, link columns and accumulate columns. This novel *calculate-link-accumulate* (CLA) data processing paradigm is an alternative to conventional SQL-like languages, map-reduce and other set-oriented approaches. In set-oriented approaches, data is being processed by producing new sets (tables, collections, lists etc.) from the data stored in other sets by applying various set operations like join, group-by, map or reduce. In CLA, data is being processed by producing new columns from existing columns by applying three main operations: calculate, link and accumulate. The calculate operation roughly corresponds to the map and SQL select operations, link roughly corresponds to the join operation, and accumulate is a column-oriented analogue of group-by and reduce. Essentially, the use of column definitions makes CLA similar to conventional spreadsheets, which are known to be rather intuitive and easy to use for data processing with the difference that column definitions are used instead of cell formulas.

## Formal basis: Concept-Oriented Model

Formally, Bistro relies on the *concept-oriented model* (COM) where the main unit of representation and processing is that of a *function* as opposed to using only sets in the relational and other set-oriented models. An advantage of COM is that it does not use such operations as join and group-by which are known to be error-prone, difficult to comprehend, require high expertise and might be inefficient when applied to analytical data processing workloads. 

# Getting started with Bistro

## Schema elements

### Creating schema

First, it is necessary to create a *schema* which can be thought of as a database and will be a collection of all other elements and parameters: 
```java
Schema schema = new Schema("My Schema");
```

A schema like tables and columns has an arbitrary (case sensitive) name. The schema is then used to create and access other elements as well as perform various operations with data. 

### Creating tables

Tables are created within the schema by providing a unique name:
```java
Table things = schema.createTable("THINGS");
Table events = schema.createTable("EVENTS");
```

A table in the concept-oriented model is a mathematical set, that is, a number of (unique) values. In Bistro, all user-defined tables are sets of primitive values the structure of which cannot be changed. These values are of long type and are interpreted as row identifiers without any additional semantics. 

There exist predefined *primitive tables* which consist of only primitive values. Currently, Bistro has only one primitive table with the name `Object` which is a set of Java objects. It is impossible to create another table with this name and do some operations with this table. 

Tables can be accessed by using their name:
```java
Table table = schema.getTable("THINGS");
Table objects = schema.getTable("Object"); // Primitive
```

Elements can be appended to a table and the returned result is their identifier:
```java
long id;
id = things.add(); // id = 0
id = things.add(); // id = 1
```
Elements are added and removed in the FIFO order, that is, the oldest element is always removed. The current range of valid identifiers can be retrieved using this method:
```java
Range range = table.getIdRange();
```
The `Range` object provides a start id (inclusive) and an end id (exclusive) for this table. These ids can be then used for data access using columns (not tables).

Any table can be used as a *data type* for schema columns.

### Creating columns

Data in Bistro is stored in columns. Formally, a column is a function and hence it defines a mathematical *mapping* from all table inputs to the values in the output table. Input and output tables of a column are specified during creation: 
```java
Column thingName = schema.createColumn("Name", things, objects);
```
This column defines a mapping from "THINGS" to the "Object" (primitive) table.

A new column does not have a definition and hence it cannot derive its output values. The only way to define their mapping is to explicitly set the output value for certain inputs using API:
```java
thingName.setValue(0, "fridge");
thingName.setValue(1, "oven");
Object value = name.getValue(1);
```

## Defining columns

### Calculate columns

A column might have a *definition* which means that it uses some operation to automatically derive (infer) its output values from the data in other columns (which in turn can derive their outputs from other columns). Depending on the logic behind such inference, there are different column definition types. The simplest derived column type is a *calculate* column: 

> For each input, a calculate column *computes* its output by using the outputs of some other columns of this same table for this same input

For example, we could define a calculate column which increments the value stored in another column:
```java
Column calc = schema.createColumn("Name Length", things, objects);
calc.calc(
        p -> ((String)p[0]).length(), // How to compute
        thingName // One parameter to compute the column
);
```
The first parameter is a lambda function. Its argument `p` is an array of outputs of other columns used to compute the output of the calculate column. The second parameter of the definition specifies the input columns used for calculations. In this example, we want to find the length of the device name and hence we pass this column reference as a parameter. The size of the `p` array has to be equal to the number of columns references passed via the second parameter. 

There exist also other ways to define calculate columns which can be more convenient in different situations, for example, in the case of complex arithmetic operations or in the case of complex computations implemented programmatically. Note also that column outputs could contain `null` values and all lambda functions must guarantee the validity of its computations including null-safety and type-safety.

### Link columns

The second column type is a *link* column. Link columns are typed by user (not primitive) tables and their output essentially is a reference to some element in the output table:

> For each input, a link column *finds* its output in the output table by providing equality criteria for the output elements. These values for these criteria are computed from the columns in this table using this input similar to calculate columns.

Let us assume that the "EVENTS" table stores records with a property (column) which can be used to link them to the "THINGS" table:
```java
Column eventThingName = schema.createColumn("Thing Name", events, objects);

facts.add(3);
eventThingName.setValue(0, "oven");
eventThingName.setValue(1, "fridge");
eventThingName.setValue(2, "oven");
```

This property however cannot be used to access the elements of the "THINGS". Therefore, we define a link column which will directly reference elements in "THINGS":
```java
Column link = schema.createColumn("Thing", events, things);
link.link(
        new Column[] { thingName }, // Columns to be used for search (in the type table)
        eventThingName // Columns providing search criteria (in this input table)
);
```
This definition essentially means that an event record will directly reference a thing record having the same name, that is, 
`EVENTS::Name == THINGS::Name`.

The main benefit of having link columns is that they are evaluated once but can be then used in many other column definitions for *direct* access to elements of another table without searching or joining records. 


It is possible that many target elements satisfy the link criteria and then one of them is chosen as the output value. In the case no output element has been found, either `null` is set as the output or a new element is appended depending on the chosen option. There exist also other ways to define links, for example, by providing lambdas for computing link criteria.

### Accumulate columns

Accumulate columns are intended for data aggregation. In contrast to other columns, an output of an accumulate column is computed incrementally: 

> For each input, an accumulate column computes its output by *updating* its current values several times for each element in another table which is mapped to this input by the specified grouping column.

It is important that a definition of an accumulate column involves two additional parameters:
* Link column from the fact table to this table (where the accumulate column is defined), called grouping column
* Table with the data being aggregated, called fact table (type of the link column)

How the data is being aggregated is specified in the `accumulate` or update function. This function has two major differences from calculate functions:
* Its parameters are read from the columns of the fact table - not this table (where the new column is being defined)
* It receives one additional parameters which is its own current output (resulted from the previous call to this function). The function has to update its own current value using the parameters and return a new value (which it will receive next time).

If we want to simply count the number of events for each device then such a column can be defined as follows:
```java
Column counts = schema.createColumn("Event Count", things, objects);
counts.accu(
        link, // How to group facts
        p -> (Double)p[0] + 1.0 // How to accumulate/update
        // No additional parameters because we only count
);
counts.setDefaultValue(0.0); // It will be used as an initial value
```
Here the `link` column maps elements of the "EVENTS" table to elements of the "THINGS" table, and hence an element of "THINGS" (where we define the accumulate column) is a group of all elements of "EVENTS" which reference it via this column. For each element of the "EVENTS", the specified accumulate function will be called and its result stored in the column output. Thus the accumulate function will be called as many times for each input of "THINGS", as it has facts that map to it.

### Numeric accumulation

Let us assume now that the "EVENTS" table has a property "Measure" and we want to numerically aggregate it (instead of simply counting):
```java
Column measure = schema.createColumn("Measure", things, objects);
measure.setValue(0, 1.0);
measure.setValue(1, 2.0);
measure.setValue(2, 3.0);
```

We can find the sum of the measure for each element in the "THINGS" table using this accumulate column:
```java
Column sums = schema.createColumn("Sum Measure", things, objects);
sums.accu(
        link, // Grouping column
        p -> (Double)p[0] + (Double)p[1], // Add the measure for each new fact
        measure // Measure
);

sums.eval();
value = sums.getValue(1); // 3 (1+2)
value = sums.getValue(2); // 3
```

### Column paths

A *column path* is a sequence of columns where each next column belongs to the type of the previous column. In programming, column paths are written using dot notation. For example, we could define a column object and then use it to *directly* access the number of events received from this same event device:
```java
ColumnPath path = new ColumnPath(link, accu);
value = path.getValue(0);
```

Many column definition methods accept column paths as parameters rather than simply column. 

## Defining tables

### Product tables 

When a new table is created it by default has no definition and hence it will not participate in inference. The only way to populate such tables is to add or remove its elements using API. If we want to populate a table using data in other columns and tables then it has to be defined as a *product table*: 
```java
myTable.prod();
```

In addition, product tables must have one or more *key columns* of non-primitive type which are defined columns with no definition and an additional parameter specifying that it is a key column: 
```java
Column myKey1 = schema.createColumn("Key1", myTable, T1);
myKey1.noop(true);
Column myKey2 = schema.createColumn("Key2", myTable, T2);
myKey2.noop(true);
```
Now the product table will be populated by all combinations of records currently stored in tables `T1` and `T2`. 

### Where functions 

If it is necessary to filter records in a product table then it is necessary to define a where-function. 

> A table will store a record only if the where-function is true. 

It is defined by providing a lambda-function as well as the necessary parameters: 
```java
myTable.where(
        p -> p[0] == 123 || p[1] == 456, 
        myKey1, myKey2 
);
```
(Currently, only key columns can be used in a where-function.) 

### Project columns 

*Project columns* are also a means of populating tables. 

> A project column is similar to link columns but in addition it will append a new record to the linked table if it has not been found 

## Schema evaluation

All columns in the schema are evaluated using the following method call:
```java
schema.eval();
```

A column can be evaluated individually, for example, if its definition has been changed:
```java
calc.eval();
```
Bistro manages all dependencies and it will automatically (recursively) evaluate all columns this column depends on (if necessary). If a column data has been modified or its definition has changed then it will also influence all columns that depend on it directly or indirectly. 

Now, if there were no errors, we can retrieve the output values:
```java
value = calc.getValue(0); // value = 6
value = calc.getValue(1); // value = 4

value = link.getValue(0); // value = 1
value = link.getValue(1); // value = 0
value = link.getValue(2); // value = 1

value = counts.getValue(0); // 1 event from fridge
value = counts.getValue(1); // 2 events from oven
```

# More information

* More information on all aspects of the concept-oriented model and concept-oriented programming including publications can found here:
    * http://www.conceptoriented.org

* A web application based on the same principles as Bistro can be evaluated here:
    * http://dc.conceptoriented.com/

* Alexandr Savinov is an author of Bistro as well as the underlying concept-oriented model (COM):
    * http://conceptoriented.org/savinov
    * https://www.researchgate.net/profile/Alexandr_Savinov

* Some papers about this approach:
    * A. Savinov. DataCommandr: Column-Oriented Data Integration, Transformation and Analysis. Proc. IoTBD 2016, 339-347. https://www.researchgate.net/publication/301764506_DataCommandr_Column-Oriented_Data_Integration_Transformation_and_Analysis
    * A. Savinov. ConceptMix: Self-Service Analytical Data Integration based on the Concept-Oriented Model. A. Savinov. 3rd International Conference on Data Technologies and Applications (DATA 2014), 78-84. https://www.researchgate.net/publication/265301356_ConceptMix_Self-Service_Analytical_Data_Integration_based_on_the_Concept-Oriented_Model

# License

See LICENSE file in the project directory
