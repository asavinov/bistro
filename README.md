# Bistro: Calculate∙Link∙Accumulate

## Waht is Bistro

*Bistro* is a general purpose, light-weight data processing engine which changes the way data is being processed. It going to be as simple as a spreadsheet and as powerful as SQL. At its core, it relies on a novel *column-oriented* data representation and processing engine which is unique in that its logic is described as a DAG of *column operations* as opposed to table operations in most other frameworks. Computations in Bistro are performed by *evaluating* columns which have custom definitions in terms of other columns. 

## New Data Processing Paradigm: Calc-Link-Accu

Bistro provides three column definition (operation) types: calculate columns, link columns and accumulate columns. This novel *calc-link-accu* (CLA) data processing paradigm is an alternative to conventional SQL-like languages, map-reduce and other set-oriented approaches. In set-oriented approaches, data is being processed by producing new sets (tables, collections, lists etc.) from the data stored in other sets by applying various set operations like join, group-by, map or reduce. In CLA, data is being processed by producing new columns from existing columns by applying three main operations: calculate, link and accumulate. Calculate operation roughly corresponds to the map and select operations, links roughly correspond to the join operation, and accumulate is a column-oriented analogue of group-by and reduce. 

## Formal basis: Concept-Oriented Model (COM)

Formally, Bistro relies on the *concept-oriented model* (COM) where the main unit of representation and processing is that of a *function* as opposed to using only sets in the relational and other set-oriented models. An advantage of this model is that it does not use such operations as join and group-by which are known to be error-prone, difficult to comprehend, require high expertise and might be inefficient when applied to analytical data processing workloads. Essentially, the use of functions makes CLA much closer to conventional spreadsheets which are known to be rather intuitive and easy to use for data processing. However, Bistro and CLA apply the functional approach in the context of table data rather than cells in spreadsheets.

## Where Bistro is Useful

As a *general-purpose* data processing engine, Bistro can be applied to many problems like data integration, data migration, extract-transform-load (ETL), big data processing, stream analytics, big data processing.

# How it works

## Schema

First of all, it is necessary to create a *schema* which can be thought of as a database and will be a collection of all other elements and parameters: 
```java
Schema schema = new Schema("My Schema");
```

A schema like tables and columns has an arbitrary (case sensitive) name. The schema is then used to create and access other elements as well as perform various operations with data. 

## Tables

Tables are created within the schema by providing a unique (case-sensitive) name:
```java
Table table = schema.createTable("Table");
Table facts = schema.createTable("Facts");
```

A table in the concept-oriented model is a set (unique) values including complex values (tuples), that is, it is a mathematical set. In Bistro, all user-defined tables are sets of primitive values the structure of which cannot be changed. These values are of long type and are interpreted as identifiers without any additional domain-specific semantics. 

There exist predefined primitive tables. Currently, Bistro has only one primitive tables with the name `Object`. It is impossible to create another table with this name and do some operations with this table. Tables can be accessed by using their name:
```java
Table table = schema.getTable("Table");
Table objects = schema.getTable("Object"); // Primitive
```

Since a table is a set of elements, Bistro provides methods for adding and deleting elements (identifiers) from a (user-defined) table:
```java
long id;
id = table.add(); // Append a new element: id = 0
id = table.add(); // Remove the oldest element: id = 1
id = table.remove(); // Remove the oldest element: id = 0
```
It is important that elements are removed from the beginning in the FIFO order, that is, the oldest element is always removed. The addition and removal of elements changes the range of the valid identifiers of this table. The current range of identifiers can be retrieved using this method:
```java
Range range = table.getIdRange();
```
The `Range` object provides a start id (inclusive) and an end id (exclusive) for this table. These ids can be then used for data access using columns (not tables).

Any table can be used as a *data type* for schema columns.

## User columns

Data in Bistro is stored in columns. Formally, a column is a function and hence it defines a *mapping* from all inputs to the outputs. Both input and output tables of a column are specified during creation: 
```java
Column name = schema.createColumn("name", table, objects);
```
This column defining a mapping from the "My Table" to the "Object" (primitive) table.

Any new column has not definition and it cannot derive its output values. Such columns are referred to as *user columns* because the only way to specify their outputs for input is to use API:
```java
name.setValue(1, "abc");
table.add();
name.setValue(2, "abc def");
Object value = name.getValue(1); // value = "abc"
```

## Calculate columns

A column might have a *definition* which means that it uses some operations to automatically derive or infer its output values from the data in other columns (which could be user columns or also derived columns). Depending on the logic behind such inference, there are different column definition types. Probably the simplest derived column type is a *calculate* column: 

> For each input, a calculate column *computes* its output by using the outputs of some other columns of this same table for this same input

For example, we could define a calculate column which increments the value stored in another column:
```java
Column calc = schema.createColumn("length", table, objects);
calc.calc(
  (p, o) -> ((String)p[0]).length(), // How to compute
  Arrays.asList(name) // Columns to be used as parameters
  );
```
The first parameter is a function which takes two arguments. The first argument `p` is an array of outputs of other columns that have to be processed. The second argument `o` is the current output of this same column which is not used for calculate columns. The second parameter of the definition specifies the input columns the outputs of which have to be processed. In this example, we use a previously defined user column the outputs of which will be incremented. The size of the `p` array has to be equal to the length of the second parameter. 

The definition itself does not do any computations, that is, the outputs of this calculate column will have default values. To really derive the outputs of this column it has to be evaluated:
```java
calc.eval();
```
Now, if there were no errors, we can retrieve the output values:
```java
value = calc.getValue(1); // value = 3
value = calc.getValue(1); // value = 7
```

There exist also other ways to define calculate columns which can be more convenient in different situations, for example, in the case of complex arithmetic operations or in the case of complex computations implemented programmatically. Note also that column outputs could contain `null` and all lambda functions must guarantee the validity of its computations including null-safety and type-safety.

## Link columns

Another column type is a *link* column. Link columns are typed by user (not primitive) tables and their output essentially is a reference to some element in the output table:

> For each intput, a link column *finds* its output in the output table by providing equality criteria for the output elemetns. These values for these criteria are computed from the columns in this table using this input similar to calculate columns.

Let us assume that there is a "Facts" table and it stores elements which have a propery which can be used to link them to the "Table":
```java
Table facts = schema.createTable("Facts");
Column group = schema.createColumn("group", facts, objects);
facts.add(3);
group.setValue(0, "abc");
group.setValue(1, "abc");
group.setValue(2, "abc def");
```

This property however cannot be used to access the elements of the "Table". Therefore, we define a link column which will directly reference elements in "Table":
```java
Column link = schema.createColumn("link to table", facts, table);
link.link(
  Arrays.asList(name) // Columns of the type table to search a target element
  Arrays.asList(group) // Expressions. Columns of this table providing criteria
  );
```
This link column can be now evaluated:
```java
link.eval();
```
Now its output values are ids of its type table "Table"
```java
value = link.getValue(0); // value = 1
value = link.getValue(1); // value = 1
value = link.getValue(2); // value = 2
```

It is possible that many elements satisfy the link criteria and then one of them is chosen as the output value. In the case no output element has been found, either `null` is set as the output or a new element is appended depending on the options. There exist also other ways to define links, for example, by providing lambdas for computing the link criteria.

## Accumulate columns

Accumulate columns are intended for data aggregation. In contrast to other columns, an output of an accumulate column is computed incrementally: 

> For each input, an accumulate column computes its output by *updating* its current values several times for each element in another table which is mapped to this input by the specified grouping column.

It is important that a definition of an accumulate column involves two additional parameters:
* Table with the data being aggregated, called fact table
* Link column from the fact table to this table (where the accumulate column is defined), called grouping column

How the data is being aggregated is specified in the accumulate or update function. This function has two major semantic differences from the calculate functions:
* Its parameters are read from the columns of the fact table - not this table
* It receives one additional parameters which is its own value resulted from the previous call to this function. The function has to update this value using the parameters and return a new value (which it will receive next time).

If we want to simply count the number of facts belonging to each element of the table then such a column can be defined as follows:
```java
Column counts = schema.createColumn("count facts", table, objects);
counts.accu(
  (p, o) -> (Double)o + 1.0, // How to accumulate/update
  null, // Nothing to aggregate except for counting
  link // How to group/map facts to this table
  );

counts.eval();
value = counts.getValue(1); // 2 occurrences of "abc"
value = counts.getValue(2); // 1 occurrence of "abc def"
```
Here the `link` column maps elements of the "Facts" to elements of the "Table", and hence an element of "Table" (where we defined the accumulate column) is a group of all elements of "Facts" which reference it via this column. For each element of the "Facts", the specified accumulate function will be called and its result stored in the column output. Thus the accumulate function will be called as many times for each input of "Table", as its has facts that map to it.

Let us assume now that the "Facts" table has a propery "Measure" we want to numerically aggregate (instead of simply counting):
```java
Column measure = schema.createColumn("measure", facts, objects);
measure.setValue(0, 1.0);
measure.setValue(1, 2.0);
measure.setValue(2, 3.0);
```

We can find the sum of the measure for each element in the "Table" using this accumulate column:
```java
Column sums = schema.createColumn("sum measure", table, objects);
sums.accu(
  (p, o) -> (Double)o + (Double)p[0], // Add the measure for each new fact
  Arrays.asList(measure) // Measure
  link // Grouping column
  );

sums.eval();
value = sums.getValue(1); // 3 (1+2)
value = sums.getValue(2); // 3
```

Accumulate functions have also other definition options, for example, specifying how the column is initialized and how it is finalized.

## Schema evaluation and dependencies

It is not necessary to evaluate a column immediately after it has been defined. It is possible to define all or some of the columns and then evalute one of them. Bistro manages all dependencies and it will automatically and recursively evaluate all the columns this column depends on. If a column data has been modified or its definition has changed then it will also influence all columns that depend on it directly or indirectly.

It is also possible to evalute the whole schema:
```java
schema.eval();
```
This method will evaluate all columns of the schema but only if it is necessary and possible (if there are no errors in their definitions).
