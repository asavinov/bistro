# Bistro-core: Calculate∙Link∙Accumulate

# What is Bistro-core

Bistro-core is a core library of Bistro for schema management, data representation and data processing.

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
table.add();
```
It is important that elements are removed from the beginning in the FIFO order, that is, the oldest element is always removed. The addition and removal of elements changes the range of the valid identifiers of this table. The current range of identifiers can be retrieved using this method:
```java
Range range = table.getIdRange();
```
The `Range` object provides a start id (inclusive) and an end id (exclusive) for this table. These ids can be then used for data access using columns (not tables).

Any table can be used as a *data type* for schema columns.

## No definition columns

Data in Bistro is stored in columns. Formally, a column is a function and hence it defines a *mapping* from all table inputs to the values in the output table. Input and output tables of a column are specified during creation: 
```java
Column name = schema.createColumn("name", table, objects);
```
This column defines a mapping from "My Table" to the "Object" (primitive) table.

A new column does not have a definition and hence it cannot derive its output values. The only way to define their mapping from outputs for inputs is to explicitly set the outputs using API:
```java
name.setValue(1, "abc");
name.setValue(2, "abc def");
Object value = name.getValue(1); // value = "abc"
```

## Calculate columns

A column might have a *definition* which means that it uses some operation to automatically derive or infer its output values from the data in other columns (which in turn can derive their outputs from other columns). Depending on the logic behind such inference, there are different column definition types. The simplest derived column type is a *calculate* column: 

> For each input, a calculate column *computes* its output by using the outputs of some other columns of this same table for this same input

For example, we could define a calculate column which increments the value stored in another column:
```java
Column calc = schema.createColumn("length", table, objects);
calc.calc(
  (p, o) -> ((String)p[0]).length(), // How to compute
  new Column[] {name}) // Parameters for computing
  );
```
The first parameter is a function which takes two arguments. The first argument `p` is an array of outputs of other columns that have to be processed. The second argument `o` is the current output of this same column which is not used for calculate columns. The second parameter of the definition specifies the input columns the outputs of which have to be processed. In this example, we use a previously defined no-definition column the outputs of which will be incremented. The size of the `p` array has to be equal to the length of the second parameter. 

The definition itself does not do any computations, that is, the outputs of this calculate column will have default values. To really derive the outputs of this column it has to be evaluated:
```java
calc.eval();
```
Now, if there were no errors, we can retrieve the output values:
```java
value = calc.getValue(1); // value = 3
value = calc.getValue(2); // value = 7
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
  new Column[] {name}, // Columns to be used for searching (in the type table)
  new Column[] {group} // Columns providing criteria for search (in this input table)
  );
```
This definition essentially means that the new column will reference elements of its type table which have the same `name` as this table `group` column.

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
The main benefit of having link columns is that they are evaluated once but can be then used in many other column definitions for *direct* access to elements of another table without searching or joining records. Link columns can be also used in *column paths* for concatenating several column access operations (dot notation). For example, now we get *directly* access columns of "My Table" from "Facts":
```java
ColumnPath path = new ColumnPath(Arrays.asList(link,length));
value = path.getValue(1);
value = path.getValue(2);
```
Many column defintion methods accept column paths as parameters.

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
  new Column[] {measure} // Measure
  link // Grouping column
  );

sums.eval();
value = sums.getValue(1); // 3 (1+2)
value = sums.getValue(2); // 3
```

Accumulate functions have also other definition options, for example, specifying how the column is initialized and how it is finalized.

## Schema evaluation and dependencies

It is not necessary to evaluate a column immediately after it has been defined. It is possible to define all or some of the columns and then evalute all of them. Bistro manages all dependencies and it will automatically (recursively) evaluate all columns this column depends on (if necessary). If a column data has been modified or its definition has changed then it will also influence all columns that depend on it directly or indirectly. The easiest way is to evaluate the whole schema:
```java
schema.eval();
```
This method will evaluate all columns of the schema but only if it is necessary and possible (if there are no errors in their definitions).

# How to build

Command line: 
* Build the project: gradlew build
* Publish the artifact: gradlew publish

# TODO

## General

* Configure project (gradle) to publish in maven repo
  * Signing and is it really important
  * How to specify release or snapshot and is it really important
  * Describe in "how to use/run" the usage of these artifacts in maven/gradle configurations

* Examples project. 
  * Example1 is what is in readme
  * Use simple CSV reader/writer to load more interesting data
  * Maybe define util methods for populating table schema from CSV header

## Import/export columns

* Define import/export column classes for reading/writing CSV
  * Import column, when configured and evaluated, adds records to its output table. 
  * Export column when configured and evaluated, writes the input records to the output file.
  * We need conception for schema evaluation and propagation. It answers quesiton how schema elements are created/deleted/updated, that is, how these collections are populated/updated.
  * What does it mean to be dirty for import/export columns? 
  * Once evaluated (imported), the import column is marked as clean and will never be evaluated again. To re-evaluate it, it has to be manually marked as dirty (in future we might introduce a mechanism for doing it regularly according to some strategy but this should be done externally by the corresponding mechanism).
  * How the system distinguishes between normal columns and import/export columns?
    * Special interface - then it would be good to introduce some methods important for it
    * Properties of input/output tables. For example, if input Table is not a normal table then it is an import column.
      * How to distinguish external tables? Special interface or property (similar to being primitive or super or key).

## Problems

* Currently, link dependencies include also output table columns (lhs columns). If we link to them then they have to be computed (is it really so - they might be USER column). If we append then we do not care - we will append anyway.
  * For example, can we link to derived (calculated) columns in the output table? Does it make sense?

## Error handling

* Error handling in calc-link-accu. 
  * Exceptions or error state? When translation/structure errors are reported and how they are supposed to be checked by the user?
  * We need user oriented view on error handling.
  * Introduce last resort exception catch in the case of null-pointer or whatever system-level problems in user code. Surround user code with exceptions.
  * Check validity of parameters whenever possible and document what is expected, for example, empty list/array or null.

## Utilities

* Load schema from CSV string/file
* Load data from CSV string/file
* Load schema/data from JSON collection/object

## API

* Maybe in addition to column parameters, introduce column name parameters (so that we do not need to resolve them)
  * calc(lambda, String[]) - use a list of column names (also list of NamePaths)
  * link(String[], String[])
  * link(String[], NamePath[])
  * link(Map<Column, Column/ColumnPath>)
  * link(Map<String, String/NamePath>)

* API simplification conception. We always work in the context of some schema/graph. 
  * Maybe use 'with' conception
  * The idea is that the current schema should not be specified for each operation
  * with graph.as_default():

* Take into account that some languages allow for argument names while other parameters are optional.
  * introduce/provide method argument name conventions for typical parameters like columns, tables, name etc.

* Should we use run() instead of eval()? Check with other frameworks.

## UDE

* Introduce special Expression types:
  * constant value UDE, for example, for initializers like 0.0 
  * Column/path UDE which return the value of the specified path/column

* Conception: 
  * it would be useful to have a possibility to combine operations defined by standard expressions to build more complex expressions.
  * For example, define Expr1 and Expr2 and then combine them somehow, e.g., using Sum (either expression or lambda)

## Table definition/population: filters, products, super, keys, append flag

* population = adding ids. In more general case, adding (some) combinations of keys, hence we need a procedure which builds these combinations and decides which of them to add.

* Formally define the role of key columns:
  * with respect to their input tables or with respect to the output tables (which provide data moved to the input table)
  * can a KEY column be a DERIVED column (if not then we did they get their data from) or only USER columns can be marked as KEYs?
  * is table id a key column if no user keys are defined? if yes, this means that key column(s) always exist (but maybe lose their purpose/uses in the case of table ids)

* Do we need to explicitly distinguish between USER tables and DERIVED tables (with definition)? 
  * If not, how do we determine if this table has to be populated?
  * Is APPEND flag of a table equivalent to DERIVED column?
  * We have two population mechanisms: APPEND from incoming column, and PRODUCT from outgoing key columns. Can they co-exist and if yes, what is the rule for choosing one of them (explicit or implicit)

* Under what conditions we decide that the table is a product?
  * Is it important and what if the table has keys and also incoming appending columns?
  * How appending incoming columns and product operation are related?
  * Do we need a special table flag for appending and is it an indication of a DERIVED (externally populated) column

* Introduce filter lambda/expression for each table. Is it really valid to have it for any table, that is, will it be valid/meaningful for any table or only for derived tables or only for products? Is it always necessary to have key columns (if yes, can we view id as the only key column)?
