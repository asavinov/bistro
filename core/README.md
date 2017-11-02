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

Tables are created within the schema by providing a unique name:
```java
Table things = schema.createTable("THINGS");
Table events = schema.createTable("EVENTS");
```

A table in the concept-oriented model is a a mathematical set, that is, a number of (unique) values. In Bistro, all user-defined tables are sets of primitive values the structure of which cannot be changed. These values are of long type and are interpreted as row identifiers without any additional semantics. 

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

## No definition columns

Data in Bistro is stored in columns. Formally, a column is a function and hence it defines a mathematical *mapping* from all table inputs to the values in the output table. Input and output tables of a column are specified during creation: 
```java
Column thingName = schema.createColumn("Name", things, objects);
```
This column defines a mapping from "THINGS" to the "Object" (primitive) table.

A new column does not have a definition and hence it cannot derive its output values. The only way to define their mapping from outputs for inputs is to explicitly set the outputs using API:
```java
thingName.setValue(0, "fridge");
thingName.setValue(1, "oven");
Object value = name.getValue(1);
```

## Calculate columns

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

## Link columns

The second column type is a *link* column. Link columns are typed by user (not primitive) tables and their output essentially is a reference to some element in the output table:

> For each intput, a link column *finds* its output in the output table by providing equality criteria for the output elemetns. These values for these criteria are computed from the columns in this table using this input similar to calculate columns.

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

## Accumulate columns

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
Here the `link` column maps elements of the "EVENTS" table to elements of the "THINGS" table, and hence an element of "THINGS" (where we define the accumulate column) is a group of all elements of "EVENTS" which reference it via this column. For each element of the "EVENTS", the specified accumulate function will be called and its result stored in the column output. Thus the accumulate function will be called as many times for each input of "THINGS", as its has facts that map to it.

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

## Column paths

Link columns can be also used in *column paths* for concatenating several column access operations (dot notation). For example, now we can *directly* access device name length from any event:
```java
ColumnPath path = new ColumnPath( Arrays.asList(link,calc) );
value = path.getValue(0);
value = path.getValue(1);
value = path.getValue(2);
```
Many column defintion methods accept column paths as parameters.

## More complex accumulations

Let us assume now that the "EVENTS" table has a propery "Measure" we want to numerically aggregate (instead of simply counting):
```java
Column measure = schema.createColumn("Measure", things, objects);
measure.setValue(0, 1.0);
measure.setValue(1, 2.0);
measure.setValue(2, 3.0);
```

We can find the sum of the measure for each element in the "Table" using this accumulate column:
```java
Column sums = schema.createColumn("sum measure", table, objects);
sums.accu(
  link, // Grouping column
  p -> (Double)p[0] + (Double)p[1], // Add the measure for each new fact
  measure // Measure
  );

sums.eval();
value = sums.getValue(1); // 3 (1+2)
value = sums.getValue(2); // 3
```

Accumulate functions have also other definition options, for example, specifying how the column is initialized and how it is finalized.





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

* Convenience. Maybe in addition to column parameters, introduce column name parameters (so that we do not need to resolve them)
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
  * can a KEY column be a DERIVED column? If yes, can it derive its output from other key columns of this same table?
  * is table id a key column if no user keys are defined? if yes, this means that key column(s) always exist (but maybe lose their purpose/uses in the case of table ids)

* Do we need to explicitly distinguish between USER tables and DERIVED tables (with definition)? 
  * If not, how do we determine if this table has to be populated?
  * Is APPEND flag of a table equivalent to DERIVED column?
  * We have two population mechanisms: APPEND from incoming column, and PRODUCT from outgoing key columns (and API). Can they co-exist and if yes, what is the rule for choosing one of them (explicit or implicit)

* Under what conditions we decide that a table is a product (that is, we need to populate it via key columns?
  * Particular case: we might well define key columns (for uniqueness) and no PRODUCT, because records will be appended either manually or using incoming columns -> Key columns are NOT an indication of PRODUCT.
  * Particular case: if a table is PRODUCT then key columns must be defined. Yet, not all of them might be actually used, e.g., calc columns have to be skipped. What about link and accu key columns?
  * Is it important and what if the table has keys AND also incoming appending columns?
  * How appending incoming columns and product operation are related?
  * Do we need a special table flag for appending and is it an indication of a DERIVED (externally populated) column

Simplifying approach:
  * key column values can be only set from an incoming column or from product or manually (they do not have their own definition). In other words, key columns do not have their definition (if we call setKey then the definition is deleted/ignored). So a USER column is either key or non-key column. They both are set from outside (API or incoming append, and key columns also by product) but key columns are used for uniqueness checks. Key columns must be declared for these reasons: uniqueness constraint (only for USER columns), product table (automatic with key columns declared and no input link columns). Append from links does not require key columns (but we might impose this constraint, see below).
  * append column has higher priority than product table (so product will be ignored) because it imposes stronger constraints (the number of appended columns will always be less than the number of combinations). Yet, filter will be always taken into account. Hence, a table may have either USER or DERIVED flag. Derive table means records can be appended/populated by automatically from other tables (but generally not from API). USER table will never be auto-populated. Product will be computed only if key columns are defined. Append from input columns can always be performed even wthout key columns.
  * Additional stronger constraint: Link columns require keys to be declared in the type AND are allowed to link to only these keys. Note that link column evaluation can be quite difficult because the target record has to be found in a potentially large table. If we declare key columns then they could be indexed and be non-derivable and non-user (indexing can be optimized). 


* Introduce filter lambda/expression for each table. Is it really valid to have it for any table, that is, will it be valid/meaningful for any table or only for derived tables or only for products? Is it always necessary to have key columns (if yes, can we view id as the only key column)?
