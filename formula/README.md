# Bistro-formula: Calculate∙Link∙Accumulate

# What is Bistro Formula

Bistro-formula is a library for defining columns using *formulas* in some expression language rather than the native programming langauge. 

Bistro Engine allows for definining columns programmatidally by using native functions (lambdas) written in the same programming language (Java) as well as native object references (like column references). For many tasks it is easier to describe how data has to be processed using expressions, called *formulas*, written in some language. 

Bistro Formula is intended for defining columns using syntactic expressions rather than native functions. Such expressions can be written using different syntactic conventions and expressions languages depending on the purpose. The main benefit of using this approach is that complex computations can be written much easier, particularly, because they are written using column names and arithmetic operations, which make them very similar to how cells in spreadsheets are defined. 

# How it works

Bistro Engine defines `Evaluator` and `Expression` interfaces which have to be implemented by user-defined functions (UDFs). These UDFs are passed as parameters to column definitions and do real data processing according to the logic of this column kind (calc, link, accu etc.) For example, a typical UDF could be defined as follows:
```java
calc.calc(
        p -> ((Double)p[0]) + ((Double)p[1]), // return columnA + columnB;
        columnA, columnB
);
```

Yet, writing such functions could be a relatively difficult task in many cases. Therefore, Bistro Formula introduces a possibility to do the same by writing expressions. For example, instead of implementing the above UDF, we could write an equivalent expression as a formula:
```
[Column A] + [Column B]
```
The translator will then parse this formula and generate the necessary function which will compute the sum of two input values.

Bistro Formula defines an interface `Formula` which has to be impelmented by any new expression language. This interface inherits `Expression` and hence it can be passed to all column definitions. When a `Formula` is instantiated, it gets concrete formula and a table this formula is defined for. This formula object knows how to parse this formula and how to compute it.
```java
Formula expr = new FormulaExp4j("[Column A] + [Column B]", schema.getTable("My Table"));
columnC.calc(expr);
columnC.eval();
```
Here we define an expression (UDF) by providing a formula rather than a lambda. This expression is then used in the calc-column definition. 

# Build

Command line: 
* Build the project: gradlew build
* Publish the artifact: gradlew publish

# TODO

* Translation of formulas in calc-link-accu have to use class name to instantiate UDE rather than use selector.
  * In addition, these UDE classes have to implement either constructor with formula or setters for formulas or translate method.
* Evalex UDE is not always impelmented and not tested but only a placeholder exists.
* JavaScript UDE
* Add support for http://mathparser.org/
