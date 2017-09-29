# Bistro-formula: Calculate∙Link∙Accumulate

# What is Bistro-formula

Bistro-formula is a library for defining columns using *formulas* in some expression language rather than the native programming langauge. 

Bistro-core allows for definining columns using native functions (lambda) written in the same programming language (Java) as well as native object references (like column references). For many tasks it is easier to describe how data has to be processed using expressions, called *formulas*, written in some language. Bistro-formula is intended for defining columns using syntactic expressions rather than native functions. Such expressions can be written using different syntactic conventions and expressions languages depending on the purpose. The main benefit of using this approach is that complex computations can be written much easier, particularly, because they are written using column names and arithmetic operations, which make them very similar to how cells in spreadsheets are defined. For example, a calcuate column could be defined using the expression `[Column A] + [Column B]` which returns the sum of two columns "Column A" and "Column B".

# Build

Command line: 
* Build the project: gradlew build
* Publish the artifact: gradlew publish

# TODO

* Translation of formulas in calc-link-accu have to use class name to instantiate UDE rather than use selector.
  * In addition, these UDE classes have to implement either constructor with formula or setters for formulas or translate method.

* Evalex UDE is not always impelmented and not tested but only a placeholder exists.

* JavaScript UDE
