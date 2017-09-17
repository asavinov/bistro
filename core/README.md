# Bistro: Calculate∙Link∙Accumulate

# Build

Command line: 
* Build the project: gradlew build
* Publish the artifact: gradlew publish

# Change Log

* v0.2.0 (2017-09-17) - Major refactoring and cleaning with new API
* v0.1.0 (2017-09-03) - Initial commit

# TODO

## General

* Core - make name comparison case-sensitive (like TF, Pandas etc.) Other libs/apps can use their own rules.
* Formula API and implementation in a separate package. Later move it to a separate project with its own deps (Antlr, string utils for comparison etc.)
* Maven public artifact

* What to do with two types of paths in UDE: objects and names? Can we use only one?

* Error handling in calc-link-accu. Simultaniously, simplify error handling in UDE classes, maybe remove two types of errors.
  * Exceptions or error state? When translation/structure errors are reported and how they are supposed to be checked by the user?
  * We need user oriented view on error handling.
  * Introduce last resort exception catch in the case of null-pointer or whatever system-level problems in user code. Surround user code with exceptions.
  * Check validity of parameters whenever possible and document what is expected, for example, empty list/array or null.

* Arrays instead of lists in high level public methods. They are easier to define and can be declared as variable arguments.

## UDE
* So we need to distinguish these clases depending on how the main procedure is given (they can be subclasses of some basic UDE):
  * Normal UDE: evaluator is implemented by the class method. 
    * Parameterization via path setters (strings or objects).
  * Lambda UDE: evaluator is provided as a function with certain signature.
    * Lambda parameter
    * Parameterization via path setters (strings or objects).
  * Formula UDE: evaluator provided syntactically and will be translated/resolved relative to the provided table object.
    * Formula parameter for procedure (and dependencies) 
    * Table object this formulas belongs to for resolution of dependencies

* Simply UDE interface by leaving one error type etc.
* Simply UDE interface by removing translate and evaluate. Only evaluate. Translation is done etiher in construtor or when formula is set. Think about setting formula only in a setter (not in constructor).
* UDE currently has two versions of parameter paths: names and objects. Do we really need both of them?

* Translation of formulas in calc-link-accu have to use class name to instantiate UDE rather than use selector.
  * In addition, these UDE classes have to implement either constructor with formula or setters for formulas or translate method.

* Evalex UDE is not always impelmented and not tested but only a placeholder exists.

* Introduce constant UDE, for example, for initializers like 0.0 and EQUAL UDE for copying field/path values to output without any expression.
  * Use case: 1) initializer, determine manaully 2) Within UdeExp4j or other expressions which determine this internally and want simplify computations by avoiding their own expression and reusing simpler way, for example, labmda or another definitionType of constant return.

* JavaScript UDE

## Import/export columns

* Define import/export column classes for reading/writing CSV
  * Import column, when evaluated, adds records to its output table. 
  * Export column when evaluated, writes the input records to the output file.
  * We need conception for schema evaluation and propagation. It answers quesiton how schema elements are created/deleted/updated, that is, how these collections are populated/updated.
  * What does it mean to be dirty for import/export columns? 

## Utilities

* Load schema from CSV string/file
* Load data from CSV string/file
* Load schema/data from JSON collection/object

## Problems
* Currently, link dependencies include also output table columns (lhs columns). If we link to them then they have to be computed (is it really so - they might be USER column). If we append then we do not care - we will append anyway.
  * For example, can we link to derived (calculated) columns in the output table? Does it make sense?
