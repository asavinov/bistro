# Bistro: calculate-accumulate-link 

Bistro is a Java library for column-oriented data processing.
Its unique distribuishing feature is that data processing is described as operations with columns 
and can be characterized as Calculate-Accumulate-Link (CAL) because of the three column operations.   
Formally, this means that data transformations are represented as a graph of operations with functions 
rather then a graph of operations with sets in other formalisms. It is also opposed to the map-reduce 
data processing paradigm because instead of the map and reduce set operations it uses three 
column operations: calculate, accumulate and link. The formal basis for the Bistro column-oriented 
data processing engine is a novel data model called the concept-oriented model.
As a general purpose engine, Bistro can be applied to many problems like data integration, 
data migration, extract-transform-load (ETL), big data processing, stream analytics, big data processing.

# Build

Command line: 
* Build the project: gradlew build
* Publish the artifact: gradlew publish

# Change Log

* v0.1.0 (2017-09-03) - Initial commit
