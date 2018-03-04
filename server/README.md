# Bistro Stream: a light-weight stream analytics engine

# What is Bistro Stream

Bistro Stream is a light-weight stream analytics engine which changes the way stream data is processed.
Different to other streaming engines, Bistro Stream defines its data processing logic using a column-oriented approach rather than a set-oriented approach.
In particular, it does not use such difficult to comprehend and execute operations like join and group-by.
As a light-weight engine, it is intended for IoT.

## How to use

### Artifacts

Group: `org.conceptoriented`
Artifact: `bistro-server`
Version: `0.6.0`

### Maven configuration

### Gradle configuration

# How to build

Command line: 
* Build the project: gradlew build
* Publish the artifact: gradlew publish
