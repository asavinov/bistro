```
  ____  _     _
 | __ )(_)___| |_ _ __ ___  ___________________________
 |  _ \| / __| __| '__/ _ \ 
 | |_) | \__ \ |_| | | (_) |  C O L U M N S  F I R S T
 |____/|_|___/\__|_|  \___/ ___________________________
```

> General information about Bistro including what is Bistro, how it works, its formal basis and why Bistro should be used can be found in the description of the root project: [../README.md](../README.md)

* [Getting started with Bistro Streams](#getting-started-with-bistro-streams)


***TODO*** Mention examples. Either centrally (in more info or references) or in relevant places discussing the topic.

# Getting started with Bistro Streams

## Createing server


Principles. Bistro Engine <- Bistro Server <- (Actions) <- Bistro Connectors



## Createing connectors


## Createing actions


## Running a server


# How to build

From the project folder (`git/bistro/server`) execute the following to clean, build and publish the artifact:

```console
$ gradlew clean
$ gradlew build
$ gradlew publish
```

The artifact will be placed in your local repository from where it will be available in other projects.

In order to include this artifact into your project add the following lines to dependencies of your `build.gradle`:

```groovy
dependencies {
    compile("org.conceptoriented:bistro-core:0.7.0-SNAPSHOT")
    compile("org.conceptoriented:bistro-formula:0.7.0-SNAPSHOT")
    compile("org.conceptoriented:bistro-server:0.7.0-SNAPSHOT")

    // Other dependencies
}
```
