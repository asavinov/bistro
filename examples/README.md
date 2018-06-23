```
  ____  _     _
 | __ )(_)___| |_ _ __ ___  ___________________________
 |  _ \| / __| __| '__/ _ \ 
 | |_) | \__ \ |_| | | (_) |  C O L U M N S  F I R S T
 |____/|_|___/\__|_|  \___/ ___________________________
```
[![Gitter chat](https://badges.gitter.im/gitterHQ/gitter.png)](https://gitter.im/conceptoriented/Lobby)

# Bistro examples

This sub-project provides basic examples of using Bistro with sample data. More details can be found in the source files.

## Bistro Engine

| Name | Data set | Operations | Description
--- | --- | --- | ---
[Example1.java](https://github.com/asavinov/bistro/blob/master/examples/src/main/java/org/conceptoriented/bistro/examples/core/Example1.java) | * | calculate, link, accumulate | Basic operations
[Example2.java](https://github.com/asavinov/bistro/blob/master/examples/src/main/java/org/conceptoriented/bistro/examples/core/Example2.java) | ds1: OrderItems.csv, Products.csv | calculate, link, accumulate | Compute sales for each product
[Example3.java](https://github.com/asavinov/bistro/blob/master/examples/src/main/java/org/conceptoriented/bistro/examples/core/Example3.java) | ds1: OrderItems.csv | product, calculate, project, accumulate | Compute sales for each order
[Example4.java](https://github.com/asavinov/bistro/blob/master/examples/src/main/java/org/conceptoriented/bistro/examples/core/Example4.java) | ds1: OrderItems.csv, Products.csv | product, calculate, link, project, accumulate | Compute sales for each product category
[Example5.java](https://github.com/asavinov/bistro/blob/master/examples/src/main/java/org/conceptoriented/bistro/examples/core/Example5.java) | ds3: BTC-EUR.csv | calculate, roll | Moving average (also smoothed) of daily bitcoin prices
[Example6.java](https://github.com/asavinov/bistro/blob/master/examples/src/main/java/org/conceptoriented/bistro/examples/core/Example6.java) | ds4: .krakenEUR.csv | calculate, roll | Compute volume weighted average price of bitcoin
[Example7.java](https://github.com/asavinov/bistro/blob/master/examples/src/main/java/org/conceptoriented/bistro/examples/core/Example7.java) | ds4: .krakenEUR.csv | calculate, range, project, accumulate | Compute hourly bitcoin price

## Bistro Streams

| Name | Data set | Operations | Description
--- | --- | --- | ---
[Example1.java](https://github.com/asavinov/bistro/blob/master/examples/src/main/java/org/conceptoriented/bistro/examples/server/Example1.java) | simulated | | Basic operations
[Example2.java](https://github.com/asavinov/bistro/blob/master/examples/src/main/java/org/conceptoriented/bistro/examples/server/Example2.java) | ds4: .krakenEUR.csv | roll | Find peaks in moving aggregation with sliding windows
[Example3.java](https://github.com/asavinov/bistro/blob/master/examples/src/main/java/org/conceptoriented/bistro/examples/server/Example3.java) | ds4: .krakenEUR.csv | range, project, accumulate, calculate | Find peaks in moving aggregation using tumbling windows
[Example4.java](https://github.com/asavinov/bistro/blob/master/examples/src/main/java/org/conceptoriented/bistro/examples/server/Example4.java) | simulated | project, link, accumulate | Find number of clicks per region for last period
[Example5.java](https://github.com/asavinov/bistro/blob/master/examples/src/main/java/org/conceptoriented/bistro/examples/server/Example5.java) | simulated | range, project, accumulate, calculate | Find anomalous sensor values
