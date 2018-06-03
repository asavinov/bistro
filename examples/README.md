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
Example1 | * | calculate, link, accumulate | Basic operations
Example2 | ds1: OrderItems.csv, Products.csv | calculate, link, accumulate | Compute sales for each product
Example3 | ds1: OrderItems.csv | product, calculate, project, accumulate | Compute sales for each order
Example4 | ds1: OrderItems.csv, Products.csv | product, calculate, link, project, accumulate | Compute sales for each product category
Example5 | ds3: BTC-EUR.csv | calculate, roll | Moving average (also smoothed) of daily bitcoin prices
Example6 | ds4: .krakenEUR.csv | calculate, roll | Compute volume weighted average price of bitcoin
Example7 | ds4: .krakenEUR.csv | calculate, range, project, accumulate | Compute hourly bitcoin price

## Bistro Streams

| Name | Data set | Operations | Description
--- | --- | --- | ---
Example1 | simulated | | Basic operations
Example2 | ds4: .krakenEUR.csv | roll | Find peaks in moving aggregation with sliding windows
Example3 | ds4: .krakenEUR.csv | range, project, accumulate, calculate | Find peaks in moving aggregation using tumbling windows
Example4 | simulated | project, link, accumulate | Find number of clicks per region for last period
Example5 | simulated | range, project, accumulate, calculate | Find anomalous sensor values
