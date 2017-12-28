
<!-- README.md is generated from README.Rmd. Please edit that file -->
dbcompare
=========

The goal of dbcompare is to compare tables accross sql server instances. It provides tools to identify table dependencies for a given stored procedure; evaluate data discrepancies between source server and target server; It also provide a solution to manage reference data or lookup data across servers

Installation
------------

You can install dbcompare from github with:

``` r
# install.packages("devtools")
devtools::install_github("JW3148/dbcompare")
```

Example
-------

This is a basic example which shows you how to compare tables used by a stored procedure on two servers:

``` r
## basic example code
udf_CompareSourceTarget(source_server, target_server, db, proc)
```
