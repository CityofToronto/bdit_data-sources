# Data Utils
*Command line python utility to index, partition, aggregate, or move large datasets (Inrix, HERE) within a PostgreSQL database with partitioned tables*

Since these datasets are so large, it is partitioned by month in the database, and operations should be batched so that these workloads can be scheduled for off-peak hours on the database server. While it would have been possible to write these loops in PostgreSQL, because of the way [transaction isolation occurs in PostgreSQL](http://dba.stackexchange.com/a/145785/101712), **no change would be committed until the loop finishes**. So it is therefore preferable to call each operation from an external script, in this case Python.

**Note:** This utility was initially created for Inrix data, it is being migrated to be more generalizable to looping over lengthy PostgreSQL maintenance tasks for big datasets.

## Usage
Download the contents of this folder. Test with `python -m unittest`. This project assumes Python 3.5 and above and has not been tested in Python 2.x. The only external dependency is psycopg2, a database connection package for PostgreSQL ([installation instructions](http://initd.org/psycopg/docs/install.html#install-from-a-package)). The following sql functions are necessary for the functionality to work: 
- [SQL aggregation functions](../../sql/aggregation/agg_extract_hour_functions.sql)
- [Copying data to a new schema](../../sql/move_data.sql) from TMCs that are not in the `gis.inrix_tmc_tor` table 
- [Removing the moved data](../../sql/remove_outside_data.sql)
- [Creating indexes](../../sql/raw_indexes/)

To run from the command line see the help text below. So for example `inrix_util -i -y 201207 201212` would index the tables from July to December 2012 inclusive with an index on the `tmc`, `tx`, and `score` columns respectively. Descriptions of the four main commands follow.
```shell
usage: inrix_util.py [-h] (-i | -p | -a | -m)
                     (-y YYYYMM YYYYMM | -Y YEARSJSON) [-d DBSETTING]
                     [-t TABLENAME] [-tx TIMECOLUMN] [--alldata]
                     [--tmctable TMCTABLE] [--indextmc] [--indextx]
                     [--indexscore]

Index, partition, or aggregate Inrix traffic data in a database.

optional arguments:
  -h, --help            show this help message and exit
  -i, --index           Index the tables specified by the years argument
  -p, --partition       Add Check Constraints to specified tablesto complete
                        table partitioning
  -a, --aggregate       Aggregate raw data
  -m, --movedata        Remove data from TMCs outside Toronto
  -y YYYYMM YYYYMM, --years YYYYMM YYYYMM
                        Range of months (YYYYMM) to operate overfrom startdate
                        to enddate
  -Y YEARSJSON, --yearsjson YEARSJSON
                        Written dictionary which contains years as keyand
                        start/end month like {'2012'=[1,12]}
  -d DBSETTING, --dbsetting DBSETTING
                        Filename with connection settings to the
                        database(default: opens default.cfg)
  -t TABLENAME, --tablename TABLENAME
                        Base table on which to perform operation of form
                        inrix.raw_data
  -tx TIMECOLUMN, --timecolumn TIMECOLUMN
                        Time column for partitioning, default: tx
  --alldata             For aggregating, specify using all rows, regardless of
                        score
  --tmctable TMCTABLE   Specify subset of tmcs to use, default:
                        gis.inrix_tmc_tor
  --indextmc            Index based on tmc, if no index option specified,
                        default is to index tmc, time, and score
  --indextx             Index based on timestamp, if no index option
                        specified, default is to index tmc, time, and score
  --indexscore          Index based on score, if no index option specified,
                        default is to index tmc, time, and score
```

The yearsjson parameter is [an open issue](https://github.com/CityofToronto/bdit_data-sources/issues/1).

Your `db.cfg` file should look like this:
```python
[DBSETTINGS]
database=bigdata
host=localhost
user=user
password=password
```

### Index
Index the specified months of disaggregate data. If none of `[--indextmc] [--indextx] [--indexscore]` are specified, defaults to creating an index for each of those columns. 

### Partition
It is faster to add the [`CHECK CONSTRAINTS`](https://www.postgresql.org/docs/current/static/ddl-partitioning.html#DDL-PARTITIONING-IMPLEMENTATION) which fully enable table partitioning after all data has been loaded. This function adds these constraints to the specified tables. Since the Inrix data are partitioned by month, the constraint is on the timestamp column (default `tx`).

### Aggregate
Calls [SQL aggregation functions](../../sql/aggregation/agg_extract_hour_functions.sql) to aggregate the disaggregate data into 15-minute bins for other analytic applications.

`--tmctable`
Specify the table of TMCs to keep in the aggregate table

`--alldata`
The default is to only use `score = 30` records (observed data). If this flag is used all available records are used.

### Move Data
Removes data from TMCs outside the City of Toronto in two stages by calling two `sql` functions:
1. [Copying data to a new schema](../../sql/move_data.sql) from TMCs that are not in the `gis.inrix_tmc_tor` table 
2. [Removing the moved data](../../sql/remove_outside_data.sql). Instead of running a `DELETE` operation, it is faster to copy the subset of data to retain, `DROP` the table, and then move the remaining data back to the original table. 

This operation requires dropping all constraints and indexes as well.

## Challenges solved

### Queries Failing Gracefully
Because of RAM issues on the server we used it was necessary to restart the PostgreSQL engine nightly to free up RAM. Because we wanted processing to continue after, the util had to accept these failures gracefully. I eventually wrote an execution wrapper based on the validation I found from [asking this question](http://softwareengineering.stackexchange.com/q/334518/251322). 

You can find that code [here](https://github.com/CityofToronto/bdit_data-sources/blob/423b5534b0de6f87d7d436b710aeb4840b37a4e5/inrix/python/inrix_utils/utils.py#L54-L63). Note that you should consider this very carefully for your own system, since the connection regularly failing isn't normal and infinite retrying queries due to dropped connections could be a Big Problem.

### Argument parsing

The advantages of making this a command line application are: 
- easier to run with different options without having to open an interactive python shell or editing a script directly
- scheduling tasks with `cron` for Un*x or `task scheduler` on Windows

Two handy resources for writing the [argument parser](https://github.com/CityofToronto/bdit_data-sources/blob/423b5534b0de6f87d7d436b710aeb4840b37a4e5/inrix/python/inrix_utils/inrix_util.py#L109) were the [argparse API documentation](https://docs.python.org/3/library/argparse.html) and [this tutorial](https://docs.python.org/3/howto/argparse.html). For testing (see below) it was helpful to migrate this a function `parse_args()` which returns the parsed arguments as a dictionary.

### Unit testing

Unit testing code is awesome and [totally worth the effort](http://stackoverflow.com/a/67500/4047679).

Think of it like exploring whether your code works or not in the interactive shell, but instead you have a history of all of the tests you attempt. If you add or modify functionality, it only takes a second to see if the functions still work the way you initially intended them to.
It also forces you to break code up into functions that are easily testable, which makes code generally more readable and understandable. It improves the portability of the code, so that functions can be easily extended and then re-tested. I reused the argument parsing component into automating congestion mapping. 

I was inspired by [this answer](http://stackoverflow.com/a/24266885/4047679) to use the similar directory structure and use the `unittest` module to structure my tests. Unittest documentation is [here](https://docs.python.org/3.5/library/unittest.html#).

## Next Steps

1. Pull out parts that could be reusable to add to the possible `bdit` Python module, such as `YYYYMM` parsing and `db.cfg` command line arguments
2. Reorganize folders so that the sql functions used in this utility are in the same folder (to make it easier to download everything necessary for this utility.) and maybe raise an error to create them if they aren't present when `inrix_util` is run