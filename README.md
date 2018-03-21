# parquet-968
Test suite to validate the backward compatibility of PARQUET-968

## Old Schema style (parquet 1.9.0 and older, prior to PARQUET-968)

```
+---------+------+-------------+----------------+--------+----------------+
|intNotSet|intSet|emptyRepeated|nonEmptyRepeated|emptyMap|     nonEmptyMap|
+---------+------+-------------+----------------+--------+----------------+
|     null|     1|           []|          [1, 1]|      []|[[1, 1], [2, 2]]|
+---------+------+-------------+----------------+--------+----------------+
```

## New schema style (specs compliant)

```
+---------+------+-------------+----------------+--------+----------------+
|intNotSet|intSet|emptyRepeated|nonEmptyRepeated|emptyMap|     nonEmptyMap|
+---------+------+-------------+----------------+--------+----------------+
|     null|     1|         null|          [1, 1]|    null|[1 -> 1, 2 -> 2]|
+---------+------+-------------+----------------+--------+----------------+
```
Notice the following differences:
- empty repeated fields (primitive and messages) are now view as null (upper level LIST wrapper is optional in parquet and not set)
- Maps are now correctly interpreted as such, they were previously written as repetead tuples of key-value

## Test suite with Spark

check-out `org.bhnte.parquet968.SparkTest` for a test suite reading parquet-protobuf files written with and without the new flag and read with Spark
