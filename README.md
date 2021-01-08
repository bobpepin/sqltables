# SQLTables
SQLTables is a Python module that provides access to SQLite tables as first-class objects.

This means that tables and views can be assigned to variables and used as parameters and return values of Python functions.

Documentation: https://sqltables.readthedocs.io/

Examples are in the `examples/` folder. The [Machine Learning](https://github.com/bobpepin/sqltables/blob/master/examples/Machine%20Learning.ipynb) example notebook should illustrate most features.

## Motivation

Relational data schemas are a proven way of organizing data, that fit the majority of query and processing use cases. 

SQL is a powerful query language, that generally maintains high readability and is familiar to most people working with data. 

SQLite is a powerful relational database, part of the Python standard library, that supports both in-memory and out-of-memory processing.

The goal of this module is to provide a high performance relational data structure that integrates seamlessly with Python's features for structuring programs, such as control flow constructs, functions or classes.

## Main Concepts and Example

The main objects are tables, represented by the `Table` class and associated with a `Database`. 
New tables are created with the `load_values` method on the Database object.
Tables are queried with the `view` and `table` methods, which execute an SQL query and return a new `Table` object backed by a temporary view or table. 
Within SQL queries, the special name `_` refers to the table associated with `self`.

A simple example:
```python
db = sqltables.Database()
rows = [["a", 1], ["b", 2], ["c", 3]]
values = db.load_values(rows, column_names=["name", "val"])
values
```
|name|val|
|-|-|
|\'a\'|1|
|\'b\'|2|
|\'c\'|3|
```python
def square(tab):
    return tab.view("select name, val, val*val as squared from _")

square(values)
```
|name|val|squared|
|-|-|-|
|\'a\'|1|1|
|\'b\'|2|4|
|\'c\'|3|9|


