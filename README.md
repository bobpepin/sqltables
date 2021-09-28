# SQLTables
SQLTables is a Python module that provides access to SQLite and PostgreSQL tables as first-class objects.

This means that tables and views can be assigned to variables and used as parameters and return values of Python functions.

Documentation: https://sqltables.readthedocs.io/

Examples are in the `examples/` folder. The [Machine Learning](https://github.com/bobpepin/sqltables/blob/master/examples/Machine%20Learning.ipynb) example notebook should illustrate most features.

## Main Concepts and Example

The main objects are tables, represented by the `Table` class and associated with a `Database`. 
New tables are created with the `create_table` method on the Database object.
Tables are queried with the `view` and `table` methods, which execute an SQL query and return a new `Table` object backed by a temporary view or table. 
Within SQL queries, the special name `_` refers to the table associated with `self`.

A simple example:
```python
db = sqltables.Database()
rows = [["a", 1], ["b", 2], ["c", 3]]
values = db.create_table(rows=rows, column_names=["name", "val"])
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


