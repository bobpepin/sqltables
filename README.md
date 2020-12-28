# sqltables
SQLite tables as first-class objects in Python.

This Python module provides a first-class interface to SQLite table. This means that tables and views can be assigned to variables and used as parameters and return values of Python functions.

A simple example:
```python
db = sqltables.Database()
values = db.load_values([[1], [2], [3]], column_names=["val"])

def square(tab):
    return tab.view("select val, val*val as squared from _")

squared = square(values)
```

