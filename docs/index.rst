Welcome to SQLTables's documentation!
=====================================

.. toctree::
   :maxdepth: 2
   :caption: Contents:

Motivation
-----------

Relational data schemas are a proven way of organizing data, that fit the majority of query and processing use cases. 

SQL is a powerful query language, that generally maintains high readability and is familiar to most people working with data. 

SQLite is a powerful relational database, part of the Python standard library, that supports both in-memory and out-of-memory processing.

The goal of this module is to provide a high performance relational data structure that integrates seamlessly with Python's features for structuring programs, such as control flow constructs, functions or classes.

Source: `https://github.com/bobpepin/sqltables <https://github.com/bobpepin/sqltables>`_

Main Concepts and Example
--------------------------

The main objects are tables, represented by the `Table` class and associated with a `Database`. 
New tables are created with the `load_values` method on the Database object.
Tables are queried with the `view` and `table` methods, which execute an SQL query and return a new `Table` object backed by a temporary view or table. 
Within SQL queries, the special name `_` refers to the table associated with `self`.


See the `examples/` folder in the source repository for usage examples.


API Reference
-----------------

.. py:module:: sqltables
.. autoclass:: Database
   :members:

.. autoclass:: Table
   :members:
   :special-members: __iter__

.. autoclass:: RowIterator
   :members:
   :special-members: __iter__

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
