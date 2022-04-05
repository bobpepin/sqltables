import sqlite3
import logging
from . import generic


logger = logging.getLogger(__name__)


class SQLiteSchemaMapping (generic.SQLObjectMapping):
    def __init__(self, db, query):
        self.query = query
        self.schema = db.open_table("sqlite_master").view(query)
        self.db = db
        
    def __contains__(self, key):
        return bool(self.schema.table("select name from _ where name = ?", parameters=[key]))
    
    def __len__(self):
        [[count]] = self.schema.view("select count(*) from _")
        return count
    
    def __iter__(self):
        return (r.name for r in self.schema.view("select * from _ order by name"))

    def __getitem__(self, key):
        if key in self:
            return self.db.open_table(key)
        else:
            raise KeyError


class Database (generic.Database):
    """Connection to a SQLite database.

    Args:
        name: Name of the database, passed to :py:func:`sqlite3.connect`. The default value "" creates a new in-memory database.
"""
    
    def __init__(self, name=""):
        conn = sqlite3.connect(name, check_same_thread=False)
        super().__init__(conn)
        self.name = name
        self.default_column_type=""
        self.value_placeholder = "?"
        self.temporary_prefix = "temp.temp_"
        self.tables = SQLiteSchemaMapping(self, "select * from _ where type = 'table'")
        self.views = SQLiteSchemaMapping(self, "select * from _ where type = 'view'")
    
    def _drop(self, statement):
        logger.debug(f"[{self!r}] Scheduling drop {statement!r}")        
        self._gc_statements.append(statement)
        self._garbage_collect()        
        
    def drop_table(self, table_name):
        """Drop a table from the database. To work around locking issues, the table is first 
        renamed and then dropped once all active iterators on the connection have ended.
        
        Args:
            table_name: The name of the table to drop.
        """        
        quoted_table_name = self.quote_name(table_name)
        temp_name = self._generate_temp_name(prefix="_drop_")
        with self._transaction():
            self.execute(f"alter table {quoted_table_name} rename to {temp_name}")
        self._drop(f"drop table {temp_name}")        
        
    def create_function(self, name, nargs, fn):
        """Register a SQLite user-defined function
        
        Args:
            name (str): name of the function in SQLite
            nargs (int): number of arguments
            fn (callable): Python function object
        """
        return self._conn.create_function(name, nargs, fn)
        