# TODO: Introduce DB object
# Retry drop table if it fails during GC
# Keep track of cursors

import sqlite3
from collections import namedtuple
import re
import logging

logger = logging.getLogger(__name__)

_next_temp_id = 0
def generate_temp_name():
    global _next_temp_id
    name = f"temp.temp_{_next_temp_id}"
    _next_temp_id += 1
    return name

def _query(db, select_stmt, kind="view", parameters=None, bindings={}):
    if re.match(r"\s*with\b", select_stmt):
        raise ValueError("sqltables: with clause not supported, please use bindings instead")
    global next_temp_id
    preamble = []
    with_clauses = [
        f"{name} as (select * from {table.name})"
        for name, table in bindings.items() if table.name is not None
    ]
    with_stmt = "with " + ", ".join(with_clauses) if with_clauses else ""
    result_name = generate_temp_name()
    statement = f"create temporary {kind} {result_name} as {with_stmt} {select_stmt}"
#     print(statement)
    if parameters is not None:
        logger.debug("Executing %s | parameters=%s", repr(statement), repr(parameters))        
        db.execute(statement, parameters)
    else:
        logger.debug("Executing %s", repr(statement))
        db.execute(statement)
    db.commit()
    result = Table(result_name, db)
    result.bindings = bindings
    result.kind = kind
    return result


def from_values(values, *, column_names, db=None, name=None):
    if name is None:
        name = generate_temp_name()
    if db is None:
        db = sqlite3.connect(":memory:")
    column_spec = ",".join(column_names)
    value_spec = ",".join("?" for _ in column_names)
    db.execute(f"create temporary table {name} ({column_spec})")
    db.executemany(f"insert into {name} values ({value_spec})", values)
    db.commit()
    return Table(name=name, db=db)

from_tuples = from_values
create_table_from_iterable = from_values

class Table:
    def __init__(self, name=None, db=None):
        self.name = name
        if db is None:
            db = sqlite3.connect(":memory:")
        self.db = db
        self.bindings = None
        self.kind = None
        
    def view(self, select_stmt, *, bindings={}):
        return _query(self.db, select_stmt, kind="view", bindings=dict(_=self, **bindings))
    
    def table(self, select_stmt=None, parameters=None, *, bindings={}):
        if select_stmt is None:
            select_stmt = "select * from _"
        return _query(self.db, select_stmt, kind="table", parameters=parameters, bindings=dict(_=self, **bindings))
  
    def describe(self):
        if "." in self.name:
            schema, name = self.name.split(".")
            stmt = f"pragma {schema}.table_info({name})"
        else:
            stmt = f"pragma table_info({self.name})"
        cur = self.db.execute(stmt)
        col_names = [x[0] for x in cur.description]
        Row = namedtuple("Row", [x.replace(":", "_") for x in col_names], rename=True)        
        return [Row._make(x) for x in cur]
    
    def __iter__(self):
        cur = self.db.execute(f"select * from {self.name}")
        logger.debug(f"Cursor {cur!r} created")
        def log_del(obj):
            logger.debug(f"Cursor {obj!r} deleted.")
        import weakref
        weakref.finalize(cur, log_del, cur)
        col_names = [x[0] for x in cur.description]
        Row = namedtuple("Row", [x.replace(":", "_") for x in col_names], rename=True)        
        return (Row._make(x) for x in cur)
    
    def __del__(self):
        if self.kind is not None:
            statement = f"drop {self.kind} {self.name}"
            logger.debug("Executing %s", repr(statement))            
#             print(f"drop temporary {self.kind} {self.name}")
            self.db.execute(statement)
            self.db.commit()
