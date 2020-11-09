import sqlite3
from collections import namedtuple, deque
from io import StringIO
import weakref
import re
import logging

logger = logging.getLogger(__name__)

# "Attempting to DROP a table gets an SQLITE_LOCKED error if there are any active statements
# belonging to the same database connection"
# https://sqlite.org/forum/forumpost/433d2fdb07?raw
class Database:
    def __init__(self, name=""):
        self.name = name
        self._conn = sqlite3.connect(name)
        self._next_temp_id = 0
        self._active_iterators = weakref.WeakSet()
        self._gc_statements = deque()

    def _generate_temp_name(self):
        name = f"temp.temp_{self._next_temp_id}"
        self._next_temp_id += 1
        return name

    def _execute(self, statement, parameters=None):
        if parameters is not None:
            logger.debug(f"[{self!r}] Executing {statement!r} with parameters {parameters!r}")
            return self._conn.execute(statement, parameters)
        else:
            logger.debug(f"[{self!r}] Executing {statement!r}")
            return self._conn.execute(statement)            

    
    def _drop(self, statement):
        logger.debug(f"[{self!r}] Scheduling drop {statement!r}")        
        self._gc_statements.append(statement)
        self.garbage_collect()
    
    def garbage_collect(self):
        self._active_iterators = weakref.WeakSet(x for x in self._active_iterators if x.active)
        if not self._active_iterators and self._gc_statements:
            logger.debug(f"[{self!r}] Starting GC on database {self.name!r} {self!r}")            
            while self._gc_statements:
                statement = self._gc_statements.popleft()
                self._execute(statement)
    
    def query(self, select_stmt, kind="view", parameters=None, bindings={}):
        if re.match(r"\s*with\b", select_stmt):
            raise ValueError("sqltables: with clause not supported in query, please use bindings instead")
        preamble = []
        with_clauses = [
            f"{name} as (select * from {table.name})"
            for name, table in bindings.items() if table.name is not None
        ]
        with_stmt = "with " + ", ".join(with_clauses) if with_clauses else ""
        result_name = self._generate_temp_name()
        statement = f"create temporary {kind} {result_name} as {with_stmt} {select_stmt}"
        self._execute(statement, parameters)
        result = Table(name=result_name, db=self)
        result.bindings = bindings
        result.kind = kind
        weakref.finalize(result, self._drop, f"drop {kind} {result_name}")
        return result

    def iterate(self, table):
        statement = f"select * from {table.name}"
        result_iterator = RowIterator(statement, table)
        self._active_iterators.add(result_iterator)
        weakref.finalize(result_iterator, self.garbage_collect)
        return result_iterator
    
    def load_values(self, values, *, column_names, name=None):
        if name is None:
            name = self._generate_temp_name()
        column_spec = ",".join(column_names)
        value_spec = ",".join("?" for _ in column_names)
        self._execute(f"create temporary table {name} ({column_spec})")
        self._conn.executemany(f"insert into {name} values ({value_spec})", values)
        return Table(name=name, db=self)


class RowIterator:
    def __init__(self, statement, table):
        cur = table.db._execute(statement)
        self._cur = cur
        self.active = True
        self.statement = statement
        self.table = table
        self.column_names = [x[0] for x in cur.description]
        self.Row = namedtuple("Row", self.column_names, rename=True)

    def __iter__(self):
        return self
        
    def __next__(self):
        try:
            return self.Row._make(next(self._cur))
        except StopIteration:
            self.close()
            raise

    def close(self):
        self._cur.close()
        self.active = False
    
    def __del__(self):
        self.close()


class Table:
    def __init__(self, name, db):
        self.name = name
        self.db = db
        self.bindings = None
        self.kind = None

    def view(self, select_stmt, *, bindings={}):
        return self.db.query(select_stmt, kind="view", bindings=dict(_=self, **bindings))

    def table(self, select_stmt=None, parameters=None, *, bindings={}):
        if select_stmt is None:
            select_stmt = "select * from _"
        return self.db.query(select_stmt, kind="table", parameters=parameters, bindings=dict(_=self, **bindings))

    def __iter__(self):
        return self.db.iterate(self)

    def _repr_markdown_(self, limit=16):
        ascii_punctuation = r"""!"#$%&'()*+,-./:;<=>?@[\]^_`{|}~"""
        def q(x):
            return "".join("\\" + c if c in ascii_punctuation else c for c in x)
        out = StringIO()
        it = iter(self.view(f"select * from _ limit {limit+1}"))
        out.write("|" + "|".join(q(x) for x in it.column_names) + "|\n")
        out.write("|" + "|".join("-" for _ in it.column_names) + "|\n")
        for i,row in enumerate(it):
            if i < limit:
                data = [q(f"{x!r}") for x in row]
            else:
                data = ["..." for _ in row]
            out.write("|" + "|".join(data) + "|\n")
        return out.getvalue()
