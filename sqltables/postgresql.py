import io
import psycopg2
from . import generic

class Database (generic.Database):
    """Connection to a PostgreSQL database.

    Args:
        name: Name of the database, passed to :py:func:`psycopg2.connect`.

    """
    
    def __init__(self, name):
        conn = psycopg2.connect(name)
        super().__init__(conn)
        self.name = name
        self.value_placeholder = "%s"  
        
    def _insert_values(self, table, values, column_names=None):
        def quote_copy_text(value):
            if value is None:
                return r"\N"
            return (str(value)
                    .replace("\\", "\\\\")
                    .replace("\t", r"\t")
                    .replace("\n", r"\n")
                    .replace("\r", r"\r"))

        def encode_rows(rows):
            buf = io.StringIO()
            for row in rows:
                buf.write("\t".join(quote_copy_text(x) for x in row))
                buf.write("\n")
            buf.seek(0)
            return buf
        
        buf = encode_rows(values)
        quoted_name = self.quote_name(table.name)
        with self._conn:
            cursor = self._conn.cursor()
            cursor.copy_from(buf, quoted_name)
        

