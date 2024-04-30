import os
from sqlalchemy import create_engine, Connection

class DB_Connection:

    def __init__(self, db_url) -> None:
        engine = create_engine(db_url)
        self._connection = engine.connect()

    @property
    def connection(self) -> Connection:
        return self._connection
    
    @classmethod
    def from_env(cls):
        return cls(os.environ["DATABASE_URL"])
    
    
def SQL_INSERT_STATEMENT_FROM_DATAFRAME(source: str, target: str) -> "str":
    """Returns a 'INSERT INTO target VALUES (...), (...);"""
    insert_rows = [str(tuple(row.values)) for index, row in source.iterrows()]
    statement = "INSERT INTO " + target + " (" + str(', '.join([f'"{x}"' for x in source.columns])) + ") " + "VALUES " + ', '.join(insert_rows)
    return statement + ";"