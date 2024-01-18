import pandas as pd
from sqlalchemy.engine import create_engine, Engine
from dataclasses import dataclass

class DbInterface:
    def read_query(self, query: str) -> pd.DataFrame:
        raise NotImplementedError()
    
    def write_to_table(self, table: str, data: pd.DataFrame):
        raise NotImplementedError()

@dataclass
class SqlServerDB(DbInterface):
  
    engine: Engine
      
    @staticmethod
    def from_url(connection_url: str) -> "SqlServerDB":
        return SqlServerDB(create_engine(connection_url))

    def read_query(self, query: str) -> pd.DataFrame:
        with self.engine.connect() as con:
            return pd.read_sql(query, con)
    
    def write_to_table(self, table: str, data: pd.DataFrame):
        with self.engine.connect() as con:
            data.to_sql(table, con, if_exists="fail")

@dataclass
class PostgreSqlDB(DbInterface):

    engine: Engine
    
    @staticmethod
    def from_url(connection_url: str) -> "PostgreSqlDB":
        return PostgreSqlDB(create_engine(connection_url))

    def read_query(self, query: str) -> pd.DataFrame:
        with self.engine.connect() as con:
            return pd.read_sql(query, con=con)
    
    def write_to_table(self, table: str, data: pd.DataFrame):
        with self.engine.connect() as con:
            data.to_sql(table, con, if_exists="fail")
            