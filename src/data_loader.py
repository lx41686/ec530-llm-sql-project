import pandas as pd

from src.db import DatabaseManager
from src.schema_manager import SchemaManager


class DataLoader:
    def __init__(self, db_manager: DatabaseManager, schema_manager: SchemaManager) -> None:
        self.db_manager = db_manager
        self.schema_manager = schema_manager

    def load_csv(self, file_path: str, table_name: str) -> None:
        """Load a CSV file into SQLite. Create the table if it does not exist."""
        df = pd.read_csv(file_path)  # read csv

        # infer schema
        schema = self.schema_manager.infer_schema_from_dataframe(df, table_name)
        existing_schema = self.schema_manager.get_existing_schema(table_name)  # check if table exist

        # if table not exist -> create new table
        if existing_schema is None:
            create_table_sql = self.schema_manager.generate_create_table_sql(schema)
            self.db_manager.execute_script(create_table_sql)

        self.insert_rows(df, table_name)

    def insert_rows(self, df: pd.DataFrame, table_name: str) -> None:
        """Insert DataFrame rows into an existing SQLite table."""
        if self.db_manager.connection is None:
            raise ValueError("Database connection has not been established.")

        normalized_columns = [
            self.schema_manager.normalize_column_name(column_name)
            for column_name in df.columns
        ]

        placeholders = ", ".join(["?"] * len(normalized_columns))
        columns_sql = ", ".join(normalized_columns)

        insert_sql = f"INSERT INTO {table_name} ({columns_sql}) VALUES ({placeholders})"

        rows = []
        # df.itertuples: change each row into tuple, e.g.:
        # ("Alice", 25)
        # ("Bob", 30)
        # so SQLite executemany() can use it
        for row in df.itertuples(index=False, name=None):
            rows.append(tuple(row))

        cursor = self.db_manager.connection.cursor()
        cursor.executemany(insert_sql, rows)
        self.db_manager.connection.commit()
