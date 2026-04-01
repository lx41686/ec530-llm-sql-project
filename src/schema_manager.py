from dataclasses import dataclass
import pandas as pd

from src.db import DatabaseManager


@dataclass
class ColumnSchema:
    """
    Structure of column, e.g. ColumnSchema(name="age", data_type="INTEGER")
    """
    name: str
    data_type: str


@dataclass
class TableSchema:
    '''
    Structure of Table, e.g.
    TableSchema(
    table_name="users",
    columns=[
        ColumnSchema(name="name", data_type="TEXT"),
        ColumnSchema(name="age", data_type="INTEGER")
    ])
    '''
    table_name: str
    columns: list[ColumnSchema]


class SchemaManager:
    def __init__(self, db_manager: DatabaseManager) -> None:
        # input db_manager as we need the data structure later
        self.db_manager = db_manager

    def normalize_column_name(self, column_name: str) -> str:
        """Normalize a column name for consistent schema comparison."""
        # remove spaces and change to lower cases
        return column_name.strip().lower().replace(" ", "_")

    def infer_sql_type(self, pandas_dtype: str) -> str:
        """Map a pandas dtype to an SQLite data type."""
        dtype = str(pandas_dtype).lower()

        # can add more types later
        if "int" in dtype:
            return "INTEGER"
        if "float" in dtype:
            return "REAL"
        return "TEXT"

    def infer_schema_from_dataframe(self, df: pd.DataFrame, table_name: str) -> TableSchema:
        """
        Infer a table schema from a pandas DataFrame.
        e.g. CSV contains：
        •	Name
        •	Age
        •	Salary
        then change to：
        •	name TEXT
        •	age INTEGER
        •	salary REAL
        """
        columns = []

        for column_name, dtype in df.dtypes.items():
            normalized_name = self.normalize_column_name(column_name)
            sql_type = self.infer_sql_type(dtype)
            columns.append(ColumnSchema(name=normalized_name, data_type=sql_type))

        return TableSchema(table_name=table_name, columns=columns)

    def get_existing_schema(self, table_name: str) -> TableSchema | None:
        """Read an existing table schema from SQLite using PRAGMA table_info."""
        if self.db_manager.connection is None:
            raise ValueError("Database connection has not been established.")

        cursor = self.db_manager.connection.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")
        rows = cursor.fetchall()

        if not rows:
            return None

        columns = []
        for row in rows:
            column_name = row[1]
            data_type = row[2]

            if column_name == "id":
                # skip id, as id is added for each new table by:
                # id INTEGER PRIMARY KEY AUTOINCREMENT
                continue

            columns.append(
                ColumnSchema(
                    name=self.normalize_column_name(column_name),
                    data_type=data_type.upper(),
                )
            )

        return TableSchema(table_name=table_name, columns=columns)

    def generate_create_table_sql(self, schema: TableSchema) -> str:
        """Generate a CREATE TABLE statement from a TableSchema."""
        column_definitions = ["id INTEGER PRIMARY KEY AUTOINCREMENT"]

        for column in schema.columns:
            column_definitions.append(f"{column.name} {column.data_type}")

        columns_sql = ",\n    ".join(column_definitions)

        return f"""CREATE TABLE {schema.table_name} (
    {columns_sql}
);"""
