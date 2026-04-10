from dataclasses import dataclass
from pathlib import Path
import pandas as pd

from src.db import DatabaseManager


@dataclass
class ColumnSchema:
    """Represents a single column in a table schema."""
    name: str
    data_type: str


@dataclass
class TableSchema:
    """Represents a table schema made of a table name and column definitions."""
    table_name: str
    columns: list[ColumnSchema]


class SchemaManager:
    """Manage schema inference, comparison, conflict handling, and table creation."""

    def __init__(self, db_manager: DatabaseManager, log_file: str = "error_log.txt") -> None:
        """Initialize SchemaManager with a database manager and log file path."""
        self.db_manager = db_manager
        self.log_file = Path(log_file)

    def log_error(self, message: str) -> None:
        """Append an error or warning message to the log file."""
        with self.log_file.open("a", encoding="utf-8") as file:
            file.write(f"{message}\n")

    def normalize_column_name(self, column_name: str) -> str:
        """Normalize a column name for consistent schema comparison."""
        return column_name.strip().lower().replace(" ", "_")

    def infer_sql_type(self, pandas_dtype: str) -> str:
        """Map a pandas dtype to an SQLite data type."""
        dtype = str(pandas_dtype).lower()

        if "int" in dtype:
            return "INTEGER"
        if "float" in dtype:
            return "REAL"
        return "TEXT"

    def infer_schema_from_dataframe(self, df: pd.DataFrame, table_name: str) -> TableSchema:
        """Infer a table schema from a pandas DataFrame."""
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

            # Skip the auto-generated primary key column.
            if column_name == "id":
                continue

            columns.append(
                ColumnSchema(
                    name=self.normalize_column_name(column_name),
                    data_type=data_type.upper(),
                )
            )

        return TableSchema(table_name=table_name, columns=columns)

    def schemas_match(self, incoming_schema: TableSchema, existing_schema: TableSchema) -> bool:
        """Return True if normalized column names and data types match exactly."""
        if len(incoming_schema.columns) != len(existing_schema.columns):
            return False

        for incoming_column, existing_column in zip(incoming_schema.columns, existing_schema.columns):
            if incoming_column.name != existing_column.name:
                return False
            if incoming_column.data_type != existing_column.data_type:
                return False

        return True

    def generate_create_table_sql(self, schema: TableSchema) -> str:
        """Generate a CREATE TABLE statement from a TableSchema."""
        column_definitions = ["id INTEGER PRIMARY KEY AUTOINCREMENT"]

        for column in schema.columns:
            column_definitions.append(f"{column.name} {column.data_type}")

        columns_sql = ",\n    ".join(column_definitions)

        return f"""CREATE TABLE {schema.table_name} (
    {columns_sql}
);"""

    def create_table(self, schema: TableSchema) -> None:
        """Create a new table in SQLite from a schema definition."""
        create_table_sql = self.generate_create_table_sql(schema)
        self.db_manager.execute_script(create_table_sql)

    def drop_table(self, table_name: str) -> None:
        """Drop an existing table from SQLite."""
        self.db_manager.execute_script(f"DROP TABLE IF EXISTS {table_name};")

    def get_non_conflicting_table_name(self, base_table_name: str) -> str:
        """Generate a new table name if the requested name already exists."""
        existing_tables = set(self.db_manager.list_tables())

        if base_table_name not in existing_tables:
            return base_table_name

        counter = 1
        while True:
            candidate_name = f"{base_table_name}_{counter}"
            if candidate_name not in existing_tables:
                return candidate_name
            counter += 1

    def prompt_schema_conflict_action(self, table_name: str) -> str:
        """Prompt the user to resolve a schema conflict by overwrite, rename, or skip."""
        while True:
            user_choice = input(
                f"Schema conflict detected for table '{table_name}'. "
                "Choose an action: overwrite / rename / skip: "
            ).strip().lower()

            if user_choice in {"overwrite", "rename", "skip"}:
                return user_choice

            print("Invalid choice. Please enter: overwrite, rename, or skip.")
            self.log_error(
                f"Invalid schema conflict choice entered for table '{table_name}': {user_choice}"
            )

    def prepare_table_for_load(self, df: pd.DataFrame, table_name: str) -> tuple[str | None, str]:
        """Determine how incoming CSV data should be loaded.

        Returns:
            A tuple of:
            - target table name (or None if skipped)
            - action string: 'append', 'create', 'overwrite', 'rename', or 'skip'
        """
        incoming_schema = self.infer_schema_from_dataframe(df, table_name)
        existing_schema = self.get_existing_schema(table_name)

        if existing_schema is None:
            self.create_table(incoming_schema)
            return table_name, "create"

        if self.schemas_match(incoming_schema, existing_schema):
            return table_name, "append"

        action = self.prompt_schema_conflict_action(table_name)

        if action == "overwrite":
            self.drop_table(table_name)
            self.create_table(incoming_schema)
            return table_name, "overwrite"

        if action == "rename":
            new_table_name = self.get_non_conflicting_table_name(table_name)
            renamed_schema = self.infer_schema_from_dataframe(df, new_table_name)
            self.create_table(renamed_schema)
            return new_table_name, "rename"

        self.log_error(f"Skipped loading CSV into '{table_name}' due to schema conflict.")
        return None, "skip"