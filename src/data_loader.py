import pandas as pd

from src.db import DatabaseManager
from src.schema_manager import SchemaManager


class DataLoader:
    """Handle CSV ingestion into the SQLite database.

    This class is responsible for the full data ingestion workflow:
    1. Read a CSV file into a pandas DataFrame
    2. Ask SchemaManager how the target table should be handled
       (create / append / overwrite / rename / skip)
    3. Insert rows into the correct database table

    Important:
        This class does NOT use pandas df.to_sql().
        Table creation and row insertion are implemented manually
        to satisfy the project requirements.
    """

    def __init__(self, db_manager: DatabaseManager, schema_manager: SchemaManager) -> None:
        """Initialize DataLoader with its dependencies.

        Args:
            db_manager: Handles low-level database execution.
            schema_manager: Handles schema inference and schema conflict logic.
        """
        self.db_manager = db_manager
        self.schema_manager = schema_manager

    def load_csv(self, file_path: str, table_name: str) -> None:
        """Load a CSV file into SQLite using schema-aware conflict handling.

        Process:
            1. Read the CSV file into a DataFrame
            2. Ask SchemaManager how this table should be handled
            3. If the action is create / append / overwrite / rename,
               insert rows into the returned target table
            4. If the action is skip, do not insert anything

        Args:
            file_path: Path to the CSV file on disk.
            table_name: Desired target table name in the database.

        Raises:
            ValueError: If the database connection has not been established.
        """
        # Read the CSV file from disk into a pandas DataFrame.
        # pandas is allowed for CSV reading and data inspection.
        df = pd.read_csv(file_path)

        # Ask SchemaManager to decide how this incoming CSV should be loaded.
        #
        # Returned values:
        # - target_table_name:
        #       The final table to insert into.
        #       This may be:
        #       * the original table name
        #       * a renamed table if there was a schema conflict
        #       * None if the user chose to skip loading
        #
        # - action:
        #       One of:
        #       * "create"    -> table did not exist, new table was created
        #       * "append"    -> schema matched, append rows
        #       * "overwrite" -> schema conflict, old table dropped and recreated
        #       * "rename"    -> schema conflict, new non-conflicting table created
        #       * "skip"      -> user chose not to load data
        target_table_name, action = self.schema_manager.prepare_table_for_load(df, table_name)

        # If the user chose to skip loading because of a schema conflict,
        # do not insert any rows.
        if action == "skip" or target_table_name is None:
            return

        # Otherwise, insert rows into the final target table.
        # This covers:
        # - create
        # - append
        # - overwrite
        # - rename
        self.insert_rows(df, target_table_name)

    def insert_rows(self, df: pd.DataFrame, table_name: str) -> None:
        """Insert DataFrame rows into an existing SQLite table.

        Args:
            df: The DataFrame containing rows to insert.
            table_name: The database table name to insert into.

        Raises:
            ValueError: If the database connection has not been established.

        Design notes:
            - Column names are normalized before generating SQL
            - Parameter placeholders (?) are used for safe insertion
            - Rows are inserted using executemany() for efficiency
        """
        if self.db_manager.connection is None:
            raise ValueError("Database connection has not been established.")

        # Normalize DataFrame column names so they match the normalized
        # schema used in the database.
        #
        # Example:
        #   "User Name" -> "user_name"
        normalized_columns = [
            self.schema_manager.normalize_column_name(column_name)
            for column_name in df.columns
        ]

        # Create a placeholder string for parameterized SQL.
        #
        # Example:
        # If there are 3 columns, placeholders becomes:
        #   "?, ?, ?"
        placeholders = ", ".join(["?"] * len(normalized_columns))

        # Build the comma-separated column list.
        #
        # Example:
        #   "name, age, score"
        columns_sql = ", ".join(normalized_columns)

        # Manually construct the INSERT statement.
        #
        # Example:
        #   INSERT INTO users (name, age) VALUES (?, ?)
        insert_sql = f"INSERT INTO {table_name} ({columns_sql}) VALUES ({placeholders})"

        # Convert each DataFrame row into a tuple so it can be used by SQLite.
        #
        # Example:
        #   ("Alice", 25)
        #   ("Bob", 30)
        #
        # We do not include the DataFrame index because it is not part
        # of the table schema.
        rows = []
        for row in df.itertuples(index=False, name=None):
            rows.append(tuple(row))

        # Execute the batch insert and commit the transaction.
        cursor = self.db_manager.connection.cursor()
        cursor.executemany(insert_sql, rows)
        self.db_manager.connection.commit()