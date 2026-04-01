import sqlite3
from typing import Optional


class DatabaseManager:
    # no sqlite3.connect(...) in other modules, only via DatabaseManager
    # db = DatabaseManager
    # db.connect() etc.
    def __init__(self, db_path: str = "app.db") -> None:
        # default name of db: app.db
        self.db_path = db_path
        # connection = None, only after db.connect() it has values
        self.connection: Optional[sqlite3.Connection] = None

    def connect(self) -> None:
        """Create a connection to the SQLite database."""
        # open db, if not exit -> create, return a connection object
        self.connection = sqlite3.connect(self.db_path)

    def close(self) -> None:
        """Close the database connection if it exists."""
        if self.connection is not None:
            self.connection.close()
            self.connection = None  # change to None

    def execute_script(self, sql: str) -> None:
        """Execute SQL script statements such as CREATE TABLE."""
        if self.connection is None:
            raise ValueError("Database connection has not been established.")  # check connection

        cursor = self.connection.cursor()
        cursor.executescript(sql)  # execute: single SQL, executescript: multiple SQL
        self.connection.commit()  # save changes

    def execute_select(self, sql: str) -> list[tuple]:
        """Execute a SELECT query and return all rows."""
        if self.connection is None:
            raise ValueError("Database connection has not been established.")

        cursor = self.connection.cursor()
        cursor.execute(sql)
        return cursor.fetchall()  # get results

    def list_tables(self) -> list[str]:
        """Return all table names in the current SQLite database."""
        if self.connection is None:
            raise ValueError("Database connection has not been established.")

        cursor = self.connection.cursor()
        # key SQL:
        cursor.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type='table'
            AND name NOT LIKE 'sqlite_%'
            ORDER BY name
            """
        )

        # example rows: [("users",), ("products",)]
        rows = cursor.fetchall()
        return [row[0] for row in rows]
