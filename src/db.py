import sqlite3
from typing import Optional


class DatabaseManager:
    def __init__(self, db_path: str = "app.db") -> None:
        self.db_path = db_path
        self.connection: Optional[sqlite3.Connection] = None

    def connect(self) -> None:
        """Create a connection to the SQLite database."""
        self.connection = sqlite3.connect(self.db_path)

    def close(self) -> None:
        """Close the database connection if it exists."""
        if self.connection is not None:
            self.connection.close()
            self.connection = None

    def execute_script(self, sql: str) -> None:
        """Execute SQL script statements such as CREATE TABLE."""
        if self.connection is None:
            raise ValueError("Database connection has not been established.")

        cursor = self.connection.cursor()
        cursor.executescript(sql)
        self.connection.commit()

    def execute_select(self, sql: str) -> list[tuple]:
        """Execute a SELECT query and return all rows."""
        if self.connection is None:
            raise ValueError("Database connection has not been established.")

        cursor = self.connection.cursor()
        cursor.execute(sql)
        return cursor.fetchall()

    def list_tables(self) -> list[str]:
        """Return all table names in the current SQLite database."""
        if self.connection is None:
            raise ValueError("Database connection has not been established.")

        cursor = self.connection.cursor()
        cursor.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type='table'
            AND name NOT LIKE 'sqlite_%'
            ORDER BY name
            """
        )
        rows = cursor.fetchall()
        return [row[0] for row in rows]
