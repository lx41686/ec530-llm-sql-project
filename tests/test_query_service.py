import pytest

from src.db import DatabaseManager
from src.query_service import QueryService
from src.validator import SQLValidator


def test_run_sql_query_returns_rows_for_valid_select():
    db = DatabaseManager(":memory:")
    db.connect()

    db.execute_script(
        """
        CREATE TABLE users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT
        );

        INSERT INTO users (name) VALUES ('Alice');
        INSERT INTO users (name) VALUES ('Bob');
        """
    )

    validator = SQLValidator()
    query_service = QueryService(db, validator)

    rows = query_service.run_sql_query("SELECT name FROM users ORDER BY id")

    assert rows == [("Alice",), ("Bob",)]

    db.close()


def test_run_sql_query_raises_error_for_invalid_query():
    db = DatabaseManager(":memory:")
    db.connect()

    validator = SQLValidator()
    query_service = QueryService(db, validator)

    with pytest.raises(ValueError, match="Only SELECT queries are allowed."):
        query_service.run_sql_query("DELETE FROM users")

    db.close()


def test_list_tables_returns_existing_tables():
    db = DatabaseManager(":memory:")
    db.connect()

    db.execute_script(
        """
        CREATE TABLE users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT
        );

        CREATE TABLE products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT
        );
        """
    )

    validator = SQLValidator()
    query_service = QueryService(db, validator)

    tables = query_service.list_tables()

    assert "users" in tables
    assert "products" in tables

    db.close()
