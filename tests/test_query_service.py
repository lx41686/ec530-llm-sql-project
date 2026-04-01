import pytest

from src.db import DatabaseManager
from src.query_service import QueryService
from src.schema_manager import SchemaManager
from src.validator import SQLValidator
from src.llm_adapter import LLMResponse

class FakeLLMAdapter:
    def __init__(self, sql: str) -> None:
        self.sql = sql

    def generate_sql(self, user_query: str, schema_context: dict[str, list[str]]) -> LLMResponse:
        return LLMResponse(sql=self.sql)


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
    schema_manager = SchemaManager(db)
    query_service = QueryService(db, validator, schema_manager)

    rows = query_service.run_sql_query("SELECT name FROM users ORDER BY id")

    assert rows == [("Alice",), ("Bob",)]

    db.close()


def test_run_sql_query_raises_error_for_invalid_query():
    db = DatabaseManager(":memory:")
    db.connect()

    validator = SQLValidator()
    schema_manager = SchemaManager(db)
    query_service = QueryService(db, validator, schema_manager)

    with pytest.raises(ValueError, match="Only SELECT queries are allowed."):
        query_service.run_sql_query("DELETE FROM users")

    db.close()


def test_run_sql_query_raises_error_for_unknown_table():
    db = DatabaseManager(":memory:")
    db.connect()

    validator = SQLValidator()
    schema_manager = SchemaManager(db)
    query_service = QueryService(db, validator, schema_manager)

    with pytest.raises(ValueError, match="Unknown table referenced: orders"):
        query_service.run_sql_query("SELECT * FROM orders")

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
    schema_manager = SchemaManager(db)
    query_service = QueryService(db, validator, schema_manager)

    tables = query_service.list_tables()

    assert "users" in tables
    assert "products" in tables

    db.close()

def test_run_natural_language_query_returns_generated_sql_and_rows():
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
    schema_manager = SchemaManager(db)
    fake_llm_adapter = FakeLLMAdapter("SELECT name FROM users ORDER BY id")
    query_service = QueryService(db, validator, schema_manager, fake_llm_adapter)

    sql, rows = query_service.run_natural_language_query("show all user names")

    assert sql == "SELECT name FROM users ORDER BY id"
    assert rows == [("Alice",), ("Bob",)]

    db.close()


def test_run_natural_language_query_raises_error_for_invalid_generated_sql():
    db = DatabaseManager(":memory:")
    db.connect()

    db.execute_script(
        """
        CREATE TABLE users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT
        );
        """
    )

    validator = SQLValidator()
    schema_manager = SchemaManager(db)
    fake_llm_adapter = FakeLLMAdapter("DELETE FROM users")
    query_service = QueryService(db, validator, schema_manager, fake_llm_adapter)

    import pytest
    with pytest.raises(ValueError, match="Only SELECT queries are allowed."):
        query_service.run_natural_language_query("delete all users")

    db.close()
