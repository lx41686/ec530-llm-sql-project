import pytest

from src.db import DatabaseManager
from src.query_service import QueryService
from src.schema_manager import SchemaManager
from src.validator import SQLValidator
from src.llm_adapter import LLMResponse

# Test QueryService → Validator → SchemaManager → DB
class FakeLLMAdapter:
    """A fake LLM adapter used for testing QueryService.

    This mock object avoids real API calls by always returning
    a predefined SQL string wrapped in an LLMResponse.
    """
    def __init__(self, sql: str) -> None:
        """Initialize the fake adapter with a fixed SQL response.

        Args:
            sql: The SQL string that should always be returned by generate_sql.
        """
        self.sql = sql

    def generate_sql(self, user_query: str, schema_context: dict[str, list[str]]) -> LLMResponse:
        """Return a predefined SQL response regardless of input.

        Args:
            user_query: The user's natural language query.
            schema_context: The database schema context.

        Returns:
            An LLMResponse containing the predefined SQL string.
        """
        return LLMResponse(sql=self.sql)


def test_run_sql_query_returns_rows_for_valid_select():
    """Test that run_sql_query returns rows for a valid SELECT statement.

    This verifies that QueryService correctly validates a safe SQL query,
    executes it through the database layer, and returns the expected results.
    """
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
    """Test that run_sql_query raises an error for invalid non-SELECT queries.

    This ensures that QueryService does not allow unsafe queries
    such as DELETE to reach the database.
    """
    db = DatabaseManager(":memory:")
    db.connect()

    validator = SQLValidator()
    schema_manager = SchemaManager(db)
    query_service = QueryService(db, validator, schema_manager)

    with pytest.raises(ValueError, match="Only SELECT queries are allowed."):
        # cannot run DELETE
        query_service.run_sql_query("DELETE FROM users")

    db.close()


def test_run_sql_query_raises_error_for_unknown_table():
    """Test that run_sql_query raises an error for queries referencing unknown tables.

    This verifies that QueryService uses schema-aware validation
    and rejects queries that reference tables not present in the database.
    """
    db = DatabaseManager(":memory:")
    db.connect()

    validator = SQLValidator()
    schema_manager = SchemaManager(db)
    query_service = QueryService(db, validator, schema_manager)

    # No orders in table
    with pytest.raises(ValueError, match="Unknown table referenced: orders"):
        query_service.run_sql_query("SELECT * FROM orders")

    db.close()


def test_list_tables_returns_existing_tables():
    """Test that list_tables returns all existing database tables.

    This verifies that QueryService correctly delegates table listing
    to the database layer and returns the expected table names.
    """
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
    """Test that run_natural_language_query returns generated SQL and query results.

    This verifies the full natural language query flow:
    natural language input → SQL generation → validation → database execution.
    """
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
    """Test that run_natural_language_query rejects invalid SQL generated by the LLM.

    This ensures that even if the LLM generates unsafe SQL, such as DELETE
    QueryService still validates it and prevents execution.
    """
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

def test_case_insensitive_query():
    """Test that a case-sensitive SQL query"""
    db = DatabaseManager(":memory:")
    db.connect()

    db.execute_script(
        """
        CREATE TABLE users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            age INTEGER
        );

        INSERT INTO users (name, age) VALUES ('Alice', 25);
        """
    )

    validator = SQLValidator()
    schema_manager = SchemaManager(db)

    fake_llm = FakeLLMAdapter("SELECT age FROM users WHERE name = 'alice'")

    query_service = QueryService(db, validator, schema_manager, fake_llm)

    sql, rows = query_service.run_natural_language_query("age of alice")

    assert rows == [(25,)]

    db.close()

# def test_case_insensitive_query_fixed():
#     """Test that a refined SQL query using LOWER() correctly handles case-insensitive matching."""
#     db = DatabaseManager(":memory:")
#     db.connect()
#
#     db.execute_script(
#         """
#         CREATE TABLE users (
#             id INTEGER PRIMARY KEY AUTOINCREMENT,
#             name TEXT,
#             age INTEGER
#         );
#
#         INSERT INTO users (name, age) VALUES ('Alice', 25);
#         """
#     )
#
#     validator = SQLValidator()
#     schema_manager = SchemaManager(db)
#
#     # fixed by adding LOWER(name)
#     fake_llm = FakeLLMAdapter(
#         "SELECT age FROM users WHERE LOWER(name) = LOWER('alice')"
#     )
#
#     query_service = QueryService(db, validator, schema_manager, fake_llm)
#
#     sql, rows = query_service.run_natural_language_query("age of alice")
#
#     assert rows == [(25,)]
#
#     db.close()
