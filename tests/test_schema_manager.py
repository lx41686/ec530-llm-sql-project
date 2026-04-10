import pandas as pd
import pytest

from src.db import DatabaseManager
from src.schema_manager import SchemaManager, TableSchema, ColumnSchema


# These tests verify that SchemaManager can:
# - normalize column names
# - infer schema from DataFrame data
# - generate CREATE TABLE SQL
# - read existing schema from SQLite
# - compare schemas
# - resolve schema conflicts
# - create, drop, and rename tables
# - log errors and invalid user choices


def test_normalize_column_name():
    """Test that normalize_column_name converts names into a consistent format.

    This verifies that:
    - leading and trailing spaces are removed
    - uppercase letters are converted to lowercase
    - internal spaces are replaced with underscores

    Example:
        " User Name " -> "user_name"
    """
    db = DatabaseManager(":memory:")
    schema_manager = SchemaManager(db)

    normalized = schema_manager.normalize_column_name(" User Name ")

    assert normalized == "user_name"


def test_infer_schema_from_dataframe():
    """Test that infer_schema_from_dataframe converts a DataFrame into a TableSchema.

    This verifies that:
    - the table name is preserved
    - column names are normalized
    - pandas data types are mapped to SQLite types
    """
    db = DatabaseManager(":memory:")
    schema_manager = SchemaManager(db)

    df = pd.DataFrame(
        {
            "Name": ["Alice", "Bob"],
            "Age": [25, 30],
            "Score": [95.5, 88.0],
        }
    )

    schema = schema_manager.infer_schema_from_dataframe(df, "students")

    assert schema.table_name == "students"
    assert schema.columns[0] == ColumnSchema(name="name", data_type="TEXT")
    assert schema.columns[1] == ColumnSchema(name="age", data_type="INTEGER")
    assert schema.columns[2] == ColumnSchema(name="score", data_type="REAL")


def test_generate_create_table_sql():
    """Test that generate_create_table_sql builds a valid CREATE TABLE statement.

    This verifies that:
    - the table name is included
    - the auto-generated primary key column is added
    - all business columns are included with the correct SQL types
    """
    db = DatabaseManager(":memory:")
    schema_manager = SchemaManager(db)

    schema = TableSchema(
        table_name="users",
        columns=[
            ColumnSchema(name="name", data_type="TEXT"),
            ColumnSchema(name="age", data_type="INTEGER"),
        ],
    )

    sql = schema_manager.generate_create_table_sql(schema)

    assert "CREATE TABLE users" in sql
    assert "id INTEGER PRIMARY KEY AUTOINCREMENT" in sql
    assert "name TEXT" in sql
    assert "age INTEGER" in sql


def test_get_existing_schema():
    """Test that get_existing_schema reads schema information from an existing table.

    This verifies that:
    - SchemaManager can read table structure from SQLite using PRAGMA
    - the table name is returned correctly
    - the auto-generated id column is excluded from the returned business schema
    """
    db = DatabaseManager(":memory:")
    db.connect()

    db.execute_script(
        """
        CREATE TABLE users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            age INTEGER
        );
        """
    )

    schema_manager = SchemaManager(db)
    schema = schema_manager.get_existing_schema("users")

    assert schema is not None
    assert schema.table_name == "users"
    assert schema.columns == [
        ColumnSchema(name="name", data_type="TEXT"),
        ColumnSchema(name="age", data_type="INTEGER"),
    ]

    db.close()


def test_schemas_match_returns_true_for_identical_schemas():
    """Test that schemas_match returns True when column names and types match exactly.

    This verifies the exact matching rule required by the project:
    - normalized column names must match
    - data types must match
    """
    db = DatabaseManager(":memory:")
    schema_manager = SchemaManager(db)

    incoming_schema = TableSchema(
        table_name="users",
        columns=[
            ColumnSchema(name="name", data_type="TEXT"),
            ColumnSchema(name="age", data_type="INTEGER"),
        ],
    )

    existing_schema = TableSchema(
        table_name="users",
        columns=[
            ColumnSchema(name="name", data_type="TEXT"),
            ColumnSchema(name="age", data_type="INTEGER"),
        ],
    )

    assert schema_manager.schemas_match(incoming_schema, existing_schema) is True


def test_schemas_match_returns_false_for_different_column_names():
    """Test that schemas_match returns False when column names differ.

    This verifies that schemas are rejected if the normalized business column names
    are not exactly the same.
    """
    db = DatabaseManager(":memory:")
    schema_manager = SchemaManager(db)

    incoming_schema = TableSchema(
        table_name="users",
        columns=[
            ColumnSchema(name="full_name", data_type="TEXT"),
            ColumnSchema(name="age", data_type="INTEGER"),
        ],
    )

    existing_schema = TableSchema(
        table_name="users",
        columns=[
            ColumnSchema(name="name", data_type="TEXT"),
            ColumnSchema(name="age", data_type="INTEGER"),
        ],
    )

    assert schema_manager.schemas_match(incoming_schema, existing_schema) is False


def test_schemas_match_returns_false_for_different_data_types():
    """Test that schemas_match returns False when data types differ.

    This verifies that schemas are rejected if matching column names
    do not also have matching SQL data types.
    """
    db = DatabaseManager(":memory:")
    schema_manager = SchemaManager(db)

    incoming_schema = TableSchema(
        table_name="users",
        columns=[
            ColumnSchema(name="name", data_type="TEXT"),
            ColumnSchema(name="age", data_type="REAL"),
        ],
    )

    existing_schema = TableSchema(
        table_name="users",
        columns=[
            ColumnSchema(name="name", data_type="TEXT"),
            ColumnSchema(name="age", data_type="INTEGER"),
        ],
    )

    assert schema_manager.schemas_match(incoming_schema, existing_schema) is False


def test_log_error_writes_message_to_log_file(tmp_path):
    """Test that log_error writes messages to the configured log file.

    This verifies that SchemaManager supports persistent error logging
    for defensive coding and conflict handling.
    """
    log_file = tmp_path / "error_log.txt"

    db = DatabaseManager(":memory:")
    schema_manager = SchemaManager(db, log_file=str(log_file))

    schema_manager.log_error("Schema conflict detected.")

    assert log_file.exists()
    assert "Schema conflict detected." in log_file.read_text(encoding="utf-8")


def test_create_table_creates_table_in_database():
    """Test that create_table creates a new SQLite table from a schema definition.

    This verifies that SchemaManager can take a TableSchema object
    and create the corresponding table in the database.
    """
    db = DatabaseManager(":memory:")
    db.connect()

    schema_manager = SchemaManager(db)
    schema = TableSchema(
        table_name="users",
        columns=[
            ColumnSchema(name="name", data_type="TEXT"),
            ColumnSchema(name="age", data_type="INTEGER"),
        ],
    )

    schema_manager.create_table(schema)

    tables = db.list_tables()

    assert "users" in tables

    db.close()


def test_drop_table_removes_existing_table():
    """Test that drop_table removes an existing table from the database.

    This verifies that SchemaManager can delete a table when overwrite behavior
    is selected during schema conflict handling.
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

    schema_manager = SchemaManager(db)
    schema_manager.drop_table("users")

    tables = db.list_tables()

    assert "users" not in tables

    db.close()


def test_get_non_conflicting_table_name_returns_original_name_if_available():
    """Test that get_non_conflicting_table_name returns the original name when no conflict exists.

    This verifies that no unnecessary rename is performed if the target table name
    is not already used in the database.
    """
    db = DatabaseManager(":memory:")
    db.connect()

    schema_manager = SchemaManager(db)

    table_name = schema_manager.get_non_conflicting_table_name("users")

    assert table_name == "users"

    db.close()


def test_get_non_conflicting_table_name_returns_renamed_name_if_conflict_exists():
    """Test that get_non_conflicting_table_name generates a new name when the table already exists.

    This verifies that SchemaManager can safely create renamed tables such as users_1
    when the original table name is already taken.
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

    schema_manager = SchemaManager(db)

    table_name = schema_manager.get_non_conflicting_table_name("users")

    assert table_name == "users_1"

    db.close()


def test_prompt_schema_conflict_action_accepts_valid_choice(monkeypatch):
    """Test that prompt_schema_conflict_action accepts a valid user choice.

    This verifies that the function returns a valid action string
    when the user enters one of the allowed options.
    """
    db = DatabaseManager(":memory:")
    schema_manager = SchemaManager(db)

    monkeypatch.setattr("builtins.input", lambda _: "overwrite")

    action = schema_manager.prompt_schema_conflict_action("users")

    assert action == "overwrite"


def test_prompt_schema_conflict_action_reprompts_after_invalid_choice(monkeypatch, tmp_path):
    """Test that prompt_schema_conflict_action logs invalid input and asks again.

    This verifies that:
    - invalid conflict choices are rejected
    - the invalid input is logged
    - the function keeps asking until a valid choice is provided
    """
    log_file = tmp_path / "error_log.txt"

    db = DatabaseManager(":memory:")
    schema_manager = SchemaManager(db, log_file=str(log_file))

    # First input is invalid, second input is valid.
    responses = iter(["wrong_choice", "skip"])
    monkeypatch.setattr("builtins.input", lambda _: next(responses))

    action = schema_manager.prompt_schema_conflict_action("users")

    assert action == "skip"
    assert "Invalid schema conflict choice entered for table 'users': wrong_choice" in log_file.read_text(
        encoding="utf-8"
    )


def test_prepare_table_for_load_returns_create_when_table_does_not_exist():
    """Test that prepare_table_for_load creates a table when it does not already exist.

    This verifies the normal ingestion path:
    - infer schema from DataFrame
    - create a new table
    - return action = 'create'
    """
    db = DatabaseManager(":memory:")
    db.connect()

    schema_manager = SchemaManager(db)

    df = pd.DataFrame(
        {
            "Name": ["Alice", "Bob"],
            "Age": [25, 30],
        }
    )

    target_table_name, action = schema_manager.prepare_table_for_load(df, "users")

    assert target_table_name == "users"
    assert action == "create"
    assert "users" in db.list_tables()

    db.close()


def test_prepare_table_for_load_returns_append_when_schema_matches():
    """Test that prepare_table_for_load returns append when existing schema matches.

    This verifies the exact-match rule:
    - if normalized column names match
    - and data types match exactly
    - then new data should be appended to the existing table
    """
    db = DatabaseManager(":memory:")
    db.connect()

    db.execute_script(
        """
        CREATE TABLE users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            age INTEGER
        );
        """
    )

    schema_manager = SchemaManager(db)

    df = pd.DataFrame(
        {
            "Name": ["Alice", "Bob"],
            "Age": [25, 30],
        }
    )

    target_table_name, action = schema_manager.prepare_table_for_load(df, "users")

    assert target_table_name == "users"
    assert action == "append"

    db.close()


def test_prepare_table_for_load_handles_overwrite(monkeypatch):
    """Test that prepare_table_for_load overwrites a conflicting table when user chooses overwrite.

    This verifies that:
    - a schema conflict is detected
    - the user chooses overwrite
    - the old table is dropped
    - a new table is created with the incoming schema
    """
    db = DatabaseManager(":memory:")
    db.connect()

    # Create an intentionally conflicting schema.
    db.execute_script(
        """
        CREATE TABLE users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT
        );
        """
    )

    schema_manager = SchemaManager(db)

    df = pd.DataFrame(
        {
            "Name": ["Alice", "Bob"],
            "Age": [25, 30],
        }
    )

    monkeypatch.setattr("builtins.input", lambda _: "overwrite")

    target_table_name, action = schema_manager.prepare_table_for_load(df, "users")

    assert target_table_name == "users"
    assert action == "overwrite"

    # After overwrite, the recreated table should now contain both name and age.
    recreated_schema = schema_manager.get_existing_schema("users")
    assert recreated_schema is not None
    assert recreated_schema.columns == [
        ColumnSchema(name="name", data_type="TEXT"),
        ColumnSchema(name="age", data_type="INTEGER"),
    ]

    db.close()


def test_prepare_table_for_load_handles_rename(monkeypatch):
    """Test that prepare_table_for_load creates a renamed table when user chooses rename.

    This verifies that:
    - a schema conflict is detected
    - the original table is preserved
    - a new non-conflicting table name is generated
    - a new table is created under that name
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

    schema_manager = SchemaManager(db)

    df = pd.DataFrame(
        {
            "Name": ["Alice", "Bob"],
            "Age": [25, 30],
        }
    )

    monkeypatch.setattr("builtins.input", lambda _: "rename")

    target_table_name, action = schema_manager.prepare_table_for_load(df, "users")

    assert target_table_name == "users_1"
    assert action == "rename"
    assert "users" in db.list_tables()
    assert "users_1" in db.list_tables()

    renamed_schema = schema_manager.get_existing_schema("users_1")
    assert renamed_schema is not None
    assert renamed_schema.columns == [
        ColumnSchema(name="name", data_type="TEXT"),
        ColumnSchema(name="age", data_type="INTEGER"),
    ]

    db.close()


def test_prepare_table_for_load_handles_skip(monkeypatch, tmp_path):
    """Test that prepare_table_for_load skips loading when user chooses skip.

    This verifies that:
    - a schema conflict is detected
    - the user chooses skip
    - no new table is created
    - the skip action is logged
    """
    log_file = tmp_path / "error_log.txt"

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

    schema_manager = SchemaManager(db, log_file=str(log_file))

    df = pd.DataFrame(
        {
            "Name": ["Alice", "Bob"],
            "Age": [25, 30],
        }
    )

    monkeypatch.setattr("builtins.input", lambda _: "skip")

    target_table_name, action = schema_manager.prepare_table_for_load(df, "users")

    assert target_table_name is None
    assert action == "skip"
    assert "users_1" not in db.list_tables()
    assert "Skipped loading CSV into 'users' due to schema conflict." in log_file.read_text(
        encoding="utf-8"
    )

    db.close()
