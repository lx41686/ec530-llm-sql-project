import pandas as pd
import pytest

from src.db import DatabaseManager
from src.schema_manager import SchemaManager
from src.data_loader import DataLoader


# These tests verify that CSV or DataFrame data can be correctly inserted into the database.
# They also verify the new schema conflict handling workflow:
# - create
# - append
# - overwrite
# - rename
# - skip


def test_insert_rows_inserts_dataframe_data():
    """Test that insert_rows correctly inserts DataFrame data into an existing table.

    This test verifies that:
    - DataLoader can normalize column names from a DataFrame
    - Rows are correctly inserted into a pre-existing table
    - Inserted data can be retrieved accurately from the database
    """
    db = DatabaseManager(":memory:")
    db.connect()

    schema_manager = SchemaManager(db)
    data_loader = DataLoader(db, schema_manager)

    # Create the table manually first because this test is only about row insertion,
    # not about schema creation.
    db.execute_script(
        """
        CREATE TABLE users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            age INTEGER
        );
        """
    )

    # The CSV-like data uses "Name" and "Age", but DataLoader should normalize
    # them to match the database columns "name" and "age".
    df = pd.DataFrame(
        {
            "Name": ["Alice", "Bob"],
            "Age": [25, 30],
        }
    )

    data_loader.insert_rows(df, "users")

    rows = db.execute_select("SELECT name, age FROM users ORDER BY id")

    assert rows == [("Alice", 25), ("Bob", 30)]

    db.close()


def test_load_csv_creates_table_and_inserts_rows(tmp_path):
    """Test that load_csv performs full data ingestion when the table does not exist.

    This test verifies the normal "create" path:
    - A CSV file is read from disk
    - SchemaManager infers the schema
    - A new table is created
    - Rows are inserted into the new table
    - Data can be queried correctly after ingestion

    This is an integration-style test for:
    CSV → DataFrame → Schema → CREATE TABLE → INSERT → DB
    """
    csv_file = tmp_path / "students.csv"
    csv_file.write_text("Name,Age,Score\nAlice,25,95.5\nBob,30,88.0\n")

    db = DatabaseManager(":memory:")
    db.connect()

    schema_manager = SchemaManager(db)
    data_loader = DataLoader(db, schema_manager)

    # Because the table does not exist yet, SchemaManager should choose "create".
    data_loader.load_csv(str(csv_file), "students")

    tables = db.list_tables()
    rows = db.execute_select("SELECT name, age, score FROM students ORDER BY id")

    assert "students" in tables
    assert rows == [("Alice", 25, 95.5), ("Bob", 30, 88.0)]

    db.close()


def test_load_csv_appends_rows_when_schema_matches(tmp_path):
    """Test that load_csv appends rows when an existing table has a matching schema.

    This test verifies the "append" path:
    - A table already exists
    - The incoming CSV schema matches the existing table schema exactly
    - New rows are appended instead of creating a new table
    """
    csv_file = tmp_path / "users.csv"
    csv_file.write_text("Name,Age\nAlice,25\nBob,30\n")

    db = DatabaseManager(":memory:")
    db.connect()

    # Pre-create a table with the same normalized schema as the CSV.
    db.execute_script(
        """
        CREATE TABLE users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            age INTEGER
        );

        INSERT INTO users (name, age) VALUES ('ExistingUser', 40);
        """
    )

    schema_manager = SchemaManager(db)
    data_loader = DataLoader(db, schema_manager)

    # Because the schema matches, loading the CSV should append rows
    # to the existing table instead of creating a new one.
    data_loader.load_csv(str(csv_file), "users")

    rows = db.execute_select("SELECT name, age FROM users ORDER BY id")

    assert rows == [
        ("ExistingUser", 40),
        ("Alice", 25),
        ("Bob", 30),
    ]

    db.close()


def test_load_csv_skips_insert_when_schema_conflicts_and_user_chooses_skip(tmp_path, monkeypatch):
    """Test that load_csv skips ingestion when schema conflicts and the user chooses skip.

    This test verifies the "skip" path:
    - A table already exists
    - The incoming CSV schema does not match the existing table schema
    - The user chooses "skip"
    - No rows are inserted and the original table remains unchanged
    """
    csv_file = tmp_path / "users.csv"
    csv_file.write_text("Name,Age\nAlice,25\nBob,30\n")

    db = DatabaseManager(":memory:")
    db.connect()

    # Create a conflicting schema on purpose: "users" has only "name",
    # but the incoming CSV has both "name" and "age".
    db.execute_script(
        """
        CREATE TABLE users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT
        );

        INSERT INTO users (name) VALUES ('ExistingUser');
        """
    )

    schema_manager = SchemaManager(db)
    data_loader = DataLoader(db, schema_manager)

    # Simulate user typing "skip" at the conflict prompt.
    monkeypatch.setattr("builtins.input", lambda _: "skip")

    data_loader.load_csv(str(csv_file), "users")

    # The original table should remain unchanged because loading was skipped.
    rows = db.execute_select("SELECT name FROM users ORDER BY id")

    assert rows == [("ExistingUser",)]

    db.close()


def test_load_csv_overwrites_table_when_schema_conflicts_and_user_chooses_overwrite(tmp_path, monkeypatch):
    """Test that load_csv overwrites an existing table when schema conflicts and user chooses overwrite.

    This test verifies the "overwrite" path:
    - A conflicting table already exists
    - The user chooses "overwrite"
    - The old table is dropped
    - A new table is created using the incoming CSV schema
    - New rows are inserted into the recreated table
    """
    csv_file = tmp_path / "users.csv"
    csv_file.write_text("Name,Age\nAlice,25\nBob,30\n")

    db = DatabaseManager(":memory:")
    db.connect()

    # Create a conflicting table that only has one business column.
    db.execute_script(
        """
        CREATE TABLE users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT
        );

        INSERT INTO users (name) VALUES ('OldUser');
        """
    )

    schema_manager = SchemaManager(db)
    data_loader = DataLoader(db, schema_manager)

    # Simulate user typing "overwrite" at the conflict prompt.
    monkeypatch.setattr("builtins.input", lambda _: "overwrite")

    data_loader.load_csv(str(csv_file), "users")

    # After overwrite, the old schema should be replaced by the new CSV schema,
    # and the old row should no longer exist.
    rows = db.execute_select("SELECT name, age FROM users ORDER BY id")

    assert rows == [("Alice", 25), ("Bob", 30)]

    db.close()


def test_load_csv_renames_table_when_schema_conflicts_and_user_chooses_rename(tmp_path, monkeypatch):
    """Test that load_csv creates a new renamed table when schema conflicts and user chooses rename.

    This test verifies the "rename" path:
    - A conflicting table already exists
    - The user chooses "rename"
    - A new non-conflicting table name is generated
    - The new table is created
    - Rows are inserted into the new table
    - The original table is preserved
    """
    csv_file = tmp_path / "users.csv"
    csv_file.write_text("Name,Age\nAlice,25\nBob,30\n")

    db = DatabaseManager(":memory:")
    db.connect()

    # Create the original conflicting table first.
    db.execute_script(
        """
        CREATE TABLE users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT
        );

        INSERT INTO users (name) VALUES ('OriginalUser');
        """
    )

    schema_manager = SchemaManager(db)
    data_loader = DataLoader(db, schema_manager)

    # Simulate user typing "rename" at the conflict prompt.
    monkeypatch.setattr("builtins.input", lambda _: "rename")

    data_loader.load_csv(str(csv_file), "users")

    # The original table should still exist.
    original_rows = db.execute_select("SELECT name FROM users ORDER BY id")
    assert original_rows == [("OriginalUser",)]

    # A new renamed table should also exist.
    tables = db.list_tables()
    assert "users" in tables
    assert "users_1" in tables

    # The incoming CSV data should have been inserted into the renamed table.
    renamed_rows = db.execute_select("SELECT name, age FROM users_1 ORDER BY id")
    assert renamed_rows == [("Alice", 25), ("Bob", 30)]

    db.close()

# 如果是name统一大写开头，后面全小写
# 如果是age，不能有小数点？