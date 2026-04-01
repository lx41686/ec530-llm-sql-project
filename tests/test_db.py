from src.db import DatabaseManager


def test_connect_creates_connection():
    db = DatabaseManager(":memory:")
    db.connect()

    assert db.connection is not None

    db.close()


def test_execute_script_and_list_tables():
    db = DatabaseManager(":memory:")
    db.connect()

    db.execute_script(
        """
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT
        );
        """
    )

    tables = db.list_tables()

    assert "users" in tables

    db.close()


def test_execute_select_returns_rows():
    db = DatabaseManager(":memory:")
    db.connect()

    db.execute_script(
        """
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT
        );

        INSERT INTO users (name) VALUES ('Alice');
        INSERT INTO users (name) VALUES ('Bob');
        """
    )

    rows = db.execute_select("SELECT name FROM users ORDER BY id")

    assert rows == [("Alice",), ("Bob",)]

    db.close()
    