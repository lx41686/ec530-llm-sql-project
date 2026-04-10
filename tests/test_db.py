from src.db import DatabaseManager


def test_connect_creates_connection():
    """
    To test if connection is successful
    """
    db = DatabaseManager(":memory:")
    db.connect()

    assert db.connection is not None

    db.close()


def test_execute_script_and_list_tables():
    db = DatabaseManager(":memory:")  # create db in memory, no file created
    db.connect()

    # Test if execute_script() can create table
    db.execute_script(
        """
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT
        );
        """
    )

    # Test if list_tables() can read the table
    tables = db.list_tables()

    assert "users" in tables

    db.close()  # release memory


def test_execute_select_returns_rows():
    """
    To test if execute_select() can return the correct results
    """
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
