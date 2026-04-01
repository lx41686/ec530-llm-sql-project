import pandas as pd

from src.db import DatabaseManager
from src.schema_manager import SchemaManager
from src.data_loader import DataLoader


def test_insert_rows_inserts_dataframe_data():
    db = DatabaseManager(":memory:")
    db.connect()

    schema_manager = SchemaManager(db)
    data_loader = DataLoader(db, schema_manager)

    db.execute_script(
        """
        CREATE TABLE users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            age INTEGER
        );
        """
    )

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
    # temp_path is temporary path provided by pytest -> temporary csv
    csv_file = tmp_path / "students.csv"
    csv_file.write_text("Name,Age,Score\nAlice,25,95.5\nBob,30,88.0\n")

    db = DatabaseManager(":memory:")
    db.connect()

    schema_manager = SchemaManager(db)
    data_loader = DataLoader(db, schema_manager)

    data_loader.load_csv(str(csv_file), "students")

    tables = db.list_tables()
    rows = db.execute_select("SELECT name, age, score FROM students ORDER BY id")

    assert "students" in tables
    assert rows == [("Alice", 25, 95.5), ("Bob", 30, 88.0)]

    db.close()
