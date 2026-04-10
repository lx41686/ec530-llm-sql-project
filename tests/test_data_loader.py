import pandas as pd

from src.db import DatabaseManager
from src.schema_manager import SchemaManager
from src.data_loader import DataLoader

# These tests verify that CSV or DataFrame data can be correctly inserted into the database.
# Test DataFrame + Schema + DB
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
    """Test that load_csv performs full data ingestion from CSV to database.

    This test verifies that:
    - A CSV file can be read from disk
    - Schema is correctly inferred from the CSV
    - A new table is created in the database
    - All rows are inserted correctly
    - Data can be queried after ingestion

    An integrated test:
    CSV → DataFrame → Schema → CREATE TABLE → INSERT → DB

    The tmp_path fixture is used to create a temporary CSV file for isolation.
    """
    # temp_path is temporary path provided by pytest -> temporary csv
    csv_file = tmp_path / "students.csv"
    # write csv
    csv_file.write_text("Name,Age,Score\nAlice,25,95.5\nBob,30,88.0\n")

    db = DatabaseManager(":memory:")
    db.connect()

    schema_manager = SchemaManager(db)
    data_loader = DataLoader(db, schema_manager)

    # This function does: read CSV → infer schema → create table → insert rows
    data_loader.load_csv(str(csv_file), "students")

    # Test if table is created
    tables = db.list_tables()
    rows = db.execute_select("SELECT name, age, score FROM students ORDER BY id")

    assert "students" in tables
    assert rows == [("Alice", 25, 95.5), ("Bob", 30, 88.0)]

    db.close()

# 如果是name统一大写开头，后面全小写
# 如果是age，不能有小数点？