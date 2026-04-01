import pandas as pd

from src.db import DatabaseManager
from src.schema_manager import SchemaManager, TableSchema, ColumnSchema


def test_normalize_column_name():
    db = DatabaseManager(":memory:")
    schema_manager = SchemaManager(db)

    normalized = schema_manager.normalize_column_name(" User Name ")

    assert normalized == "user_name"


def test_infer_schema_from_dataframe():
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
    