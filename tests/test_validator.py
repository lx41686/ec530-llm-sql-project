from src.validator import SQLValidator, ValidationResult


# The validator ensures that only safe and valid SQL queries are executed.
def test_is_select_query_returns_true_for_select():
    """Test that is_select_query returns True for valid SELECT statements.

    This verifies that queries starting with SELECT are recognized as valid read-only queries.
    """
    validator = SQLValidator()
    assert validator.is_select_query("SELECT * FROM users")


def test_is_select_query_returns_false_for_delete():
    """Test that is_select_query returns False for non-SELECT statements.

    This ensures that destructive queries like DELETE are rejected.
    """
    validator = SQLValidator()
    assert not validator.is_select_query("DELETE FROM users")


def test_has_single_statement_returns_true_without_semicolon():
    """Test that has_single_statement returns True for a single query without semicolon.

    This verifies that a basic single SQL statement is accepted.
    """
    validator = SQLValidator()
    assert validator.has_single_statement("SELECT * FROM users")


def test_has_single_statement_returns_true_with_trailing_semicolon():
    """Test that has_single_statement returns True for a single query with a trailing semicolon.

    This ensures that a valid SQL statement ending with a semicolon is still accepted.
    """
    validator = SQLValidator()
    assert validator.has_single_statement("SELECT * FROM users;")


def test_has_single_statement_returns_false_for_multiple_statements():
    """Test that has_single_statement returns False for multiple SQL statements.

    This prevents execution of chained queries such as SQL injection attempts.
    """
    validator = SQLValidator()
    assert not validator.has_single_statement("SELECT * FROM users; DROP TABLE users;")


# 表名解析
def test_extract_table_names_returns_table_from_from_clause():
    """Test that extract_table_names correctly identifies tables in FROM clause.

    This ensures that the validator can detect which table is being queried.
    """
    validator = SQLValidator()

    tables = validator.extract_table_names("SELECT * FROM users")

    assert tables == ["users"]


def test_extract_table_names_returns_tables_from_join_clause():
    """Test that extract_table_names extracts multiple tables from JOIN clauses.

    This verifies that queries involving joins correctly identify all referenced tables.
    """
    validator = SQLValidator()

    tables = validator.extract_table_names(
        "SELECT * FROM users JOIN orders ON users.id = orders.user_id"
    )

    assert tables == ["users", "orders"]


def test_extract_selected_columns_returns_star():
    """Test that extract_selected_columns returns '*' for SELECT * queries.

    This ensures that selecting all columns is correctly handled.
    """
    validator = SQLValidator()

    columns = validator.extract_selected_columns("SELECT * FROM users")

    assert columns == ["*"]


def test_extract_selected_columns_returns_simple_column_names():
    """Test that extract_selected_columns returns column names for simple SELECT queries.

    This verifies that individual column names are extracted correctly.
    """
    validator = SQLValidator()

    columns = validator.extract_selected_columns("SELECT name, age FROM users")

    assert columns == ["name", "age"]


def test_extract_selected_columns_handles_table_prefixed_columns():
    """Test that extract_selected_columns handles table-prefixed column names.

    This ensures that prefixes like 'users.name' are correctly reduced to 'name'.
    """
    validator = SQLValidator()

    columns = validator.extract_selected_columns("SELECT users.name, users.age FROM users")

    assert columns == ["name", "age"]


def test_validate_returns_invalid_for_non_select_query():
    """Test that validate rejects non-SELECT queries.

    This ensures that only read-only queries are allowed for safety.
    """
    validator = SQLValidator()

    result = validator.validate("DELETE FROM users", {"users": ["id", "name"]})

    assert result == ValidationResult(
        is_valid=False,
        error_message="Only SELECT queries are allowed.",
    )


def test_validate_returns_invalid_for_multiple_statements():
    """Test that validate rejects queries with multiple statements.

    This prevents execution of potentially dangerous chained SQL commands.
    """
    validator = SQLValidator()

    result = validator.validate(
        "SELECT * FROM users; DROP TABLE users;",
        {"users": ["id", "name"]},
    )

    assert result == ValidationResult(
        is_valid=False,
        error_message="Only a single SQL statement is allowed.",
    )


def test_validate_returns_invalid_for_unknown_table():
    """Test that validate rejects queries referencing unknown tables.

    This ensures queries only access tables present in the schema context.
    """
    validator = SQLValidator()

    result = validator.validate(
        "SELECT * FROM orders",
        {"users": ["id", "name"]},
    )

    assert result == ValidationResult(
        is_valid=False,
        error_message="Unknown table referenced: orders",
    )


def test_validate_returns_invalid_for_unknown_column():
    """Test that validate rejects queries referencing unknown columns.

    This prevents invalid or hallucinated column access.
    """
    validator = SQLValidator()

    result = validator.validate(
        "SELECT salary FROM users",
        {"users": ["id", "name", "age"]},
    )

    assert result == ValidationResult(
        is_valid=False,
        error_message="Unknown column referenced: salary",
    )


def test_validate_returns_valid_for_known_column_selection():
    """Test that validate accepts queries with valid column references.

    This verifies that correct queries pass validation.
    """
    validator = SQLValidator()

    result = validator.validate(
        "SELECT name, age FROM users",
        {"users": ["id", "name", "age"]},
    )

    assert result == ValidationResult(is_valid=True)


def test_validate_returns_valid_for_select_star():
    """Test that validate accepts SELECT * queries.

    This ensures selecting all columns is allowed.
    """
    validator = SQLValidator()

    result = validator.validate(
        "SELECT * FROM users",
        {"users": ["id", "name", "age"]},
    )

    assert result == ValidationResult(is_valid=True)
