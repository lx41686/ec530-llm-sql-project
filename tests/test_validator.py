from src.validator import SQLValidator, ValidationResult


def test_is_select_query_returns_true_for_select():
    validator = SQLValidator()
    assert validator.is_select_query("SELECT * FROM users")


def test_is_select_query_returns_false_for_delete():
    validator = SQLValidator()
    assert not validator.is_select_query("DELETE FROM users")


def test_has_single_statement_returns_true_without_semicolon():
    validator = SQLValidator()
    assert validator.has_single_statement("SELECT * FROM users")


def test_has_single_statement_returns_true_with_trailing_semicolon():
    validator = SQLValidator()
    assert validator.has_single_statement("SELECT * FROM users;")


def test_has_single_statement_returns_false_for_multiple_statements():
    validator = SQLValidator()
    assert not validator.has_single_statement("SELECT * FROM users; DROP TABLE users;")


def test_extract_table_names_returns_table_from_from_clause():
    validator = SQLValidator()

    tables = validator.extract_table_names("SELECT * FROM users")

    assert tables == ["users"]


def test_extract_table_names_returns_tables_from_join_clause():
    validator = SQLValidator()

    tables = validator.extract_table_names(
        "SELECT * FROM users JOIN orders ON users.id = orders.user_id"
    )

    assert tables == ["users", "orders"]


def test_extract_selected_columns_returns_star():
    validator = SQLValidator()

    columns = validator.extract_selected_columns("SELECT * FROM users")

    assert columns == ["*"]


def test_extract_selected_columns_returns_simple_column_names():
    validator = SQLValidator()

    columns = validator.extract_selected_columns("SELECT name, age FROM users")

    assert columns == ["name", "age"]


def test_extract_selected_columns_handles_table_prefixed_columns():
    validator = SQLValidator()

    columns = validator.extract_selected_columns("SELECT users.name, users.age FROM users")

    assert columns == ["name", "age"]


def test_validate_returns_invalid_for_non_select_query():
    validator = SQLValidator()

    result = validator.validate("DELETE FROM users", {"users": ["id", "name"]})

    assert result == ValidationResult(
        is_valid=False,
        error_message="Only SELECT queries are allowed.",
    )


def test_validate_returns_invalid_for_multiple_statements():
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
    validator = SQLValidator()

    result = validator.validate(
        "SELECT name, age FROM users",
        {"users": ["id", "name", "age"]},
    )

    assert result == ValidationResult(is_valid=True)


def test_validate_returns_valid_for_select_star():
    validator = SQLValidator()

    result = validator.validate(
        "SELECT * FROM users",
        {"users": ["id", "name", "age"]},
    )

    assert result == ValidationResult(is_valid=True)
