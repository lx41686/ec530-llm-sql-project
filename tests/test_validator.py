from src.validator import SQLValidator


def test_is_select_query_returns_true_for_select():
    validator = SQLValidator()
    assert validator.is_select_query("SELECT * FROM users")


def test_is_select_query_returns_false_for_delete():
    validator = SQLValidator()
    assert not validator.is_select_query("DELETE FROM users")
