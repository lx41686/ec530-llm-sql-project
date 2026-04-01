from dataclasses import dataclass


@dataclass
class ValidationResult:
    is_valid: bool
    error_message: str | None = None


class SQLValidator:
    def is_select_query(self, sql: str) -> bool:
        return sql.strip().upper().startswith("SELECT")

    def has_single_statement(self, sql: str) -> bool:
        stripped_sql = sql.strip()

        if not stripped_sql:
            return False

        semicolon_count = stripped_sql.count(";")

        if semicolon_count == 0:
            return True

        if semicolon_count == 1 and stripped_sql.endswith(";"):
            return True

        return False

    def validate(self, sql: str) -> ValidationResult:
        if not self.is_select_query(sql):
            return ValidationResult(
                is_valid=False,
                error_message="Only SELECT queries are allowed.",
            )

        if not self.has_single_statement(sql):
            return ValidationResult(
                is_valid=False,
                error_message="Only a single SQL statement is allowed.",
            )

        return ValidationResult(is_valid=True)
