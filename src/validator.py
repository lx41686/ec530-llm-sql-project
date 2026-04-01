from dataclasses import dataclass


@dataclass
class ValidationResult:
    is_valid: bool
    error_message: str | None = None


class SQLValidator:
    def is_select_query(self, sql: str) -> bool:
        return sql.strip().upper().startswith("SELECT")

    def validate(self, sql: str) -> ValidationResult:
        if not self.is_select_query(sql):
            return ValidationResult(
                is_valid=False,
                error_message="Only SELECT queries are allowed.",
            )

        return ValidationResult(is_valid=True)