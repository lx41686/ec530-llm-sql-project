from dataclasses import dataclass
import re


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

    def extract_table_names(self, sql: str) -> list[str]:
        """
        Extract table names from simple FROM and JOIN clauses.
        This is a lightweight rule-based approach, not a full SQL parser.
        """
        matches = re.findall(r"\b(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*)", sql, flags=re.IGNORECASE)
        return [match.lower() for match in matches]

    def validate_table_names(self, sql: str, schema_context: dict[str, list[str]]) -> ValidationResult:
        table_names = self.extract_table_names(sql)
        known_tables = {table_name.lower() for table_name in schema_context.keys()}

        for table_name in table_names:
            if table_name not in known_tables:
                return ValidationResult(
                    is_valid=False,
                    error_message=f"Unknown table referenced: {table_name}",
                )

        return ValidationResult(is_valid=True)

    def validate(self, sql: str, schema_context: dict[str, list[str]]) -> ValidationResult:
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

        table_validation = self.validate_table_names(sql, schema_context)
        if not table_validation.is_valid:
            return table_validation

        return ValidationResult(is_valid=True)
