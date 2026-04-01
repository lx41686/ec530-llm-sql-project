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
        matches = re.findall(
            r"\b(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*)",
            sql,
            flags=re.IGNORECASE,
        )
        return [match.lower() for match in matches]

    def extract_selected_columns(self, sql: str) -> list[str]:
        """
        Extract selected column names from simple SELECT ... FROM queries.
        Supports basic comma-separated columns, '*', and table.column forms.
        """
        match = re.search(
            r"SELECT\s+(.*?)\s+FROM\s",
            sql,
            flags=re.IGNORECASE | re.DOTALL,
        )

        if match is None:
            return []

        select_part = match.group(1).strip()

        if select_part == "*":
            return ["*"]

        raw_columns = [column.strip() for column in select_part.split(",")]
        normalized_columns = []

        for column in raw_columns:
            if column == "*":
                normalized_columns.append("*")
                continue

            if "." in column:
                column = column.split(".")[-1].strip()

            normalized_columns.append(column.lower())

        return normalized_columns

    def validate_table_names(
        self,
        sql: str,
        schema_context: dict[str, list[str]],
    ) -> ValidationResult:
        table_names = self.extract_table_names(sql)
        known_tables = {table_name.lower() for table_name in schema_context.keys()}

        for table_name in table_names:
            if table_name not in known_tables:
                return ValidationResult(
                    is_valid=False,
                    error_message=f"Unknown table referenced: {table_name}",
                )

        return ValidationResult(is_valid=True)

    def validate_column_names(
        self,
        sql: str,
        schema_context: dict[str, list[str]],
    ) -> ValidationResult:
        selected_columns = self.extract_selected_columns(sql)

        if not selected_columns:
            return ValidationResult(
                is_valid=False,
                error_message="Could not parse selected columns.",
            )

        if "*" in selected_columns:
            return ValidationResult(is_valid=True)

        table_names = self.extract_table_names(sql)

        if not table_names:
            return ValidationResult(
                is_valid=False,
                error_message="Could not determine referenced table.",
            )

        available_columns = set()
        for table_name in table_names:
            table_columns = schema_context.get(table_name, [])
            for column in table_columns:
                available_columns.add(column.lower())

        for column in selected_columns:
            if column not in available_columns:
                return ValidationResult(
                    is_valid=False,
                    error_message=f"Unknown column referenced: {column}",
                )

        return ValidationResult(is_valid=True)

    def validate(
        self,
        sql: str,
        schema_context: dict[str, list[str]],
    ) -> ValidationResult:
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

        column_validation = self.validate_column_names(sql, schema_context)
        if not column_validation.is_valid:
            return column_validation

        return ValidationResult(is_valid=True)
