from src.db import DatabaseManager
from src.validator import SQLValidator


class QueryService:
    def __init__(self, db_manager: DatabaseManager, validator: SQLValidator) -> None:
        # need db and validator
        self.db_manager = db_manager
        self.validator = validator

    def run_sql_query(self, sql: str) -> list[tuple]:
        """Validate a SQL query and execute it if valid."""
        # validate first
        validation_result = self.validator.validate(sql)

        if not validation_result.is_valid:
            raise ValueError(validation_result.error_message)

        return self.db_manager.execute_select(sql)

    def list_tables(self) -> list[str]:
        """Return the names of all tables in the database."""
        return self.db_manager.list_tables()
