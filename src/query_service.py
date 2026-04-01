from src.db import DatabaseManager
from src.llm_adapter import LLMAdapter
from src.schema_manager import SchemaManager
from src.validator import SQLValidator


class QueryService:
    def __init__(
        self,
        db_manager: DatabaseManager,
        validator: SQLValidator,
        schema_manager: SchemaManager,
        llm_adapter: LLMAdapter | None = None,
    ) -> None:
        self.db_manager = db_manager
        self.validator = validator
        self.schema_manager = schema_manager
        self.llm_adapter = llm_adapter

    def build_schema_context(self) -> dict[str, list[str]]:
        """Build a schema context mapping table names to column names."""
        schema_context = {}

        for table_name in self.db_manager.list_tables():
            schema = self.schema_manager.get_existing_schema(table_name)

            if schema is not None:
                schema_context[table_name] = [column.name for column in schema.columns]

        return schema_context

    def run_sql_query(self, sql: str) -> list[tuple]:
        """Validate a SQL query and execute it if valid."""
        schema_context = self.build_schema_context()
        validation_result = self.validator.validate(sql, schema_context)

        if not validation_result.is_valid:
            raise ValueError(validation_result.error_message)

        return self.db_manager.execute_select(sql)

    def run_natural_language_query(self, user_query: str) -> tuple[str, list[tuple]]:
        """Generate SQL from a natural language query, validate it, and execute it."""
        if self.llm_adapter is None:
            raise ValueError("LLM adapter has not been configured.")

        schema_context = self.build_schema_context()
        llm_response = self.llm_adapter.generate_sql(user_query, schema_context)

        validation_result = self.validator.validate(llm_response.sql, schema_context)

        if not validation_result.is_valid:
            raise ValueError(validation_result.error_message)

        rows = self.db_manager.execute_select(llm_response.sql)
        return llm_response.sql, rows

    def list_tables(self) -> list[str]:
        """Return the names of all tables in the database."""
        return self.db_manager.list_tables()
