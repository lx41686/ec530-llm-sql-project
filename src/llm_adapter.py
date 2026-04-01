from dataclasses import dataclass


@dataclass
class LLMResponse:
    sql: str
    explanation: str | None = None


class LLMAdapter:
    def __init__(self, client=None) -> None:
        self.client = client

    def build_schema_prompt(self, schema_context: dict[str, list[str]]) -> str:
        """Build a text description of the database schema for the LLM."""
        lines = ["The database contains the following tables:"]

        for table_name, columns in schema_context.items():
            column_list = ", ".join(columns)
            lines.append(f"- {table_name} ({column_list})")

        return "\n".join(lines)

    def build_prompt(self, user_query: str, schema_context: dict[str, list[str]]) -> str:
        """Build the full prompt for converting natural language to SQL."""
        schema_description = self.build_schema_prompt(schema_context)

        return (
            "You are an AI assistant that converts natural language questions into SQLite SQL queries.\n"
            "Only generate a single SELECT query.\n"
            "Do not generate INSERT, UPDATE, DELETE, DROP, ALTER, or multiple statements.\n\n"
            f"{schema_description}\n\n"
            f'User query: "{user_query}"\n\n'
            "Return only the SQL query."
        )

    def generate_sql(self, user_query: str, schema_context: dict[str, list[str]]) -> LLMResponse:
        """
        Generate SQL from a natural language query.

        For now, this requires a client object with a generate(prompt: str) -> str method.
        """
        if self.client is None:
            raise ValueError("LLM client has not been configured.")

        prompt = self.build_prompt(user_query, schema_context)
        sql = self.client.generate(prompt)

        return LLMResponse(sql=sql.strip())