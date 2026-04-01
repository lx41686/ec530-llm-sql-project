from dataclasses import dataclass
import os

from openai import OpenAI


@dataclass
class LLMResponse:
    sql: str
    explanation: str | None = None


class LLMAdapter:
    def __init__(self, client: OpenAI | None = None, model: str | None = None) -> None:
        self.client = client or OpenAI()
        self.model = model or os.getenv("OPENAI_MODEL", "gpt-5.2")

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
            "Do not generate INSERT, UPDATE, DELETE, DROP, ALTER, or multiple statements.\n"
            "Use only table names and column names that exist in the provided schema.\n"
            "Return only the SQL query, with no markdown and no explanation.\n\n"
            f"{schema_description}\n\n"
            f'User query: "{user_query}"'
        )

    def generate_sql(self, user_query: str, schema_context: dict[str, list[str]]) -> LLMResponse:
        """Generate SQL from a natural language query using the OpenAI Responses API."""
        prompt = self.build_prompt(user_query, schema_context)

        response = self.client.responses.create(
            model=self.model,
            input=prompt,
        )

        sql = response.output_text.strip()

        return LLMResponse(sql=sql)
