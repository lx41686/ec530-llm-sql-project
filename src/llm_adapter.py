from dataclasses import dataclass
import os

from openai import OpenAI


@dataclass
class LLMResponse:
    sql: str
    explanation: str | None = None


class LLMAdapter:
    def __init__(self, client=None, model: str | None = None) -> None:
        self.client = client
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
            "Rules:\n"
            "- Only generate ONE SELECT statement\n"
            "- When comparing text values, use LOWER(column) = LOWER(value) for case-insensitive matching\n"
            "- Use only provided table and column names\n"
            "- Do NOT hallucinate tables or columns\n"
            "- Do NOT include explanation\n"
            "- Output ONLY SQL\n\n"
            f"{schema_description}\n\n"
            f'User query: "{user_query}"'
        )

    def _get_client(self):
        """Lazily create the real OpenAI client only when needed."""
        if self.client is None:
            self.client = OpenAI()
        return self.client

    def generate_sql(self, user_query: str, schema_context: dict[str, list[str]]) -> LLMResponse:
        """Generate SQL from a natural language query."""
        prompt = self.build_prompt(user_query, schema_context)
        client = self._get_client()

        # Support fake test clients with a simple generate(prompt) method.
        if hasattr(client, "generate"):
            sql = client.generate(prompt).strip()
        else:
            response = client.responses.create(
                model=self.model,
                input=prompt,
            )
            sql = response.output_text.strip()

        if sql.startswith("```"):
            sql = sql.replace("```sql", "").replace("```", "").strip()

        return LLMResponse(sql=sql)
