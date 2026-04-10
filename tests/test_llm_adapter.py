from src.llm_adapter import LLMAdapter, LLMResponse


# These tests verify that the LLMAdapter correctly builds prompts and interacts with an LLM client.
class FakeLLMClient:
    def __init__(self, response: str) -> None:
        self.response = response
        self.last_prompt = None

    def generate(self, prompt: str) -> str:
        self.last_prompt = prompt
        return self.response


def test_build_schema_prompt_includes_tables_and_columns():
    """Test that build_schema_prompt includes all tables and their columns.

    This verifies that the schema context is correctly formatted and embedded
    into the prompt so the LLM can generate accurate SQL queries.
    """
    adapter = LLMAdapter()

    schema_context = {
        "users": ["id", "name", "age"],
        "products": ["id", "title", "price"],
    }

    prompt = adapter.build_schema_prompt(schema_context)

    assert "users (id, name, age)" in prompt
    assert "products (id, title, price)" in prompt


def test_build_prompt_includes_user_query_and_schema():
    """Test that build_prompt includes the user query, schema context, and instructions.

    This ensures that the generated prompt contains all necessary information
    for the LLM to produce correct and safe SQL queries.
    """
    adapter = LLMAdapter()

    schema_context = {
        "users": ["id", "name", "age"],
    }

    prompt = adapter.build_prompt("show all users", schema_context)

    assert "show all users" in prompt
    assert "users (id, name, age)" in prompt
    assert "Only generate ONE SELECT statement" in prompt


def test_generate_sql_returns_llm_response():
    """Test that generate_sql returns the SQL produced by the LLM client.

    This verifies that the adapter correctly wraps the LLM output into an LLMResponse object.
    """
    fake_client = FakeLLMClient("SELECT name FROM users;")
    adapter = LLMAdapter(client=fake_client)

    schema_context = {
        "users": ["id", "name", "age"],
    }

    result = adapter.generate_sql("show all user names", schema_context)

    assert result == LLMResponse(sql="SELECT name FROM users;")


def test_generate_sql_passes_prompt_to_client():
    """Test that generate_sql passes the correct prompt to the LLM client.

    This ensures that the constructed prompt includes the user query and schema context
    before being sent to the LLM.
    """
    fake_client = FakeLLMClient("SELECT * FROM users;")
    adapter = LLMAdapter(client=fake_client)

    schema_context = {
        "users": ["id", "name"],
    }

    adapter.generate_sql("show all users", schema_context)

    assert fake_client.last_prompt is not None
    assert "show all users" in fake_client.last_prompt
    assert "users (id, name)" in fake_client.last_prompt
