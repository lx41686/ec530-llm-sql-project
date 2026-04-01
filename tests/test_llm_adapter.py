from src.llm_adapter import LLMAdapter, LLMResponse


class FakeLLMClient:
    def __init__(self, response: str) -> None:
        self.response = response
        self.last_prompt = None

    def generate(self, prompt: str) -> str:
        self.last_prompt = prompt
        return self.response


def test_build_schema_prompt_includes_tables_and_columns():
    adapter = LLMAdapter()

    schema_context = {
        "users": ["id", "name", "age"],
        "products": ["id", "title", "price"],
    }

    prompt = adapter.build_schema_prompt(schema_context)

    assert "users (id, name, age)" in prompt
    assert "products (id, title, price)" in prompt


def test_build_prompt_includes_user_query_and_schema():
    adapter = LLMAdapter()

    schema_context = {
        "users": ["id", "name", "age"],
    }

    prompt = adapter.build_prompt("show all users", schema_context)

    assert "show all users" in prompt
    assert "users (id, name, age)" in prompt
    assert "Only generate a single SELECT query." in prompt


def test_generate_sql_returns_llm_response():
    fake_client = FakeLLMClient("SELECT name FROM users;")
    adapter = LLMAdapter(client=fake_client)

    schema_context = {
        "users": ["id", "name", "age"],
    }

    result = adapter.generate_sql("show all user names", schema_context)

    assert result == LLMResponse(sql="SELECT name FROM users;")


def test_generate_sql_passes_prompt_to_client():
    fake_client = FakeLLMClient("SELECT * FROM users;")
    adapter = LLMAdapter(client=fake_client)

    schema_context = {
        "users": ["id", "name"],
    }

    adapter.generate_sql("show all users", schema_context)

    assert fake_client.last_prompt is not None
    assert "show all users" in fake_client.last_prompt
    assert "users (id, name)" in fake_client.last_prompt
