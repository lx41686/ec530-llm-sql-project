from dotenv import load_dotenv

from src.cli import CLI
from src.data_loader import DataLoader
from src.db import DatabaseManager
from src.llm_adapter import LLMAdapter
from src.query_service import QueryService
from src.schema_manager import SchemaManager
from src.validator import SQLValidator


def main() -> None:
    load_dotenv()

    db_manager = DatabaseManager("app.db")
    db_manager.connect()

    schema_manager = SchemaManager(db_manager)
    validator = SQLValidator()
    llm_adapter = LLMAdapter()
    data_loader = DataLoader(db_manager, schema_manager)
    query_service = QueryService(
        db_manager=db_manager,
        validator=validator,
        schema_manager=schema_manager,
        llm_adapter=llm_adapter,
    )

    cli = CLI(data_loader=data_loader, query_service=query_service)

    try:
        cli.run()
    finally:
        db_manager.close()


if __name__ == "__main__":
    main()
