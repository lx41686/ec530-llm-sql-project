from dotenv import load_dotenv

from src.cli import CLI
from src.data_loader import DataLoader
from src.db import DatabaseManager
from src.llm_adapter import LLMAdapter
from src.query_service import QueryService
from src.schema_manager import SchemaManager
from src.validator import SQLValidator


def main() -> None:
    """
    create components
    ↓
    connect components
    ↓
    start system
    ↓
    clean up resources
    """
    load_dotenv()  # Load environmental variables from .env, including OPENAI_API_KEY

    # "app.db" if the SQLite file name, create if not exit
    db_manager = DatabaseManager("app.db")  # Create db manager object
    db_manager.connect()  # connect to db

    schema_manager = SchemaManager(db_manager)  # create schema manager, relies on DB manager
    validator = SQLValidator()  # validator doesn't rely on DB
    llm_adapter = LLMAdapter()  # LLMAdapter doesn't rely on DB, and doesn't execute SQL
    data_loader = DataLoader(db_manager, schema_manager)  # CSV → Table → Insert rows
    query_service = QueryService(
        db_manager=db_manager,
        validator=validator,
        schema_manager=schema_manager,
        llm_adapter=llm_adapter,
    )

    # CLI doesn't not connect to DB -> separation of concerns for safety
    cli = CLI(data_loader=data_loader, query_service=query_service)

    try:
        cli.run()  # goes into while loop
    finally:
        db_manager.close()  # exit or error


if __name__ == "__main__":
    main()
