from src.data_loader import DataLoader
from src.query_service import QueryService


class CLI:
    """Command-line interface for interacting with the data system.

    This class is responsible for:
    - displaying the menu
    - collecting user input
    - routing commands to the appropriate service layer
    - displaying results or errors

    It does not directly access the database.
    """
    def __init__(self, data_loader: DataLoader, query_service: QueryService) -> None:
        """Initialize the CLI with its dependencies."""
        self.data_loader = data_loader
        self.query_service = query_service

    def print_menu(self) -> None:
        """Display the main menu options to the user."""
        print("\n=== EC530 LLM SQL Project ===")
        print("1. Load CSV file")
        print("2. List tables")
        print("3. Run SQL query")
        print("4. Run natural language query")
        print("5. Exit")

    def run(self) -> None:
        """Run the main CLI loop.

        This function repeatedly shows the menu, reads the user's choice,
        and dispatches the request to the corresponding handler function.
        The loop continues until the user chooses to exit.
        """
        while True:  # run until user said exit
            self.print_menu()
            choice = input("Enter your choice: ").strip()

            if choice == "1":
                self.handle_load_csv()  # load csv file
            elif choice == "2":
                self.handle_list_tables()  # list tables
            elif choice == "3":
                self.handle_sql_query()  # run SQL query
            elif choice == "4":
                self.handle_natural_language_query()  # Run natural language query
            elif choice == "5":
                print("Goodbye.")
                break
            else:
                print("Invalid choice. Please enter a number from 1 to 5.")

    def handle_load_csv(self) -> None:
        """Handle the CSV loading workflow.

        This function asks the user for a CSV file path and a target table name,
        then delegates the ingestion task to the DataLoader.
        If an error occurs, it catches the exception and prints a user-friendly message.
        """
        file_path = input("Enter CSV file path: ").strip()  # e.g. data/users.csv
        table_name = input("Enter table name: ").strip()  # e.g. users

        try:
            self.data_loader.load_csv(file_path, table_name)
            print(f"Loaded CSV into table '{table_name}'.")
        except Exception as error:
            print(f"Error loading CSV: {error}")

    def handle_list_tables(self) -> None:
        """Handle the table-listing workflow.

        This function requests the list of tables from the QueryService
        and prints them for the user. The printed names are database table names,
        not CSV file names from the data folder.
        """
        try:
            tables = self.query_service.list_tables()

            if not tables:
                print("No tables found.")
                return

            print("Tables:")
            for table in tables:
                print(f"- {table}")
        except Exception as error:
            print(f"Error listing tables: {error}")

    def handle_sql_query(self) -> None:
        """Handle the SQL query workflow.

        This function asks the user to enter a SQL query string,
        then delegates execution to the QueryService.
        The QueryService is responsible for validation and database access.
        """
        sql = input("Enter SQL query: ").strip()  # e.g. SELECT name FROM users;

        try:
            rows = self.query_service.run_sql_query(sql)
            self.print_rows(rows)
        except Exception as error:
            print(f"Error running SQL query: {error}")

    def handle_natural_language_query(self) -> None:
        """Handle the natural language query workflow.

        This function asks the user for a natural language request,
        then sends it to the QueryService. The QueryService uses the LLM adapter
        to generate SQL, validates the generated SQL, executes it, and returns results.
        """
        user_query = input("Enter natural language query: ").strip()

        try:
            sql, rows = self.query_service.run_natural_language_query(user_query)
            print(f"Generated SQL: {sql}")
            self.print_rows(rows)
        except Exception as error:
            print(f"Error running natural language query: {error}")

    def print_rows(self, rows: list[tuple]) -> None:
        """Print query results in a simple row-by-row format.

        Args:
            rows: A list of tuples returned from the database query.

        If the result set is empty, the function informs the user that no results were found.
        """
        if not rows:
            print("No results found.")
            return

        print("Results:")
        for row in rows:
            print(row)
