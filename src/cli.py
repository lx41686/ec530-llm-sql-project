from src.data_loader import DataLoader
from src.query_service import QueryService
# only connect to loader and QS, no direct connection with DB


class CLI:
    def __init__(self, data_loader: DataLoader, query_service: QueryService) -> None:
        self.data_loader = data_loader
        self.query_service = query_service

    def print_menu(self) -> None:
        print("\n=== EC530 LLM SQL Project ===")
        print("1. Load CSV file")
        print("2. List tables")
        print("3. Run SQL query")
        print("4. Run natural language query")
        print("5. Exit")

    def run(self) -> None:
        while True:
            self.print_menu()
            choice = input("Enter your choice: ").strip()

            if choice == "1":
                self.handle_load_csv()
            elif choice == "2":
                self.handle_list_tables()
            elif choice == "3":
                self.handle_sql_query()
            elif choice == "4":
                self.handle_natural_language_query()
            elif choice == "5":
                print("Goodbye.")
                break
            else:
                print("Invalid choice. Please enter a number from 1 to 5.")

    def handle_load_csv(self) -> None:
        file_path = input("Enter CSV file path: ").strip()
        table_name = input("Enter table name: ").strip()

        try:
            self.data_loader.load_csv(file_path, table_name)
            print(f"Loaded CSV into table '{table_name}'.")
        except Exception as error:
            print(f"Error loading CSV: {error}")

    def handle_list_tables(self) -> None:
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
        sql = input("Enter SQL query: ").strip()

        try:
            rows = self.query_service.run_sql_query(sql)
            self.print_rows(rows)
        except Exception as error:
            print(f"Error running SQL query: {error}")

    def handle_natural_language_query(self) -> None:
        user_query = input("Enter natural language query: ").strip()

        try:
            sql, rows = self.query_service.run_natural_language_query(user_query)
            print(f"Generated SQL: {sql}")
            self.print_rows(rows)
        except Exception as error:
            print(f"Error running natural language query: {error}")

    def print_rows(self, rows: list[tuple]) -> None:
        if not rows:
            print("No results found.")
            return

        print("Results:")
        for row in rows:
            print(row)
