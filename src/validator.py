class SQLValidator:
    def is_select_query(self, sql: str) -> bool:
        """
        Only allow SELECT
        """
        # Before checking: remove spaces, change to upper letters if not
        return sql.strip().upper().startswith("SELECT")  # sql is an input string
