import duckdb


def find_by_language(con: duckdb.DuckDBPyConnection, language: str) -> list[dict]:
    """Find profiles by programming language

    Args:
        con: DuckDB connection
        language: Programming language to search for (e.g., 'Python', 'Rust')

    Returns:
        List of profiles using that language
    """
    sql = """
        SELECT name, bio, html_url
        FROM github_profiles
        WHERE LIST_CONTAINS(languages, ?)
        ORDER BY followers DESC;
    """
    return con.execute(sql, [language]).fetchall()


def get_all_users(con: duckdb.DuckDBPyConnection) -> set[str]:
    """Get all logins from the database
    Args:
        con: DuckDB connection
    Returns:
        Set of all logins
    """
    sql = """
        SELECT login
        FROM github_profiles
        ORDER BY followers DESC;
    """
    # Extract first element from each tuple and convert to set
    return {row[0] for row in con.execute(sql).fetchall()}
