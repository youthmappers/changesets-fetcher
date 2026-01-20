"""Shared utilities for query loading."""

from pathlib import Path


def load_query_from_path(query_path: str, sql_dir: str = "sql") -> str:
    """Load a SQL query from a file path or a named block.

    Supports:
      - "filename.sql" (entire file)
      - "filename.sql:query-name" (block between -- BEGIN name and --END)

    Args:
        query_path: Query path string.
        sql_dir: Default directory for relative SQL files.

    Returns:
        SQL query string.

    Raises:
        FileNotFoundError: If the SQL file cannot be found.
        KeyError: If the named query block cannot be found.
        ValueError: If the query_path is invalid.
    """
    if ":" not in query_path:
        filename = query_path
        query_name = None
    else:
        filename, query_name = query_path.split(":", 1)

    sql_file = Path(filename)
    if not sql_file.exists() and not sql_file.is_absolute():
        sql_file = Path(sql_dir) / filename

    if not sql_file.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_file}")

    content = sql_file.read_text()
    if not query_name:
        return content.strip()

    begin_markers = {f"-- BEGIN {query_name}", f"--BEGIN {query_name}"}

    in_block = False
    block_lines: list[str] = []

    for line in content.split("\n"):
        stripped = line.strip()

        if stripped in begin_markers:
            in_block = True
            continue

        if in_block and (stripped.startswith("--END") or stripped.startswith("-- END")):
            break

        if in_block:
            block_lines.append(line)

    query_sql = "\n".join(block_lines).strip()
    if not query_sql:
        raise KeyError(
            f"Query '{query_name}' not found in {sql_file}. "
            f"Expected a block starting with '-- BEGIN {query_name}'"
        )

    return query_sql
