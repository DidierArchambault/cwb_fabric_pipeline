from __future__ import annotations
from pathlib import Path
import duckdb
import re

_VALID_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


# -------- Helper functions for error reporting --------
def _extract_line_from_duckdb_error(msg: str) -> int | None:
    m = re.search(r"\bLINE\s+(\d+)\b", msg)
    return int(m.group(1)) if m else None

def _print_context(sql_text: str, line: int, context: int = 5) -> None:
    """
    Prints context lines around a specific line number in the given SQL text.
    :param sql_text: The full SQL text.
    :param line: The line number to center the context around.
    :param context: Number of lines of context to show before and after the line.
    :return: None
    """
    lines = sql_text.splitlines()
    i = max(1, line - context)
    j = min(len(lines), line + context)
    for n in range(i, j + 1):
        prefix = ">>" if n == line else "  "
        print(f"{prefix} {n:4d} | {lines[n-1]}")


# -------- Main functions for DuckDB Activities --------
def register_parquet_views(con: duckdb.DuckDBPyConnection, parquet_root: Path, schema: str = "bronze") -> list[str]:
    """
    Registers parquet files as views in DuckDB. Parquets are searched recursively
    under the specified root directory and can then be queried as tables under the given schema.
    :param con: DuckDB connection.
    :param parquet_root: Path to the root directory containing parquet files.
    :param schema: Schema name to register the views under.
    :return: List of created view names.
    """
    parquet_root = parquet_root.resolve() # make sure we have absolute path
    con.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}";')

    files = sorted(parquet_root.rglob("*.parquet")) # make sure order is consistent
    if files == []:
        raise ValueError(f"Warning: no parquet files found in {parquet_root}")

    created = []
    for f in files:
        table = f.stem # remove extension
        path = f.as_posix() # duckdb needs forward slashes

        con.execute(f"""
            CREATE OR REPLACE VIEW "{schema}".{table} AS
            SELECT * FROM read_parquet('{path}');
        """)
        created.append(table)
        # '{path}' is quoted by the f-string automatically for path safety

    return created


def run_sql_dir(con: duckdb.DuckDBPyConnection, sql_dir: Path) -> None:
    """
    Executes all .sql files found recursively under the specified directory.
    :param con: DuckDB connection.
    :param sql_dir: Path to the directory containing .sql files.
    :return: None
    """
    sql_files = sorted(sql_dir.rglob("*.sql"))
    if not sql_files:
        raise RuntimeError(f"No .sql files found in: {sql_dir}")

    for p in sql_files:
        sql_text = p.read_text(encoding="utf-8")
        print(f"[SQL] executing: {p}")

        try:
            con.execute(sql_text)
        except duckdb.Error as e:
            msg = str(e)
            print(f"[SQL] FAILED in file: {p}")
            line = _extract_line_from_duckdb_error(msg)
            if line is not None:
                print(f"[SQL] Context around LINE {line}:")
                _print_context(sql_text, line, context=6)
            raise


def run_sql_script(con: duckdb.DuckDBPyConnection, sql_file: Path) -> None:
    """
    Executes a single .sql files specified in the config file.
    :param con: DuckDB connection.
    :param sql_file: Path to the .sql file.
    :return: None
    """
    sql_text = sql_file.read_text(encoding="utf-8")
    print(f"[SQL] executing: {sql_file}")
    try:
        con.execute(sql_text)
    except duckdb.Error as e:
        msg = str(e)
        print(f"[SQL] FAILED for file: {sql_file}")
        line = _extract_line_from_duckdb_error(msg)
        if line is not None:
            print(f"[SQL] Context around LINE {line}:")
            _print_context(sql_text, line, context=6)
        raise


def export_all_tables_to_parquet(con: duckdb.DuckDBPyConnection, out_dir: Path) -> None:
    """
    Exports all tables in the main schema of the DuckDB connection to parquet files
    under the specified output directory.
    :param con: DuckDB connection.
    :param out_dir: Path to the output directory.
    :return: None
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    rows = con.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'main'
          AND table_type = 'BASE TABLE'
    """).fetchall()

    for (t,) in rows:
        con.execute(f"""
            COPY (SELECT * FROM "{t}")
            TO '{(out_dir / f"{t}.parquet").as_posix()}'
            (FORMAT PARQUET);
        """)