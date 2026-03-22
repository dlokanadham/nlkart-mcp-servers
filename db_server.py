"""
nlkart-db MCP Server — Read-only database access for Claude Code.

Exposes tools:
  - query_db(sql) — Execute read-only SQL, return JSON results
  - get_table_schema(table_name) — Column definitions for a table
  - get_table_stats() — Row counts for all tables
"""

import json
import pyodbc
from mcp.server.fastmcp import FastMCP

server = FastMCP("nlkart-db")

CONNECTION_STRING = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=nlkart_db;"
    "Trusted_Connection=yes;"
)


def get_connection():
    return pyodbc.connect(CONNECTION_STRING)


@server.tool()
def query_db(sql: str) -> str:
    """Execute a read-only SQL query against the nlkart database. Returns JSON results."""
    blocked = ["INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE", "TRUNCATE", "EXEC"]
    sql_upper = sql.upper().strip()
    for keyword in blocked:
        if sql_upper.startswith(keyword):
            return json.dumps({"error": f"Blocked: {keyword} statements are not allowed. This is a read-only server."})

    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(sql)
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        rows = cursor.fetchall()
        results = [dict(zip(columns, row)) for row in rows]
        conn.close()
        return json.dumps(results, default=str)
    except Exception as e:
        return json.dumps({"error": str(e)})


@server.tool()
def get_table_schema(table_name: str) -> str:
    """Get column definitions for a specific table in the nlkart database."""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                c.COLUMN_NAME,
                c.DATA_TYPE,
                c.CHARACTER_MAXIMUM_LENGTH,
                c.IS_NULLABLE,
                CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 'YES' ELSE 'NO' END AS IS_PRIMARY_KEY
            FROM INFORMATION_SCHEMA.COLUMNS c
            LEFT JOIN (
                SELECT ku.COLUMN_NAME
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
                    ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
                WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                    AND tc.TABLE_NAME = ?
            ) pk ON c.COLUMN_NAME = pk.COLUMN_NAME
            WHERE c.TABLE_NAME = ?
            ORDER BY c.ORDINAL_POSITION
        """, table_name, table_name)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        results = [dict(zip(columns, row)) for row in rows]
        conn.close()
        return json.dumps(results, default=str)
    except Exception as e:
        return json.dumps({"error": str(e)})


@server.tool()
def get_table_stats() -> str:
    """Get row counts for all tables in the nlkart database."""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                t.NAME AS TableName,
                p.rows AS RowCount
            FROM sys.tables t
            JOIN sys.partitions p ON t.object_id = p.object_id
            WHERE p.index_id IN (0, 1)
            ORDER BY t.NAME
        """)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        results = [dict(zip(columns, row)) for row in rows]
        conn.close()
        return json.dumps(results, default=str)
    except Exception as e:
        return json.dumps({"error": str(e)})


if __name__ == "__main__":
    server.run(transport="stdio")
