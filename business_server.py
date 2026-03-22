"""
nlkart-business MCP Server — Business operations for Claude Code.

Exposes tools:
  - get_pending_products() — List products awaiting approval
  - approve_product(product_id, notes) — Approve a product
  - reject_product(product_id, reason) — Reject a product
  - get_user_wallet(user_id) — Check wallet balance
  - get_sales_report(date_from, date_to) — Sales summary
  - run_rating_algorithm() — Trigger rating recalculation
  - run_offer_algorithm() — Trigger offer recalculation
  - send_notification(user_id, title, message) — Send notification
"""

import json
import pyodbc
from mcp.server.fastmcp import FastMCP

server = FastMCP("nlkart-business")

CONNECTION_STRING = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=nlkart_db;"
    "Trusted_Connection=yes;"
)


def get_connection():
    return pyodbc.connect(CONNECTION_STRING)


@server.tool()
def get_pending_products() -> str:
    """List all products awaiting approval (Status = 'Pending')."""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT p.ProductId, p.Name, p.Price, p.Stock, c.CategoryName,
                   u.Username AS DealerName, p.CreatedAt
            FROM Products p
            JOIN Categories c ON p.CategoryId = c.CategoryId
            JOIN Users u ON p.DealerId = u.UserId
            WHERE p.Status = 'Pending'
            ORDER BY p.CreatedAt DESC
        """)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        results = [dict(zip(columns, row)) for row in rows]
        conn.close()
        return json.dumps(results, default=str)
    except Exception as e:
        return json.dumps({"error": str(e)})


@server.tool()
def approve_product(product_id: int, notes: str = "") -> str:
    """Approve a pending product by its ID."""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE Products SET Status = 'Approved' WHERE ProductId = ? AND Status = 'Pending'",
            product_id
        )
        affected = cursor.rowcount
        conn.commit()
        conn.close()
        if affected == 0:
            return json.dumps({"error": "Product not found or not in Pending status"})
        return json.dumps({"success": True, "productId": product_id, "notes": notes})
    except Exception as e:
        return json.dumps({"error": str(e)})


@server.tool()
def reject_product(product_id: int, reason: str) -> str:
    """Reject a pending product by its ID with a reason."""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE Products SET Status = 'Rejected' WHERE ProductId = ? AND Status = 'Pending'",
            product_id
        )
        affected = cursor.rowcount
        conn.commit()
        conn.close()
        if affected == 0:
            return json.dumps({"error": "Product not found or not in Pending status"})
        return json.dumps({"success": True, "productId": product_id, "reason": reason})
    except Exception as e:
        return json.dumps({"error": str(e)})


@server.tool()
def get_user_wallet(user_id: int) -> str:
    """Check wallet balance for a user."""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT UserId, Username, WalletBalance FROM Users WHERE UserId = ?",
            user_id
        )
        row = cursor.fetchone()
        conn.close()
        if not row:
            return json.dumps({"error": "User not found"})
        return json.dumps({
            "userId": row.UserId,
            "username": row.Username,
            "walletBalance": float(row.WalletBalance)
        })
    except Exception as e:
        return json.dumps({"error": str(e)})


@server.tool()
def get_sales_report(date_from: str, date_to: str) -> str:
    """Get sales summary between two dates (YYYY-MM-DD format)."""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                COUNT(DISTINCT o.OrderId) AS TotalOrders,
                SUM(oi.Quantity * oi.UnitPrice) AS TotalRevenue,
                COUNT(DISTINCT o.UserId) AS UniqueCustomers,
                AVG(oi.Quantity * oi.UnitPrice) AS AvgOrderValue
            FROM Orders o
            JOIN OrderItems oi ON o.OrderId = oi.OrderId
            WHERE o.CreatedAt BETWEEN ? AND ?
        """, date_from, date_to)
        row = cursor.fetchone()
        conn.close()
        return json.dumps({
            "dateFrom": date_from,
            "dateTo": date_to,
            "totalOrders": row.TotalOrders or 0,
            "totalRevenue": float(row.TotalRevenue or 0),
            "uniqueCustomers": row.UniqueCustomers or 0,
            "avgOrderValue": float(row.AvgOrderValue or 0)
        })
    except Exception as e:
        return json.dumps({"error": str(e)})


@server.tool()
def run_rating_algorithm() -> str:
    """Trigger Bayesian weighted rating recalculation for all products."""
    try:
        import subprocess
        result = subprocess.run(
            ["python", "../nlkart-utils/rating_algorithm.py"],
            capture_output=True, text=True, timeout=60
        )
        return result.stdout if result.returncode == 0 else json.dumps({"error": result.stderr})
    except Exception as e:
        return json.dumps({"error": str(e)})


@server.tool()
def run_offer_algorithm() -> str:
    """Trigger offer matching algorithm for all products."""
    try:
        import subprocess
        result = subprocess.run(
            ["python", "../nlkart-utils/offer_algorithm.py"],
            capture_output=True, text=True, timeout=60
        )
        return result.stdout if result.returncode == 0 else json.dumps({"error": result.stderr})
    except Exception as e:
        return json.dumps({"error": str(e)})


@server.tool()
def send_notification(user_id: int, title: str, message: str) -> str:
    """Send a notification to a specific user."""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO Notifications (UserId, Title, Message, IsRead) VALUES (?, ?, ?, 0)",
            user_id, title, message
        )
        conn.commit()
        notification_id = cursor.execute("SELECT @@IDENTITY").fetchone()[0]
        conn.close()
        return json.dumps({
            "success": True,
            "notificationId": int(notification_id),
            "userId": user_id,
            "title": title
        })
    except Exception as e:
        return json.dumps({"error": str(e)})


if __name__ == "__main__":
    server.run(transport="stdio")
