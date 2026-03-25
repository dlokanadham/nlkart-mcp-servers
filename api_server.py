"""
nlkart-api-test MCP Server — API integration testing for Claude Code.

Exposes tools:
  - call_api(method, path, body, username, password) — Call any nlkart-api endpoint
  - login_as(role) — Login as a specific role user
  - health_check() — Verify API is running
"""

import json
import base64
import urllib.request
import urllib.error
from mcp.server.fastmcp import FastMCP

server = FastMCP("nlkart-api-test")

API_BASE = "http://localhost:8007"

# Pre-configured test users for each role
TEST_USERS = {
    "Administrator": {"username": "admin", "password": "admin123"},
    "Dealer": {"username": "dealer1", "password": "dealer123"},
    "Reviewer": {"username": "reviewer1", "password": "reviewer123"},
    "EndUser": {"username": "user1", "password": "user123"},
    "SupportAgent": {"username": "support1", "password": "support123"},
}


def make_request(method: str, path: str, body: str = None, username: str = None, password: str = None) -> dict:
    """Make an HTTP request to the nlkart API."""
    url = f"{API_BASE}{path}"
    headers = {"Content-Type": "application/json"}

    if username and password:
        credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
        headers["Authorization"] = f"Basic {credentials}"

    data = body.encode() if body else None
    req = urllib.request.Request(url, data=data, headers=headers, method=method)

    try:
        with urllib.request.urlopen(req) as response:
            response_body = response.read().decode()
            return {
                "status": response.status,
                "body": json.loads(response_body) if response_body else None,
                "headers": dict(response.headers)
            }
    except urllib.error.HTTPError as e:
        error_body = e.read().decode() if e.fp else ""
        return {
            "status": e.code,
            "body": json.loads(error_body) if error_body else None,
            "error": str(e.reason)
        }
    except urllib.error.URLError as e:
        return {"error": f"Connection failed: {str(e.reason)}"}


@server.tool()
def call_api(method: str, path: str, body: str = None, username: str = None, password: str = None) -> str:
    """Call any nlkart-api endpoint. Method: GET/POST/PUT/DELETE. Path starts with /api/."""
    result = make_request(method.upper(), path, body, username, password)
    return json.dumps(result, default=str)


@server.tool()
def login_as(role: str) -> str:
    """Login as a specific role user and return auth details. Roles: Administrator, Dealer, Reviewer, EndUser, SupportAgent."""
    if role not in TEST_USERS:
        return json.dumps({"error": f"Unknown role: {role}. Available: {list(TEST_USERS.keys())}"})

    user = TEST_USERS[role]
    result = make_request(
        "POST", "/api/auth/login",
        body=json.dumps({"username": user["username"], "password": user["password"]}),
        username=user["username"],
        password=user["password"]
    )

    if result.get("status") == 200:
        cred_string = user["username"] + ":" + user["password"]
        result["credentials"] = {
            "username": user["username"],
            "role": role,
            "authHeader": f"Basic {base64.b64encode(cred_string.encode()).decode()}"
        }

    return json.dumps(result, default=str)


@server.tool()
def health_check() -> str:
    """Verify the nlkart API is running and responsive."""
    result = make_request("GET", "/api/products")
    if "error" in result:
        return json.dumps({
            "status": "DOWN",
            "error": result["error"],
            "suggestion": "Start the API with: cd ../nlkart-api && python run.py"
        })
    return json.dumps({
        "status": "UP",
        "httpStatus": result.get("status"),
        "apiBase": API_BASE
    })


if __name__ == "__main__":
    server.run(transport="stdio")
