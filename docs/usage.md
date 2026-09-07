# SparkLens Configuration & Usage Guide

This document describes how to configure, start, and integrate SparkLens into your local environment and IDE clients for both Apache Spark 3.x and Spark 4.x.

---

## Configuration Reference

SparkLens is configured via environment variables or a `.env` file.

| Environment Variable | Description | Default | Valid Values |
|---|---|---|---|
| `SPARK_HISTORY_URL` | Root URL of your Spark History Server | `http://localhost:18080` | Any valid HTTP/HTTPS URL |
| `SPARK_AUTH_TYPE` | Authentication mechanism to connect to Spark | `none` | `none`, `basic`, `oauth` |
| `SPARK_USERNAME` | Username for basic authentication | *None* | String |
| `SPARK_PASSWORD` | Password for basic authentication | *None* | String |
| `SPARK_ACCESS_TOKEN`| Bearer/OAuth token | *None* | String |
| `SPARK_DEFAULT_VERSION` | Fallback Spark version when undetectable dynamically | `auto` | `auto`, `3.x`, `4.x` |
| `SPARK_MCP_TRANSPORT` | MCP Transport type | `stdio` | `stdio`, `sse` |
| `SPARK_MCP_HOST` | Host for SSE server | `0.0.0.0` | IP or hostname |
| `SPARK_MCP_PORT` | Port for SSE server | `8030` | Valid TCP port |

---

## Launching SparkLens

### Option A: Using Make (Quickest)

```bash
# Install dependencies & prepare .env
make install

# Run server with STDIO transport
make run

# Or run server with SSE transport on port 8030
make run-sse

# Launch FastMCP Developer Inspector
make inspector
```

### Option B: Running Directly with uv

```bash
# Create a .env file from template
cp .env.example .env

# Run the server via STDIO transport using uv
uv run --env-file .env python -m sparklens.server
```

To run with **HTTP/SSE** transport:
```bash
SPARK_MCP_TRANSPORT=sse SPARK_MCP_PORT=8030 uv run --env-file .env python -m sparklens.server
```

---

## Testing with the MCP Inspector

You can inspect all SparkLens tools and prompts interactively using FastMCP's built-in inspector:

```bash
make inspector
# or directly with uv:
uv run --env-file .env fastmcp dev inspector src/sparklens/server.py
```

---

## Client Integration

### 1. Claude Desktop
Add SparkLens to `claude_desktop_config.json` (`~/Library/Application Support/Claude/claude_desktop_config.json` on macOS):

```json
{
  "mcpServers": {
    "sparklens": {
      "command": "uv",
      "args": [
        "--directory",
        "/Users/ervijay/Documents/Programs/Repo/spark-lens",
        "run",
        "python",
        "-m",
        "sparklens.server"
      ],
      "env": {
        "SPARK_HISTORY_URL": "http://localhost:18080",
        "SPARK_DEFAULT_VERSION": "auto"
      }
    }
  }
}
```

### 2. Cursor IDE
1. Open **Settings** > **Cursor Settings** > **Features** > **MCP**.
2. Click **+ Add New MCP Server**.
3. Set:
   - **Name**: `SparkLens`
   - **Type**: `stdio`
   - **Command**: `uv --directory /Users/ervijay/Documents/Programs/Repo/spark-lens run python -m sparklens.server`
