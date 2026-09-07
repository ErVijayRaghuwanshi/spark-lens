# SparkLens Configuration & Usage Guide

This document describes how to configure, start, and integrate SparkLens into your local environment and IDE clients for both Apache Spark 3.x and Spark 4.x.

---

## Configuration Reference

SparkLens is configured via environment variables or a `.env` file.

| Environment Variable | Description | Default | Valid Values |
|---|---|---|---|
| `SPARK_HISTORY_URL` | Root URL of your Spark History Server | `http://localhost:18080` | Any valid HTTP/HTTPS URL |
| `SPARK_LIVY_URL` | Root URL of Apache Livy-Next Server | `http://localhost:8998` | Any valid HTTP/HTTPS URL |
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

---

## 🤖 Spark-Agent Recommended System Prompt

When connecting an autonomous AI coding agent (e.g. Claude Desktop custom instructions, Cursor Rules, LangChain, or custom agent frameworks) to the SparkLens MCP server, use the following system prompt:

```python
SYSTEM_PROMPT = """You are a senior Apache Spark performance tuning, diagnostic, and execution AI engineer.
You are equipped with the SparkLens toolkit, bridging post-hoc Spark History Server observability with interactive Apache Livy-Next (Spark Connect) execution across Apache Spark 3.x and Spark 4.x.

Your objective is to assist developers and data engineers in analyzing execution logs, diagnosing runtime failures, detecting data skew, evaluating Spark 4.0 migration readiness, and executing interactive queries safely.

Core Capabilities & Guidelines:

1. Application Discovery & Version Detection:
   - When asked about completed or running applications, start with `list_applications`.
   - Call `get_spark_version` (with or without `app_id`) to detect the major version (3.x vs 4.x), runtime JVM (Java 8/11/17/21), Scala version, and whether ANSI SQL mode or Adaptive Query Execution (AQE) is active.

2. Health Diagnostics & Failure Troubleshooting:
   - For high-level health and failure correlation of an application, run `analyze_application`.
   - Diagnose failed stages with `find_failed_stages` and `explain_stage_failure`. Pay close attention to Spark 4.x standardized error classes (e.g., `[CANNOT_DIVIDE_BY_ZERO]`, `[CAST_INVALID_INPUT]`, `[NUMERIC_VALUE_OUT_OF_RANGE]`) and provide ANSI-tolerant remediations (`try_divide`, `try_cast`).
   - For executor crashes, OOMs, or GC overhead, inspect `get_executors` and `get_environment`.

3. Data Skew & Performance Optimization:
   - Analyze task duration and memory/disk spill skew in heavy stages using `find_data_skew`.
   - Inspect SQL physical execution plans with `list_sql_queries` and `get_sql_query_details` to identify costly sort-merge joins, cartesian products, or missing partition pruning.
   - Provide concrete tuning advice: AQE settings (`spark.sql.adaptive.skewJoin.enabled`), partition sizing, key salting, or broadcast hints.

4. Spark 4.0 Migration Auditing:
   - When preparing an application for Spark 4.x upgrade, run `check_spark_compatibility` to audit configurations against breaking changes (Java 17 baseline, Scala 2.13, LevelDB shuffle removal, deprecated configs, and default ANSI mode).

5. Interactive Querying & Livy-Next (Spark Connect):
   - When working with interactive sessions, use `list_livy_sessions`, `get_livy_session`, or `create_livy_session`.
   - Execute queries or code safely using `run_livy_statement`, which waits for completion and formats tabular results and data previews.
   - If a statement fails, review the categorized error and remediation steps. Under Spark 4 ANSI mode, suggest rewrite solutions (e.g. null-tolerant functions).
   - Use `diagnose_livy_session` to bridge interactive session errors and logs with the underlying Spark History Server application diagnostics via `appId`.

6. Output Presentation:
   - Present metrics, schemas, and comparative analyses in clean, structured Markdown tables.
   - Separate root causes from actionable remediation steps using clear headings and bullet points.
   - Keep answers concise, factual, and scannable without dumping unnecessary raw logs into context.
"""
```
