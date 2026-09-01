# SparkLens 🔍 Spark Observability & AI-Assisted Troubleshooting

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![FastMCP](https://img.shields.io/badge/MCP-FastMCP-orange.svg)](https://github.com/prefecthq/fastmcp)
[![Spark](https://img.shields.io/badge/Apache_Spark-3.x_%7C_4.x-E25A1C.svg)](https://spark.apache.org/)

**SparkLens** is a Model Context Protocol (MCP) server that exposes Apache Spark History Server data as structured, context-rich tools for Large Language Models (LLMs). Built to support both **Apache Spark 3.x** and **Apache Spark 4.x**, SparkLens enables AI agents to troubleshoot failures, analyze data skew, inspect execution DAGs, and audit configurations for Spark 4.0 migrations without bloating LLM context windows with massive log dumps.

---

## 🚀 Key Features

* **Multi-Version Spark Support (3.x & 4.x)**: Automatically detects the Spark version per application, adapting diagnostic heuristics and recommendations.
* **Spark 4.0 Structured Error Framework & ANSI SQL**: Parses structured error classes (such as `[CANNOT_DIVIDE_BY_ZERO]`, `[CAST_INVALID_INPUT]`, `[NUMERIC_VALUE_OUT_OF_RANGE]`) and delivers targeted ANSI mode remediation.
* **Spark 4.x Migration Readiness Auditor**: Evaluates Spark 3.x application properties against Spark 4.x breaking changes (Java 17 baseline, Scala 2.13, RocksDB shuffle backend, removed JVM flags, ANSI defaults).
* **LLM-Oriented Diagnostics**: Concise summaries for stage failures (`find_failed_stages`, `explain_stage_failure`), application health (`analyze_application`), and partition/data skew (`find_data_skew`).
* **Deep-Dive Metric Exploration**: Inspect jobs, stages, executors, JVM environments, and SQL execution plans on-demand.
* **Flexible Authentication**: Out-of-the-box support for anonymous, Basic Auth, and OAuth Bearer tokens.

---

## 📦 Project Structure

```
spark-lens/
├── docs/
│   ├── architecture.md       # Architectural design & multi-version details
│   ├── api_mapping.md        # Spark History REST API to LLM Tool mappings
│   ├── roadmap.md            # Feature roadmap and release plan
│   └── usage.md              # Setup, settings, and client instructions
├── src/
│   └── sparklens/
│       ├── __init__.py       # Package initialization & exports
│       ├── client.py         # Async Spark History REST API client
│       ├── config.py         # Settings & environment configuration
│       ├── compatibility.py  # Spark 3.x -> 4.x migration rules & compatibility engine
│       ├── diagnostics.py    # Skew, failure categorization & health diagnostic engine
│       ├── models.py         # Pydantic schemas for MCP responses
│       ├── server.py         # FastMCP server, tools registration, and prompts
│       └── version.py        # Version parsing & capability matrix (Spark 3.x vs 4.x)
├── tests/
│   ├── conftest.py           # Test fixtures for Spark 3.x and 4.x payloads
│   ├── test_client.py        # Client & authentication tests
│   ├── test_compatibility.py # Migration auditor tests
│   ├── test_diagnostics.py   # Diagnostics & skew analysis tests
│   ├── test_server.py        # FastMCP tool & prompt tests
│   └── test_version.py       # Version detection & feature matrix tests
├── pyproject.toml            # Build, dependencies, and script entry points
└── README.md                 # This file
```

---

## ⚡ Quick Start

### 1. Prerequisites
- Python 3.10+
- Access to an Apache Spark History Server (Spark 3.x or 4.x)

### 2. Installation & Running with uv (Recommended)

```bash
# Create a .env file from template
cp .env.example .env

# Run the server via stdio transport using uv
uv run --env-file .env python -m sparklens.server
```

Alternatively, install the package locally:
```bash
uv pip install -e .
# or:
pip install -e .
```

### 3. Testing with the MCP Inspector
Launch FastMCP's built-in developer inspector:
```bash
uv run --env-file .env fastmcp dev inspector src/sparklens/server.py
```

### 4. Running the Test Suite
```bash
uv run pytest tests/ -v
```

---

## 🔌 Connecting to MCP Clients

### Claude Desktop
Add SparkLens to your `claude_desktop_config.json` (`~/Library/Application Support/Claude/claude_desktop_config.json` on macOS):

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
        "SPARK_HISTORY_URL": "http://your-spark-history-server:18080",
        "SPARK_DEFAULT_VERSION": "auto"
      }
    }
  }
}
```

---

## 🛠️ Main Tools Exposed

| Tool Name | Scope | Description |
|---|---|---|
| `get_spark_version` | Discovery | Resolves detected Spark version (3.x vs 4.x), runtime JVM, and active feature flags. |
| `list_applications` | Discovery | Lists recent application runs. |
| `get_application` | Discovery | High-level metadata and attempts for an application. |
| `get_jobs` | Deep-Dive | Lists jobs for an application. |
| `get_stages` | Deep-Dive | Lists stages with duration & task metrics. |
| `get_stage_details` | Deep-Dive | Detailed task summaries and metrics for a stage. |
| `get_executors` | Deep-Dive | Active/dead executor stats, cores, RAM, and GC pauses. |
| `get_environment` | Deep-Dive | System properties, JVM options, classpath, and Spark configurations. |
| `list_sql_queries` | Deep-Dive | List executed SQL queries. |
| `get_sql_query_details`| Deep-Dive | Detailed SQL plans and physical DAG layouts. |
| `analyze_application` | Diagnostic | Execution health report with Spark 3.x/4.x version classification and failure correlation. |
| `find_failed_stages` | Diagnostic | Scans and categorizes failed stages with Spark 4 ANSI error classes & remediations. |
| `explain_stage_failure`| Diagnostic | Deep-dive root-cause analysis for a specific failed stage. |
| `find_data_skew` | Diagnostic | Analyzes task runtime distribution and memory/disk spills with version-aware tuning advice. |
| `check_spark_compatibility` | Migration | Audits Spark 3.x applications against Spark 4.x breaking changes & deprecated configs. |

---

## 💡 Built-in MCP Prompts

- `diagnose_application`: Guides an LLM through full troubleshooting of a failed or degraded application.
- `optimize_application`: Guides an LLM through performance auditing, AQE tuning, and skew mitigation.
- `audit_spark4_migration`: Generates a Spark 4.0 migration readiness report and remediation checklist.
- `explain_sql_query`: Explains physical SQL query plans and identifies optimization opportunities.

---

## 📄 License
This project is licensed under the Apache 2.0 License.
