# Spark History Server REST API to MCP Tool Mapping

This document details how raw Apache Spark History Server REST API endpoints map to SparkLens MCP tools across Spark 3.x and Spark 4.x.

---

## Tool Mapping Table

| Spark REST API Endpoint | SparkLens MCP Tool | Scope | Inputs | Spark 3.x / 4.x Capabilities |
|---|---|---|---|---|
| `/version` & `/applications/{appId}/environment` | `get_spark_version` | Discovery | `app_id` (str, optional) | Detects major/minor version, runtime Java/Scala versions, ANSI mode, AQE status, and feature matrix. |
| `/applications` | `list_applications` | Discovery | `status` (str), `limit` (int) | List application runs across Spark 3.x & 4.x clusters. |
| `/applications/{appId}` | `get_application` | Discovery | `app_id` (str) | High-level application attempt details. |
| `/applications/{appId}/jobs` | `get_jobs` | Deep-Dive | `app_id` (str), `status` (str) | List execution jobs. |
| `/applications/{appId}/stages` | `get_stages` | Deep-Dive | `app_id` (str), `status` (str) | List stages with execution duration & task metrics. |
| `/applications/{appId}/stages/{stageId}/{attemptId}` | `get_stage_details` | Deep-Dive | `app_id`, `stage_id`, `stage_attempt_id` | Full metrics, task breakdown, and errors. |
| `/applications/{appId}/executors` | `get_executors` | Deep-Dive | `app_id` (str) | Active/dead executor stats, cores, RAM, GC times. |
| `/applications/{appId}/environment` | `get_environment` | Deep-Dive | `app_id` (str) | System, JVM, classpath, and Spark configurations. |
| `/applications/{appId}/sql` | `list_sql_queries` | Deep-Dive | `app_id` (str), `limit` (int) | List executed SQL queries (summary list). |
| `/applications/{appId}/sql/{sqlId}` | `get_sql_query_details` | Deep-Dive | `app_id` (str), `sql_id` (int) | Detailed SQL plans and physical DAG layouts. |
| `/applications/{appId}/stages/{stageId}/{attemptId}/taskSummary` | `find_data_skew` | Diagnostic | `app_id` (str), `stage_id` (int), `stage_attempt_id` (int) | Detects runtime and memory/disk spills anomalies with version-specific AQE / tuning recommendations. |
| Composite (`/stages`, `/environment`) | `find_failed_stages` | Diagnostic | `app_id` (str) | Extracts failed stages, identifies Spark 4.x ANSI error classes (`CANNOT_DIVIDE_BY_ZERO`, `CAST_INVALID_INPUT`, etc.) and Spark 3 exceptions. |
| Composite (`/stages/{id}/{attempt}`) | `explain_stage_failure` | Diagnostic | `app_id` (str), `stage_id` (int), `stage_attempt_id` (int) | Deep-dive root-cause analysis for a specific stage failure. |
| Composite (`/applications`, `/jobs`, `/stages`, `/environment`) | `analyze_application` | Diagnostic | `app_id` (str) | High-level execution health report with Spark 3.x/4.x version classification. |
| Composite (`/environment`) | `check_spark_compatibility` | Migration | `app_id` (str) | Audits Spark 3.x app configuration against Spark 4.x breaking changes, LevelDB removal, CMS GC, and ANSI SQL. |

---

## MCP Prompts Available

| Prompt Name | Parameters | Purpose |
|---|---|---|
| `diagnose_application` | `app_id` (str) | Guides LLM through diagnosing job/stage failures, OOMs, and ANSI SQL errors. |
| `optimize_application` | `app_id` (str) | Guides LLM through auditing bottlenecks, runtime skew, memory spills, and AQE tuning. |
| `audit_spark4_migration` | `app_id` (str) | Guides LLM through creating a Spark 4.0 migration readiness report and checklist. |
| `explain_sql_query` | `app_id` (str), `sql_id` (int) | Guides LLM through inspecting and optimizing Spark SQL physical query plans. |
